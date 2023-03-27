import asyncio
import inspect
import json
import logging
import os
import time
import traceback
from tempfile import (
    NamedTemporaryFile,
)

import requests
from connect.client import (
    AsyncConnectClient,
)
from connect.client.models import (
    AsyncResource,
)
from openpyxl import (
    Workbook,
    load_workbook,
)

from connect.eaas.core.enums import (
    ResultType,
)
from connect.eaas.core.logging import (
    RequestLogger,
)
from connect.eaas.core.proto import (
    Task,
    TaskOutput,
)
from connect.eaas.core.responses import (
    TransformationResponse,
)
from connect.eaas.runner.constants import (
    DOWNLOAD_CHUNK_SIZE,
    TRANSFORMATION_TASK_MAX_PARALLEL_LINES,
    UPLOAD_CHUNK_SIZE,
)
from connect.eaas.runner.managers.base import (
    TasksManagerBase,
)
from connect.eaas.runner.managers.utils import (
    ResultStore,
)


logger = logging.getLogger(__name__)


class RowTransformationError(Exception):
    pass


class TransformationTasksManager(TasksManagerBase):
    """
    Class for Transformations managers.
    """

    def get_client(self, task_data):
        if task_data.options.api_key:
            return AsyncConnectClient(
                task_data.options.api_key,
                endpoint=self.config.get_api_url(),
                use_specs=False,
                default_headers=self.config.get_user_agent(),
                logger=RequestLogger(logger),
            )

        return self.client

    def send_skip_response(self, data, output):
        future = asyncio.Future()
        future.set_result(TransformationResponse.skip(output))
        asyncio.create_task(self.enqueue_result(data, future))

    async def get_argument(self, task_data):
        """
        Get the transformation request through Connect public API
        related to the processing task.
        """
        client = self.get_client(task_data)
        object_exists = await self.filter_collection_by_event_definition(
            client,
            task_data,
        )
        if not object_exists:
            return

        definition = self.config.event_definitions[task_data.input.event_type]
        url = definition.api_resource_endpoint.format(pk=task_data.input.object_id)
        resource = AsyncResource(client, url)

        return await resource.get()

    def get_method_name(self, task_data, argument):
        return task_data.input.data['method']

    async def invoke(self, task_data, method, tfn_request):
        """
        Creates async task for transformation process.

        :param task_data: Data of the task to be processed.
        :type task_data: connect.eaas.core.proto.Task
        :param method: The method that has to be invoked.
        :param tfn_request: Connect tfn request that need to be processed.
        :type tfn_request: dict
        """

        future = asyncio.create_task(self.process_transformation(
            task_data, tfn_request, method,
        ))

        logger.info(f'Enqueue result for task {task_data.options.task_id}')
        asyncio.create_task(self.enqueue_result(task_data, future))

    async def build_response(self, task_data, future):
        result_message = Task(**task_data.dict())
        timeout = self.config.get_timeout('transformation')
        try:
            begin_ts = time.monotonic()
            result = await asyncio.wait_for(
                future,
                timeout=timeout,
            )
            result_message.output = TaskOutput(result=result.status)
            result_message.output.runtime = time.monotonic() - begin_ts
            logger.info(
                f'Transformation task {task_data.options.task_id} '
                f'result: {result.status}, '
                f'took: {result_message.output.runtime}',
            )
            if result.status in (ResultType.SKIP, ResultType.FAIL):
                result_message.output.message = result.output
        except Exception as e:
            cause = (
                str(e) if not isinstance(e, asyncio.TimeoutError)
                else 'timed out after {timeout} s'
            )
            self.log_exception(task_data, e)
            await self._fail_task(task_data, cause)
            result_message.output = TaskOutput(result=ResultType.FAIL)
            result_message.output.message = traceback.format_exc()[:4000]

        return result_message

    async def _fail_task(self, task_data, message):
        try:
            client = self.get_client(task_data)
            await client('billing').requests[task_data.input.object_id]('fail').post()
            await client.conversations[task_data.input.object_id].messages.create(
                payload={
                    'type': 'message',
                    'text': f'Transformation request processing failed: {message}.',
                },
            )
        except Exception:
            logger.exception(f'Cannot fail the transformation request {task_data.input.object_id}')

    async def process_transformation(self, task_data, tfn_request, method):
        semaphore = asyncio.Semaphore(TRANSFORMATION_TASK_MAX_PARALLEL_LINES)
        input_file = await asyncio.get_running_loop().run_in_executor(
            self.executor,
            self.download_excel,
            tfn_request,
            task_data.options.api_key,
        )
        output_file = NamedTemporaryFile(
            suffix=f'.{tfn_request["files"]["input"]["name"].split(".")[-1]}',
        )

        read_queue = asyncio.Queue(TRANSFORMATION_TASK_MAX_PARALLEL_LINES)
        result_store = ResultStore()

        loop = asyncio.get_event_loop()

        reader_task = loop.run_in_executor(
            self.executor,
            self.read_excel,
            input_file,
            read_queue,
            loop,
        )
        writer_task = loop.run_in_executor(
            self.executor,
            self.write_excel,
            output_file.name,
            result_store,
            tfn_request['stats']['rows']['total'],
            tfn_request['transformation']['columns']['output'],
            task_data,
            loop,
        )
        processor_task = asyncio.create_task(self.process_rows(
            semaphore,
            read_queue,
            result_store,
            method,
            tfn_request['stats']['rows']['total'],
        ))

        tasks = [reader_task, writer_task, processor_task]
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(
                f'Error during processing transformation '
                f'for {task_data.options.task_id}: {e}',
            )
            for task in tasks:
                if not task.done():
                    task.cancel()
            input_file.close()
            output_file.close()
            client = self.get_client(task_data)
            await client('billing').requests[task_data.input.object_id]('fail').post()
            await client.conversations[task_data.input.object_id].messages.create(
                payload={
                    'type': 'message',
                    'text': f'Transformation request processing failed: {str(e)}',
                },
            )
            return TransformationResponse.fail(output=str(e))

        await self.send_output_file(task_data, tfn_request['batch']['id'], output_file)
        input_file.close()
        output_file.close()
        return TransformationResponse.done()

    def download_excel(self, tfn_request, api_key):
        input_file_name = tfn_request['files']['input']['name']
        input_file = NamedTemporaryFile(suffix=f'.{input_file_name.split(".")[-1]}')

        with requests.get(
            url=f'{self.config.get_api_address()}{input_file_name}',
            stream=True,
            headers={
                'Authorization': api_key,
                **self.config.get_user_agent(),
            },
        ) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                input_file.write(chunk)

        return input_file

    def read_excel(self, filename, queue, loop):
        wb = load_workbook(filename=filename, read_only=True)
        ws = wb['Data']
        lookup_columns = {}

        for idx, row in enumerate(ws.rows, start=1):
            if idx == 1:
                for col_idx, col_value in enumerate(row, start=1):
                    lookup_columns[col_idx] = col_value.value
                continue

            row_data = {}
            for col_idx, col_value in enumerate(row, start=1):
                row_data[lookup_columns[col_idx]] = col_value.value
            asyncio.run_coroutine_threadsafe(
                queue.put((idx, row_data)),
                loop,
            )

        wb.close()

    async def process_rows(self, semaphore, read_queue, result_store, method, total_rows):
        rows_processed = 0
        tasks = []
        while rows_processed < total_rows:
            await semaphore.acquire()
            row_idx, row = await read_queue.get()
            if inspect.iscoroutinefunction(method):
                tasks.append(
                    asyncio.create_task(
                        asyncio.wait_for(
                            self.async_process_row(
                                semaphore,
                                method,
                                row_idx,
                                row,
                                result_store,
                            ),
                            self.config.get_timeout('row_transformation'),
                        ),
                    ),
                )
            else:
                loop = asyncio.get_running_loop()
                tasks.append(
                    asyncio.create_task(
                        asyncio.wait_for(
                            loop.run_in_executor(
                                self.executor,
                                self.sync_process_row,
                                semaphore,
                                method,
                                row_idx,
                                row,
                                result_store,
                                loop,
                            ),
                            self.config.get_timeout('row_transformation'),
                        ),
                    ),
                )

            rows_processed += 1

        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.exception('Error during applying transformations.')
            for task in tasks:
                task.cancel()
            raise e

    async def async_process_row(self, semaphore, method, row_idx, row, result_store):
        try:
            transformed_row = await method(row)
            await result_store.put(row_idx, transformed_row)
        except Exception as e:
            raise RowTransformationError(
                f'Error applying transformation function {method.__name__} '
                f'to row #{row_idx}: {str(e)}.',
            ) from e
        finally:
            semaphore.release()

    def sync_process_row(self, semaphore, method, row_idx, row, result_store, loop):
        try:
            transformed_row = method(row)
            asyncio.run_coroutine_threadsafe(result_store.put(row_idx, transformed_row), loop)
        except Exception as e:
            raise RowTransformationError(
                f'Error applying transformation function {method.__name__} '
                f'to row #{row_idx}: {str(e)}.',
            ) from e
        finally:
            semaphore.release()

    def write_excel(self, filename, result_store, total_rows, output_columns, task_data, loop):
        wb = Workbook(write_only=True)

        ws_columns = wb.create_sheet('Columns')
        ws = wb.create_sheet('Data')
        ws_columns.title = 'Columns'
        ws_columns.append(['Name', 'Type', 'Nullable', 'Description', 'Precision'])
        column_keys = ['name', 'type', 'nullable', 'description', 'precision']

        column_names = []

        for column in output_columns:
            row = [column.get(key) for key in column_keys]
            ws_columns.append(row)
            column_names.append(column.get('name'))

        ws.append(column_names)

        rows_processed = 0
        delta = 1 if total_rows <= 10 else round(total_rows / 10)

        for idx in range(2, total_rows + 2):
            future = asyncio.run_coroutine_threadsafe(
                result_store.get(idx),
                loop,
            )
            row_data = future.result(
                timeout=self.config.env['transformation_write_queue_timeout'],
            )
            row = [row_data.get(col_name) for col_name in column_names]

            ws.append(row)
            rows_processed += 1
            if rows_processed % delta == 0 or rows_processed == total_rows:
                asyncio.run_coroutine_threadsafe(
                    self.send_stat_update(task_data, rows_processed, total_rows),
                    loop,
                )
                logger.debug(
                    f'{task_data.input.object_id} processed {rows_processed}'
                    f' of {total_rows} rows',
                )

        wb.save(filename)

    async def send_stat_update(self, task_data, rows_processed, total_rows):
        client = self.get_client(task_data)
        await client('billing').requests[task_data.input.object_id].update(
            payload={'stats': {'rows': {'total': total_rows, 'processed': rows_processed}}},
        )

    async def send_output_file(self, task_data, batch_id, output_file):
        client = self.get_client(task_data)

        fileobj = open(output_file.name, 'rb')
        fileobj.seek(0, os.SEEK_END)
        file_size = fileobj.tell()
        fileobj.seek(0)

        filename = f'{task_data.input.object_id}-out.{output_file.name.split(".")[-1]}'
        headers = {
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'Content-Disposition': f'attachment; filename="{filename}"',
            'Content-Length': str(file_size),
        }

        async def chunks_iterator():    # pragma: no cover
            while data := fileobj.read(UPLOAD_CHUNK_SIZE):
                yield data

        media_file = await client.ns('media').ns('folders').collection(
            'streams_batches',
        )[batch_id].action(
            'files',
        ).post(
            content=chunks_iterator(),
            headers=headers,
        )

        media_file_id = json.loads(media_file)['id']
        await client('billing').requests[task_data.input.object_id].update(
            payload={'files': {'output': {'id': media_file_id}}},
        )
        await client('billing').requests[task_data.input.object_id]('process').post()
