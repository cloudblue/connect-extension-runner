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


logger = logging.getLogger(__name__)


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
        try:
            begin_ts = time.monotonic()
            result = await asyncio.wait_for(
                future,
                timeout=self.config.get_timeout('transformation'),
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
            self.log_exception(task_data, e)
            result_message.output = TaskOutput(result=ResultType.FAIL)
            result_message.output.message = traceback.format_exc()[:4000]

        return result_message

    async def process_transformation(self, task_data, tfn_request, method):
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
        write_queue = asyncio.Queue()

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
            write_queue,
            tfn_request['stats']['rows']['total'],
            tfn_request['transformation']['columns']['output'],
            task_data,
            loop,
        )
        processor_task = asyncio.create_task(self.process_rows(
            read_queue,
            write_queue,
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

    async def process_rows(self, read_queue, write_queue, method, total_rows):
        rows_processed = 0
        tasks = []
        while rows_processed < total_rows - 1:
            row_idx, row = await read_queue.get()

            if inspect.iscoroutinefunction(method):
                tasks.append(asyncio.create_task(self.async_process_row(
                    method,
                    row_idx,
                    row,
                    write_queue,
                )))
            else:
                loop = asyncio.get_running_loop()
                tasks.append(loop.run_in_executor(
                    self.executor,
                    self.sync_process_row,
                    method,
                    row_idx,
                    row,
                    write_queue,
                    loop,
                ))

            rows_processed += 1

        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error('Error during applying transformation to row.')
            for task in tasks:
                task.cancel()
            raise e

    async def async_process_row(self, method, row_idx, row, write_queue):
        transformed_row = await method(row)
        await write_queue.put((row_idx, transformed_row))

    def sync_process_row(self, method, row_idx, row, write_queue, loop):
        transformed_row = method(row)
        asyncio.run_coroutine_threadsafe(
            write_queue.put((row_idx, transformed_row)),
            loop,
        )

    def write_excel(self, filename, queue, total_rows, output_columns, task_data, loop):
        wb = Workbook()

        ws_columns = wb.active
        ws = wb.create_sheet('Data')
        ws_columns.title = 'Columns'
        ws_columns.append(['Name', 'Type', 'Nullable', 'Description', 'Precision'])
        column_keys = ['name', 'type', 'nullable', 'description', 'precision']
        lookup_columns = {}
        for col_idx, column in enumerate(output_columns, start=1):
            row = [column.get(key) for key in column_keys]
            ws_columns.append(row)
            lookup_columns[column.get('name')] = col_idx
            ws.cell(row=1, column=col_idx, value=column.get('name'))

        rows_processed = 0
        total_rows -= 1
        delta = 1 if total_rows <= 10 else round(total_rows / 10)

        while rows_processed < total_rows:
            future = asyncio.run_coroutine_threadsafe(
                queue.get(),
                loop,
            )
            row_idx, row = future.result(
                timeout=self.config.env['transformation_write_queue_timeout'],
            )
            for name, value in row.items():
                ws.cell(row=row_idx, column=lookup_columns[name], value=value)
            rows_processed += 1
            if rows_processed % delta == 0 or rows_processed == total_rows:
                asyncio.run_coroutine_threadsafe(
                    self.send_stat_update(task_data, rows_processed),
                    loop,
                )

        wb.save(filename)

    async def send_stat_update(self, task_data, rows_processed):
        client = self.get_client(task_data)
        await client('billing').requests[task_data.input.object_id].update(
            payload={'rows_processed': rows_processed},
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
