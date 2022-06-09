import logging
from multiprocessing import Process

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pkg_resources import iter_entry_points

from connect.eaas.core.decorators import router
from connect.eaas.runner.config import ConfigHelper


logger = logging.getLogger(__name__)


class WebApp:
    """
    Handle the lifecycle of an extension.
    """
    def __init__(self, config: ConfigHelper):
        self._config = config
        self._webapp_class = self.get_webapp_class()
        self._logging_handler = None
        self._server_process = None

    @property
    def should_start(self):
        return self._webapp_class is not None

    @property
    def ui_modules(self):
        return self._webapp_class.get_descriptor().get('ui', {})

    @property
    def variables(self):
        return self._webapp_class.get_variables()

    def start(self):
        if self._webapp_class:
            self._server_process = Process(daemon=True, target=self.run_server)
            self._server_process.start()

    def stop(self):
        if self._server_process:
            self._server_process.terminate()

    def run_server(self):
        app = FastAPI()
        app.include_router(router)
        static_root = self._webapp_class.get_static_root()
        if static_root:
            app.mount('/static', StaticFiles(directory=static_root), name='static')
        uvicorn.run(
            app,
            host='127.0.0.1',
            port=self._config.webapp_port,
        )

    @property
    def readme(self):
        return self._webapp_class.get_descriptor()['readme_url']

    @property
    def changelog(self):
        return self._webapp_class.get_descriptor()['changelog_url']

    def get_webapp_class(self):
        ext_class = next(iter_entry_points('connect.eaas.ext', 'webapp'), None)
        return ext_class.load() if ext_class else None
