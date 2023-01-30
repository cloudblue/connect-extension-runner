import functools
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.utils import generate_operation_summary, get_openapi

from connect.client import ClientError
from connect.eaas.core.utils import client_error_exception_handler
from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.handlers.base import ApplicationHandlerBase


logger = logging.getLogger(__name__)


class _OpenApiCORSMiddleware(CORSMiddleware):
    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http' or scope['path'] != '/openapi/spec.json':  # pragma: no cover
            await self.app(scope, receive, send)
            return

        await super().__call__(scope, receive, send)  # pragma: no cover


class WebApp(ApplicationHandlerBase):
    """
    Handle the lifecycle of an extension.
    """

    LOGGER_NAME = 'eaas.webapp'

    def __init__(self, config: ConfigHelper):
        super().__init__(config)
        self._logging_handler = None
        self._app = None

    @property
    def ui_modules(self):
        return self.get_application().get_ui_modules()

    @property
    def app(self):
        if not self._app:
            self._app = self.get_asgi_application()
        return self._app

    def get_application(self):
        return self.load_application('webapp')

    def get_features(self):
        return {
            'endpoints': self.get_endpoints(),
            'ui_modules': self.get_ui_components(),
        }

    def get_ui_components(self):
        components = []

        if 'settings' in self.ui_modules:
            components.append({
                'name': self.ui_modules['settings']['label'],
                'url': self.ui_modules['settings']['url'],
                'integration_point': 'Account Settings',
            })

        if 'modules' in self.ui_modules:
            components.append({
                'name': self.ui_modules['modules']['label'],
                'url': self.ui_modules['modules']['url'],
                'integration_point': 'Module Feature',
            })
            for child in self.ui_modules['modules'].get('children', []):
                components.append({
                    'name': child['label'],
                    'url': child['url'],
                    'integration_point': 'Module Feature',
                })

        for admin in self.ui_modules.get('admins', []):
            components.append({
                'name': admin['label'],
                'url': admin['url'],
                'integration_point': 'Installations Admin',
            })
        return components

    def get_endpoints(self):
        auth, no_auth = self.get_application().get_routers()
        endpoints = {
            'auth': [],
            'no_auth': [],
        }
        for router, group in ((auth, 'auth'), (no_auth, 'no_auth')):
            for route in router.routes:
                for method in route.methods:
                    endpoints[group].append({
                        'summary': generate_operation_summary(route=route, method=method),
                        'method': method,
                        'path': route.path,
                    })
        return endpoints

    def get_asgi_application(self):
        app = FastAPI(
            openapi_url='/openapi/spec.json',
            exception_handlers={
                ClientError: client_error_exception_handler,
            },
        )
        app.add_middleware(
            _OpenApiCORSMiddleware,
            allow_origins=['*'],
        )
        auth, no_auth = self.get_application().get_routers()
        app.include_router(auth, prefix='/api')
        app.include_router(no_auth, prefix='/guest')
        app.openapi = functools.partial(self.get_api_schema, app)
        static_root = self.get_application().get_static_root()
        if static_root:
            app.mount('/static', StaticFiles(directory=static_root), name='static')

        return app

    def get_api_schema(self, app):
        if app.openapi_schema:
            return app.openapi_schema

        openapi_schema = get_openapi(
            title=self.name,
            description=self.description,
            version=self.version,
            routes=app.routes,
        )

        paths = openapi_schema.get('paths')

        if paths:
            for path in paths.values():
                for operation in path.values():
                    params = []
                    for param in operation.get('parameters', []):
                        if param['in'] == 'header' and param['name'].startswith('x-connect-'):
                            continue
                        params.append(param)
                    operation['parameters'] = params

        app.openapi_schema = openapi_schema
        return app.openapi_schema
