#
# Copyright (c) 2023 Ingram Micro. All Rights Reserved.
#

import functools
import inspect
import logging

from fastapi import (
    FastAPI,
)
from fastapi.middleware.cors import (
    CORSMiddleware,
)
from fastapi.openapi.utils import (
    generate_operation_summary,
    get_openapi,
)
from fastapi.staticfiles import (
    StaticFiles,
)
from starlette.middleware.base import (
    BaseHTTPMiddleware,
)

from connect.client import (
    ClientError,
)
from connect.eaas.core.decorators import (
    router as root_router,
)
from connect.eaas.core.utils import (
    client_error_exception_handler,
)
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.handlers.base import (
    ApplicationHandlerBase,
)


logger = logging.getLogger(__name__)


class _OpenApiCORSMiddleware(CORSMiddleware):
    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http' or scope['path'] != '/openapi/spec.json':  # pragma: no cover
            await self.app(scope, receive, send)
            return

        await super().__call__(scope, receive, send)  # pragma: no cover


class _ProxyHeadersMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] == 'http':  # pragma: no branch
            headers = dict(scope['headers'])
            if b'x-forwarded-host' in headers:
                headers[b'host'] = headers[b'x-forwarded-host']
                scope['headers'] = list(headers.items())
            if b'x-forwarded-proto' in headers:
                scope['scheme'] = headers[b'x-forwarded-proto'].decode('ascii')

        await self.app(scope, receive, send)


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
    def proxied_connect_api(self):
        return self.get_application().get_proxied_connect_api()

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

        for customer in self.ui_modules.get('customer', []):
            components.append({
                'name': customer['label'],
                'url': customer['url'],
                'integration_point': 'Customer Home Page',
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
        exception_handlers = {ClientError: client_error_exception_handler}
        if hasattr(self.get_application(), 'get_exception_handlers'):
            exception_handlers = self.get_application().get_exception_handlers(
                exception_handlers,
            )

        app = FastAPI(
            openapi_url='/openapi/spec.json',
            exception_handlers=exception_handlers,
        )
        app.add_middleware(
            _OpenApiCORSMiddleware,
            allow_origins=['*'],
        )
        app.add_middleware(_ProxyHeadersMiddleware)

        if hasattr(self.get_application(), 'get_middlewares'):
            self.setup_middlewares(app, self.get_application().get_middlewares() or [])

        auth, no_auth = self.get_application().get_routers()
        app.include_router(auth, prefix='/api')
        app.include_router(no_auth, prefix='/guest')
        app.include_router(no_auth, prefix='/unauthorized')
        app.openapi = functools.partial(self.get_api_schema, app)
        static_root = self.get_application().get_static_root()
        if static_root:
            app.mount('/static', StaticFiles(directory=static_root), name='static')

        return app

    def setup_middlewares(self, app, middlewares):
        for middleware in middlewares:
            if inspect.isfunction(middleware):
                app.add_middleware(BaseHTTPMiddleware, dispatch=middleware)
            elif inspect.isclass(middleware):
                app.add_middleware(middleware)
            elif isinstance(middleware, tuple):
                app.add_middleware(middleware[0], **middleware[1])

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

    def reload(self):
        root_router.routes = []
        return super().reload()
