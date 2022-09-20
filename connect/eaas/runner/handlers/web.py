import functools
import logging

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.utils import get_openapi
from pkg_resources import iter_entry_points

from connect.eaas.core.constants import GUEST_ENDPOINT_ATTR_NAME
from connect.eaas.core.decorators import router
from connect.eaas.runner.config import ConfigHelper


logger = logging.getLogger(__name__)


class _OpenApiCORSMiddleware(CORSMiddleware):
    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http' or scope['path'] != '/openapi/spec.json':  # pragma: no cover
            await self.app(scope, receive, send)
            return

        await super().__call__(scope, receive, send)  # pragma: no cover


class WebApp:
    """
    Handle the lifecycle of an extension.
    """
    def __init__(self, config: ConfigHelper):
        self._config = config
        self._webapp_class = self.get_webapp_class()
        self._logging_handler = None
        self._app = None

    @property
    def config(self):
        return self._config

    @property
    def should_start(self):
        return self._webapp_class is not None

    @property
    def name(self):
        return self._webapp_class.get_descriptor().get('name', 'Unnamed API')

    @property
    def description(self):
        return self._webapp_class.get_descriptor().get('description', '')

    @property
    def version(self):
        return self._webapp_class.get_descriptor().get('version', '0.0.0')

    @property
    def ui_modules(self):
        return self._webapp_class.get_descriptor().get('ui', {})

    @property
    def variables(self):
        return self._webapp_class.get_variables()

    @property
    def readme(self):
        return self._webapp_class.get_descriptor()['readme_url']

    @property
    def changelog(self):
        return self._webapp_class.get_descriptor()['changelog_url']

    @property
    def icon(self):
        return self._webapp_class.get_descriptor().get('icon')

    @property
    def app(self):
        if not self._app:
            self._app = self.get_asgi_application()
        return self._app

    def get_asgi_application(self):
        app = FastAPI(
            openapi_url='/openapi/spec.json',
        )
        app.add_middleware(
            _OpenApiCORSMiddleware,
            allow_origins=['*'],
        )
        auth, no_auth = self.get_routers()
        app.include_router(auth, prefix='/api')
        app.include_router(no_auth, prefix='/guest')
        app.openapi = functools.partial(self.get_api_schema, app)
        static_root = self._webapp_class.get_static_root()
        if static_root:
            app.mount('/static', StaticFiles(directory=static_root), name='static')

        return app

    def get_webapp_class(self):
        ext_class = next(iter_entry_points('connect.eaas.ext', 'webapp'), None)
        return ext_class.load() if ext_class else None

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

    def get_routers(self):
        auth = APIRouter()
        no_auth = APIRouter()
        for route in router.routes:
            if getattr(route.endpoint, GUEST_ENDPOINT_ATTR_NAME, False):
                no_auth.routes.append(route)
            else:
                auth.routes.append(route)
        return auth, no_auth
