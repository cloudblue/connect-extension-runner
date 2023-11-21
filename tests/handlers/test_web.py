import os

import django.conf
import django.core.handlers.asgi
import pytest
from django.core.exceptions import (
    ImproperlyConfigured,
)
from fastapi import (
    Depends,
    Header,
)
from fastapi.routing import (
    APIRouter,
)
from starlette.middleware.base import (
    BaseHTTPMiddleware,
)

from connect.client import (
    ClientError,
    ConnectClient,
)
from connect.eaas.core.decorators import (
    account_settings_page,
    admin_pages,
    customer_pages,
    guest,
    module_pages,
    unauthorized,
    web_app,
)
from connect.eaas.core.extension import (
    WebApplicationBase,
)
from connect.eaas.core.inject.synchronous import (
    get_installation,
    get_installation_client,
)
from connect.eaas.core.utils import (
    client_error_exception_handler,
)
from connect.eaas.runner.config import (
    ConfigHelper,
)
from connect.eaas.runner.handlers.web import (
    WebApp,
    _OpenApiCORSMiddleware,
    _ProxyHeadersMiddleware,
)


def test_get_webapp_class(mocker, settings_payload):

    config = ConfigHelper()

    class MyExtension:
        @classmethod
        def get_static_root(cls):
            return ''

    mocked_load = mocker.patch.object(
        WebApp,
        'load_application',
        return_value=MyExtension,
    )

    handler = WebApp(config)

    assert handler.get_application() == MyExtension
    mocked_load.assert_called_once_with('webapp')


def test_properties(mocker):

    descriptor = {
        'readme_url': 'https://readme.com',
        'changelog_url': 'https://changelog.org',
        'icon': 'my-icon',
        'audience': ['vendor'],
    }

    variables = [
        {
            'name': 'var1',
            'initial_value': 'val1',
        },
    ]

    config = ConfigHelper()

    class MyExtension:
        @classmethod
        def get_descriptor(cls):
            return descriptor

        @classmethod
        def get_variables(cls):
            return variables

        @classmethod
        def get_static_root(cls):
            return ''

        @classmethod
        def get_ui_modules(cls):
            return {
                'settings': {
                    'label': 'Test label',
                    'url': '/static/settings.html',
                },
            }

        @classmethod
        def get_proxied_connect_api(cls):
            return ['/abc']

    mocker.patch.object(
        WebApp,
        'load_application',
        return_value=MyExtension,
    )

    handler = WebApp(config)

    assert handler.get_application() == MyExtension
    assert handler.ui_modules == {
        'settings': {
            'label': 'Test label',
            'url': '/static/settings.html',
        },
    }
    assert handler.variables == variables
    assert handler.config == config
    assert handler.readme == descriptor['readme_url']
    assert handler.changelog == descriptor['changelog_url']
    assert handler.icon == descriptor['icon']
    assert handler.audience == descriptor['audience']
    assert handler.should_start is True
    assert handler.proxied_connect_api == ['/abc']


@pytest.mark.parametrize('dj_settings', ('some.settings', None))
@pytest.mark.parametrize('static_root', ('static', None))
def test_get_asgi_application(mocker, static_root, dj_settings):

    config = ConfigHelper()

    auth_router = mocker.MagicMock()
    no_auth_router = mocker.MagicMock()

    async def middleware_fn():
        pass

    class MiddlewareClass:
        pass

    class MyExtension:
        @classmethod
        def get_descriptor(cls):
            return {}

        @classmethod
        def get_variables(cls):
            return []

        @classmethod
        def get_static_root(cls):
            return static_root

        @classmethod
        def get_routers(cls):
            return auth_router, no_auth_router

        @classmethod
        def get_middlewares(cls):
            return [
                middleware_fn,
                MiddlewareClass,
                (MiddlewareClass, {'arg1': 'val1'}),
            ]

        @classmethod
        def get_exception_handlers(cls, exception_handlers):
            exception_handlers[ValueError] = 1
            return exception_handlers

    mocker.patch.object(
        WebApp,
        'load_application',
        return_value=MyExtension,
    )

    mocked_mount_dj_apps = mocker.patch.object(
        WebApp,
        'mount_django_apps',
        return_value=MyExtension,
    )

    mocked_fastapi = mocker.MagicMock()
    mocked_fastapi_constructor = mocker.patch(
        'connect.eaas.runner.handlers.web.FastAPI',
        return_value=mocked_fastapi,
    )

    mocked_static = mocker.MagicMock()

    mocked_static_files = mocker.patch(
        'connect.eaas.runner.handlers.web.StaticFiles',
        return_value=mocked_static,
    )
    handler = WebApp(config)
    handler._django_settings_module = dj_settings

    assert handler.app == mocked_fastapi

    mocked_fastapi_constructor.assert_called_once_with(
        openapi_url='/openapi/spec.json',
        exception_handlers={
            ClientError: client_error_exception_handler,
            ValueError: 1,
        },
    )

    assert mocked_fastapi.include_router.mock_calls[0].args[0] == auth_router
    assert mocked_fastapi.include_router.mock_calls[0].kwargs['prefix'] == '/api'
    assert mocked_fastapi.include_router.mock_calls[1].args[0] == no_auth_router
    assert mocked_fastapi.include_router.mock_calls[1].kwargs['prefix'] == '/guest'

    assert mocked_fastapi.add_middleware.mock_calls[0].args[0] == _OpenApiCORSMiddleware
    assert mocked_fastapi.add_middleware.mock_calls[0].kwargs['allow_origins'] == ['*']

    assert mocked_fastapi.add_middleware.mock_calls[1].args[0] == _ProxyHeadersMiddleware

    assert mocked_fastapi.add_middleware.mock_calls[2].args[0] == BaseHTTPMiddleware
    assert mocked_fastapi.add_middleware.mock_calls[2].kwargs['dispatch'] == middleware_fn

    assert mocked_fastapi.add_middleware.mock_calls[3].args[0] == MiddlewareClass

    assert mocked_fastapi.add_middleware.mock_calls[4].args[0] == MiddlewareClass
    assert mocked_fastapi.add_middleware.mock_calls[4].kwargs['arg1'] == 'val1'

    if static_root:
        mocked_static_files.assert_called_once_with(directory=static_root)
        mocked_fastapi.mount.assert_called_once_with(
            '/static',
            mocked_static,
            name='static',
        )

    if dj_settings:
        mocked_mount_dj_apps.assert_called_once()
    else:
        mocked_mount_dj_apps.assert_not_called()


def test_openapi_schema_generation(mocker):
    config = ConfigHelper()

    router = APIRouter()
    mocker.patch('connect.eaas.core.extension.router', router)

    @web_app(router)
    class MyExtension(WebApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'name': 'My Api Name',
                'description': 'My Api Description',
                'version': 'my api version',
            }

        @classmethod
        def get_static_root(cls):
            return os.getcwd()

        @router.get('/example')
        def example_method(
            self,
            installation: dict = Depends(get_installation),  # noqa: B008
            installation_client: ConnectClient = Depends(get_installation_client),  # noqa: B008
            custom_header: str = Header(),  # noqa: B008
        ):
            return

    mocker.patch.object(
        WebApp,
        'load_application',
        return_value=MyExtension,
    )

    handler = WebApp(config)

    schema = handler.get_api_schema(handler.app)

    assert handler.app.openapi_schema == schema

    assert schema['info']['title'] == 'My Api Name'
    assert schema['info']['description'] == 'My Api Description'
    assert schema['info']['version'] == 'my api version'

    documented_headers = []

    for path in schema['paths'].values():
        for operation in path.values():
            for param in operation.get('parameters', []):
                documented_headers.append(param['name'])

    assert documented_headers == ['custom-header']

    mocked_get_openapi = mocker.patch('connect.eaas.runner.handlers.web.get_openapi')

    assert handler.get_api_schema(handler.app) == schema
    mocked_get_openapi.assert_not_called()


def test_get_features(mocker):
    config = ConfigHelper()

    router = APIRouter()
    mocker.patch('connect.eaas.core.extension.router', router)

    @web_app(router)
    @account_settings_page('Settings', '/static/settings.html')
    @admin_pages([{'label': 'Admin', 'url': '/static/admin.html'}])
    @customer_pages(
        [
            {
                'label': 'Customer home page',
                'url': '/static/customer.html',
                'icon': '/static/icon.png',
            },
        ],
    )
    @module_pages(
        'Main',
        '/static/index.html',
        children=[
            {'label': 'Child1', 'url': '/static/child1.html'},
        ],
    )
    class MyExtension(WebApplicationBase):
        @classmethod
        def get_descriptor(cls):
            return {
                'name': 'My Api Name',
                'description': 'My Api Description',
                'version': 'my api version',
            }

        @classmethod
        def get_static_root(cls):
            return os.getcwd()

        @router.get('/auth')
        def example_auth(self):
            return

        @router.get('/no_auth_deprecated')
        @guest()
        def example_no_auth_deprecated(self):
            return

        @router.get('/no_auth')
        @unauthorized()
        def example_no_auth(self):
            return

    mocker.patch.object(
        WebApp,
        'load_application',
        return_value=MyExtension,
    )

    handler = WebApp(config)

    assert handler.features == {
        'endpoints': {
            'auth': [
                {
                    'method': 'GET',
                    'path': '/auth',
                    'summary': 'Example Auth',
                },
            ],
            'no_auth': [
                {
                    'method': 'GET',
                    'path': '/no_auth_deprecated',
                    'summary': 'Example No Auth Deprecated',
                },
                {
                    'method': 'GET',
                    'path': '/no_auth',
                    'summary': 'Example No Auth',
                },
            ],
        },
        'ui_modules': [
            {
                'integration_point': 'Account Settings',
                'url': '/static/settings.html',
                'name': 'Settings',
            },
            {
                'integration_point': 'Module Feature',
                'url': '/static/index.html',
                'name': 'Main',
            },
            {
                'integration_point': 'Module Feature',
                'url': '/static/child1.html',
                'name': 'Child1',
            },
            {
                'integration_point': 'Installations Admin',
                'url': '/static/admin.html',
                'name': 'Admin',
            },
            {
                'integration_point': 'Customer Home Page',
                'url': '/static/customer.html',
                'name': 'Customer home page',
            },
        ],
    }


def test_empty_extension(mocker):
    config = ConfigHelper()

    router = APIRouter()
    mocker.patch('connect.eaas.core.extension.router', router)

    @web_app(router)
    class MyExtension(WebApplicationBase):
        pass

    mocker.patch.object(
        WebApp,
        'load_application',
        return_value=MyExtension,
    )

    handler = WebApp(config)

    assert handler.features == {
        'endpoints': {'auth': [], 'no_auth': []},
        'ui_modules': [],
    }


def test_mount_django_apps(mocker):
    config = ConfigHelper()

    router = APIRouter()
    mocker.patch('connect.eaas.core.extension.router', router)

    @web_app(router)
    class MyExtension(WebApplicationBase):
        @classmethod
        def get_static_root(cls):
            return '/static'

    mocker.patch.object(
        WebApp,
        'load_application',
        return_value=MyExtension,
    )

    handler = WebApp(config)

    mocked_fastapi = mocker.MagicMock()

    mocked_static = mocker.MagicMock()

    mocked_static_files = mocker.patch(
        'connect.eaas.runner.handlers.web.StaticFiles',
        return_value=mocked_static,
    )

    mocked_asgi_handler = mocker.MagicMock()

    mocked_asgi_handler_constr = mocker.patch.object(
        django.core.handlers.asgi,
        'ASGIHandler',
    )
    mocked_asgi_handler_constr.return_value = mocked_asgi_handler

    mocked_settings = mocker.MagicMock()
    mocked_settings.FORCE_SCRIPT_NAME = '/guest/django'
    mocked_settings.STATIC_ROOT = '/static/django'
    mocked_settings.STATIC_URL = 'dj-static/'
    mocker.patch.object(django.conf, 'settings', mocked_settings)

    handler.mount_django_apps(mocked_fastapi)

    assert mocked_fastapi.mount.mock_calls[0].args == (
        '/guest/django/dj-static/',
        mocked_static,
    )
    assert mocked_fastapi.mount.mock_calls[1].args == ('/guest/django', mocked_asgi_handler)
    mocked_static_files.assert_called_once_with(directory=mocked_settings.STATIC_ROOT)


@pytest.mark.parametrize('script_name', ('/wathever', '/guest/', None))
def test_mount_django_apps_invalid_force_script_name(mocker, script_name):
    config = ConfigHelper()

    handler = WebApp(config)

    mocked_settings = mocker.MagicMock()
    mocked_settings.FORCE_SCRIPT_NAME = script_name
    mocker.patch.object(django.conf, 'settings', mocked_settings)

    with pytest.raises(ImproperlyConfigured) as cv:
        handler.mount_django_apps(mocker.MagicMock())

    assert str(cv.value) == '`FORCE_SCRIPT_NAME` must be set and must start with "/guest/"'


@pytest.mark.parametrize('script_name', ('/wathever', '/guest/', None))
def test_mount_django_apps_invalid_static_root(mocker, script_name):
    config = ConfigHelper()

    router = APIRouter()
    mocker.patch('connect.eaas.core.extension.router', router)

    @web_app(router)
    class MyExtension(WebApplicationBase):
        @classmethod
        def get_static_root(cls):
            return '/static'

    mocker.patch.object(
        WebApp,
        'load_application',
        return_value=MyExtension,
    )

    mocked_settings = mocker.MagicMock()
    mocked_settings.FORCE_SCRIPT_NAME = '/guest/a'
    mocked_settings.STATIC_ROOT = '/django-static'

    mocker.patch.object(django.conf, 'settings', mocked_settings)

    handler = WebApp(config)

    with pytest.raises(ImproperlyConfigured) as cv:
        handler.mount_django_apps(mocker.MagicMock())

    assert str(cv.value) == '`STATIC_ROOT` must be a directory within /static'
