import os
from importlib.metadata import EntryPoint

import pytest
from fastapi import Depends, Header
from fastapi.routing import APIRouter

from connect.client import ConnectClient
from connect.eaas.core.decorators import (
    account_settings_page,
    admin_pages,
    guest,
    module_pages,
    web_app,
)
from connect.eaas.core.extension import WebApplicationBase
from connect.eaas.core.inject.synchronous import get_installation, get_installation_client
from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.handlers.web import _OpenApiCORSMiddleware, WebApp


def test_get_webapp_class(mocker, settings_payload):

    config = ConfigHelper()

    class MyExtension:
        @classmethod
        def get_static_root(cls):
            return ''

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.web.iter_entry_points',
        return_value=iter([
            EntryPoint('webapp', None, 'connect.eaas.ext'),
        ]),
    )

    handler = WebApp(config)

    assert handler._webapp_class == MyExtension


def test_properties(mocker):

    descriptor = {
        'readme_url': 'https://readme.com',
        'changelog_url': 'https://changelog.org',
        'icon': 'my-icon',
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

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.web.iter_entry_points',
        return_value=iter([
            EntryPoint('webapp', None, 'connect.eaas.ext'),
        ]),
    )

    handler = WebApp(config)

    assert handler._webapp_class == MyExtension
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
    assert handler.should_start is True


@pytest.mark.parametrize('static_root', ('static', None))
def test_get_asgi_application(mocker, static_root):

    config = ConfigHelper()

    auth_router = mocker.MagicMock()
    no_auth_router = mocker.MagicMock()

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

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.web.iter_entry_points',
        return_value=iter([
            EntryPoint('webapp', None, 'connect.eaas.ext'),
        ]),
    )

    mocked_fastapi = mocker.MagicMock()
    mocker.patch('connect.eaas.runner.handlers.web.FastAPI', return_value=mocked_fastapi)

    mocked_static = mocker.MagicMock()

    mocked_static_files = mocker.patch(
        'connect.eaas.runner.handlers.web.StaticFiles',
        return_value=mocked_static,
    )
    handler = WebApp(config)

    assert handler.app == mocked_fastapi

    assert mocked_fastapi.include_router.mock_calls[0].args[0] == auth_router
    assert mocked_fastapi.include_router.mock_calls[0].kwargs['prefix'] == '/api'
    assert mocked_fastapi.include_router.mock_calls[1].args[0] == no_auth_router
    assert mocked_fastapi.include_router.mock_calls[1].kwargs['prefix'] == '/guest'

    mocked_fastapi.add_middleware.assert_called_once_with(
        _OpenApiCORSMiddleware,
        allow_origins=['*'],
    )
    if static_root:
        mocked_static_files.assert_called_once_with(directory=static_root)
        mocked_fastapi.mount.assert_called_once_with(
            '/static',
            mocked_static,
            name='static',
        )


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
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.web.iter_entry_points',
        return_value=iter([
            EntryPoint('webapp', None, 'connect.eaas.ext'),
        ]),
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

        @router.get('/no_auth')
        @guest()
        def example_no_auth(self):
            return

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.web.iter_entry_points',
        return_value=iter([
            EntryPoint('webapp', None, 'connect.eaas.ext'),
        ]),
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
        ],
    }
