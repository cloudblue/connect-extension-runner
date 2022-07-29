import pytest
from pkg_resources import EntryPoint

from connect.eaas.core.decorators import router
from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.handlers.web import WebApp


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
            EntryPoint('webapp', 'connect.eaas.ext'),
        ]),
    )

    handler = WebApp(config)

    assert handler._webapp_class == MyExtension


def test_properties(mocker):

    descriptor = {
        'ui': {
            'settings': {
                'label': 'Test label',
                'url': '/static/settings.html',
            },
        },
        'readme_url': 'https://readme.com',
        'changelog_url': 'https://changelog.org',
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

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.web.iter_entry_points',
        return_value=iter([
            EntryPoint('webapp', 'connect.eaas.ext'),
        ]),
    )

    handler = WebApp(config)

    assert handler._webapp_class == MyExtension
    assert handler.ui_modules == descriptor['ui']
    assert handler.variables == variables
    assert handler.readme == descriptor['readme_url']
    assert handler.changelog == descriptor['changelog_url']
    assert handler.should_start is True


@pytest.mark.parametrize('static_root', ('static', None))
def test_get_asgi_application(mocker, static_root):

    config = ConfigHelper()

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

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.web.iter_entry_points',
        return_value=iter([
            EntryPoint('webapp', 'connect.eaas.ext'),
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
    mocked_fastapi.include_router.assert_called_once_with(router)
    if static_root:
        mocked_static_files.assert_called_once_with(directory=static_root)
        mocked_fastapi.mount.assert_called_once_with(
            '/static',
            mocked_static,
            name='static',
        )
