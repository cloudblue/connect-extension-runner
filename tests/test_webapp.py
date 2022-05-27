from pkg_resources import EntryPoint

from connect.eaas.core.decorators import router
from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.webapp import WebApp


def test_get_webapp_class(mocker, settings_payload):

    config = ConfigHelper()

    class MyExtension:
        pass

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.webapp.iter_entry_points',
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

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.webapp.iter_entry_points',
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


def test_start(mocker):

    config = ConfigHelper()

    class MyExtension:
        pass

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.webapp.iter_entry_points',
        return_value=iter([
            EntryPoint('webapp', 'connect.eaas.ext'),
        ]),
    )

    mocked_process = mocker.MagicMock()
    mocked_process_cls = mocker.patch(
        'connect.eaas.runner.webapp.Process',
        return_value=mocked_process,
    )

    handler = WebApp(config)

    handler.start()

    mocked_process_cls.assert_called_once_with(daemon=True, target=handler.run_server)
    mocked_process.start.assert_called_once()


def test_stop(mocker):

    config = ConfigHelper()

    class MyExtension:
        pass

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.webapp.iter_entry_points',
        return_value=iter([
            EntryPoint('webapp', 'connect.eaas.ext'),
        ]),
    )

    handler = WebApp(config)
    handler._server_process = mocker.MagicMock()

    handler.stop()
    handler._server_process.terminate.assert_called_once()


def test_run_server(mocker):
    config = ConfigHelper()

    class MyExtension:
        @classmethod
        def get_static_root(cls):
            return '/path/to/static'

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.webapp.iter_entry_points',
        return_value=iter([
            EntryPoint('webapp', 'connect.eaas.ext'),
        ]),
    )

    mocked_fast_api = mocker.MagicMock()

    mocker.patch('connect.eaas.runner.webapp.FastAPI', return_value=mocked_fast_api)

    mocked_static_app = mocker.MagicMock()
    mocked_staticfiles = mocker.patch(
        'connect.eaas.runner.webapp.StaticFiles',
        return_value=mocked_static_app,
    )
    mocked_run = mocker.patch('connect.eaas.runner.webapp.uvicorn.run')

    handler = WebApp(config)
    handler.run_server()

    mocked_fast_api.include_router.assert_called_once_with(router)
    mocked_staticfiles.assert_called_once_with(directory='/path/to/static')
    mocked_fast_api.mount.assert_called_once_with(
        '/static',
        mocked_static_app,
        name='static',
    )
    mocked_run.assert_called_once_with(
        mocked_fast_api,
        host='127.0.0.1',
        port=config.webapp_port,
    )
