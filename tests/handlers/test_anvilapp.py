import pytest
from pkg_resources import EntryPoint

from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.handlers.anvilapp import AnvilApp


def test_get_anvilapp_class(mocker, settings_payload):

    config = ConfigHelper()

    class MyExtension:
        pass

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.anvilapp.iter_entry_points',
        return_value=iter([
            EntryPoint('anvilapp', 'connect.eaas.ext'),
        ]),
    )

    handler = AnvilApp(config)

    assert handler._anvilapp_class == MyExtension


def test_properties(mocker):

    descriptor = {
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
        'connect.eaas.runner.handlers.anvilapp.iter_entry_points',
        return_value=iter([
            EntryPoint('anvilapp', 'connect.eaas.ext'),
        ]),
    )

    handler = AnvilApp(config)

    assert handler._anvilapp_class == MyExtension
    assert handler.variables == variables
    assert handler.readme == descriptor['readme_url']
    assert handler.changelog == descriptor['changelog_url']
    assert handler.should_start is True


def test_start(mocker):

    mocker.patch.object(
        ConfigHelper,
        'variables',
        new_callable=mocker.PropertyMock(return_value={'ANVIL_API_KEY': 'my_anvil_key'}),
    )

    config = ConfigHelper()

    class MyExtension:
        @classmethod
        def get_anvil_key_variable(cls):
            return 'ANVIL_API_KEY'

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.anvilapp.iter_entry_points',
        return_value=iter([
            EntryPoint('anvilapp', 'connect.eaas.ext'),
        ]),
    )

    mocked_process = mocker.MagicMock()
    mocked_process_cls = mocker.patch(
        'connect.eaas.runner.handlers.anvilapp.Process',
        return_value=mocked_process,
    )

    handler = AnvilApp(config)

    handler.start()

    mocked_process_cls.assert_called_once_with(
        daemon=True,
        target=handler.run_server,
        args=('my_anvil_key',),
    )
    mocked_process.start.assert_called_once()


def test_start_no_api_key(mocker):

    mocker.patch.object(
        ConfigHelper,
        'variables',
        new_callable=mocker.PropertyMock(return_value={}),
    )

    config = ConfigHelper()

    class MyExtension:
        @classmethod
        def get_anvil_key_variable(cls):
            return 'ANVIL_API_KEY'

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.anvilapp.iter_entry_points',
        return_value=iter([
            EntryPoint('anvilapp', 'connect.eaas.ext'),
        ]),
    )

    mocked_process = mocker.MagicMock()
    mocked_process_cls = mocker.patch(
        'connect.eaas.runner.handlers.anvilapp.Process',
        return_value=mocked_process,
    )

    handler = AnvilApp(config)

    handler.start()

    mocked_process_cls.assert_not_called()
    mocked_process.start.assert_not_called()


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
        'connect.eaas.runner.handlers.anvilapp.iter_entry_points',
        return_value=iter([
            EntryPoint('anvilapp', 'connect.eaas.ext'),
        ]),
    )

    handler = AnvilApp(config)
    handler._server_process = mocker.MagicMock()

    handler.stop()
    handler._server_process.terminate.assert_called_once()


@pytest.mark.parametrize('logging_key', ('test', None))
def test_run_server(mocker, logging_key):

    mocker.patch.object(
        ConfigHelper,
        'logging_api_key',
        new_callable=mocker.PropertyMock(return_value=logging_key),
    )

    mocker.patch.object(
        ConfigHelper,
        'metadata',
        new_callable=mocker.PropertyMock(return_value={}),
    )

    mocker.patch.object(
        ConfigHelper,
        'variables',
        new_callable=mocker.PropertyMock(return_value={'ANVIL_API_KEY': 'my_anvil_key'}),
    )

    config = ConfigHelper()

    class MyExtension:

        def __init__(self, *args):
            pass

        @classmethod
        def get_anvil_key_variable(cls):
            return 'ANVIL_API_KEY'

        def setup_anvil_callables(self):
            pass

    mocked_setup_callables = mocker.patch.object(MyExtension, 'setup_anvil_callables')

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.anvilapp.iter_entry_points',
        return_value=iter([
            EntryPoint('anvilapp', 'connect.eaas.ext'),
        ]),
    )

    mocked_anvil_connect = mocker.patch(
        'connect.eaas.runner.handlers.anvilapp.anvil.server.connect',
    )
    mocked_wait_forever = mocker.patch(
        'connect.eaas.runner.handlers.anvilapp.anvil.server.wait_forever',
    )

    handler = AnvilApp(config)
    handler.run_server('anvil_api_key')

    mocked_anvil_connect.assert_called_once_with('anvil_api_key')
    mocked_wait_forever.assert_called_once()
    mocked_setup_callables.assert_called_once()
