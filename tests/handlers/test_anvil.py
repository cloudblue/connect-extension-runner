from importlib.metadata import EntryPoint

import pytest

from connect.eaas.core.decorators import anvil_callable
from connect.eaas.core.extension import AnvilApplicationBase
from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.handlers.anvil import AnvilApp


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
        'connect.eaas.runner.handlers.anvil.iter_entry_points',
        return_value=iter([
            EntryPoint('anvilapp', None, 'connect.eaas.ext'),
        ]),
    )

    handler = AnvilApp(config)

    assert handler._anvilapp_class == MyExtension


def test_properties(mocker):

    descriptor = {
        'readme_url': 'https://readme.com',
        'changelog_url': 'https://changelog.org',
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

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.anvil.iter_entry_points',
        return_value=iter([
            EntryPoint('anvilapp', None, 'connect.eaas.ext'),
        ]),
    )

    handler = AnvilApp(config)

    assert handler._anvilapp_class == MyExtension
    assert handler.config == config
    assert handler.variables == variables
    assert handler.readme == descriptor['readme_url']
    assert handler.changelog == descriptor['changelog_url']
    assert handler.audience == descriptor['audience']
    assert handler.should_start is True


@pytest.mark.parametrize('logging_key', ('test', None))
def test_start(mocker, logging_key):

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
        'connect.eaas.runner.handlers.anvil.iter_entry_points',
        return_value=iter([
            EntryPoint('anvilapp', None, 'connect.eaas.ext'),
        ]),
    )

    mocked_anvil_connect = mocker.patch(
        'connect.eaas.runner.handlers.anvil.anvil.server.connect',
    )

    handler = AnvilApp(config)
    handler.start()

    mocked_anvil_connect.assert_called_once_with('my_anvil_key')
    mocked_setup_callables.assert_called_once()


def test_start_no_api_key(mocker):

    mocker.patch.object(
        ConfigHelper,
        'logging_api_key',
        new_callable=mocker.PropertyMock(return_value=None),
    )

    mocker.patch.object(
        ConfigHelper,
        'metadata',
        new_callable=mocker.PropertyMock(return_value={}),
    )

    mocker.patch.object(
        ConfigHelper,
        'variables',
        new_callable=mocker.PropertyMock(return_value={}),
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
        'connect.eaas.runner.handlers.anvil.iter_entry_points',
        return_value=iter([
            EntryPoint('anvilapp', None, 'connect.eaas.ext'),
        ]),
    )

    mocked_anvil_connect = mocker.patch(
        'connect.eaas.runner.handlers.anvil.anvil.server.connect',
    )

    handler = AnvilApp(config)
    handler.start()

    mocked_anvil_connect.assert_not_called()
    mocked_setup_callables.assert_not_called()


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
        'connect.eaas.runner.handlers.anvil.iter_entry_points',
        return_value=iter([
            EntryPoint('anvilapp', None, 'connect.eaas.ext'),
        ]),
    )

    mocked_anvil_disconnect = mocker.patch(
        'connect.eaas.runner.handlers.anvil.anvil.server.disconnect',
    )

    handler = AnvilApp(config)
    handler.stop()
    mocked_anvil_disconnect.assert_called_once()


def test_features(mocker):
    config = ConfigHelper()

    class MyExtension(AnvilApplicationBase):
        @anvil_callable()
        def my_callable(self):
            pass

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=MyExtension,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.anvil.iter_entry_points',
        return_value=iter([
            EntryPoint('anvilapp', None, 'connect.eaas.ext'),
        ]),
    )

    mocker.patch(
        'connect.eaas.runner.handlers.anvil.anvil.server.disconnect',
    )

    handler = AnvilApp(config)

    assert handler.features == {'callables': [
        {
            'method': 'my_callable',
            'summary': 'My Callable',
            'description': '',
        },
    ]}
