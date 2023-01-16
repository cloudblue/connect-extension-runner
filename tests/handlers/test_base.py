from importlib.metadata import EntryPoint

import pytest

from connect.eaas.runner.handlers.base import ApplicationHandlerBase


def test_config(mocker):
    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            pass

        def get_features(self):
            pass

    mocked_config = mocker.MagicMock()
    handler = AppHandler(mocked_config)
    assert handler.config == mocked_config


def test_variables(mocker):
    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            pass

        def get_features(self):
            pass

    mocker.patch.object(
        AppHandler,
        'get_variables',
        return_value='variables',
    )

    handler = AppHandler(mocker.MagicMock())
    assert handler.variables == 'variables'


def test_features(mocker):
    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            pass

        def get_features(self):
            pass

    mocker.patch.object(
        AppHandler,
        'get_features',
        return_value='features',
    )

    handler = AppHandler(mocker.MagicMock())
    assert handler.features == 'features'


def test_should_start(mocker):
    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            pass

        def get_features(self):
            pass

    assert AppHandler(mocker.MagicMock()).should_start is False

    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            return mocker.MagicMock()

        def get_features(self):
            pass

    assert AppHandler(mocker.MagicMock()).should_start is True


def test_descriptor_properties(mocker):
    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            pass

        def get_features(self):
            pass

    descriptor = {
        'name': 'Test name',
        'description': 'test description',
        'version': '1.0.0',
        'readme_url': 'https://example.com/readme.md',
        'changelog_url': 'https://example.com/change.md',
        'audience': ['vendor'],
        'icon': 'fa-hello',
    }

    mocker.patch.object(
        AppHandler,
        'get_descriptor',
        return_value=descriptor,
    )

    handler = AppHandler(mocker.MagicMock())

    assert handler.name == descriptor['name']
    assert handler.description == descriptor['description']
    assert handler.version == descriptor['version']
    assert handler.readme == descriptor['readme_url']
    assert handler.changelog == descriptor['changelog_url']
    assert handler.audience == descriptor['audience']
    assert handler.icon == descriptor['icon']

    mocker.patch.object(
        AppHandler,
        'get_descriptor',
        return_value={},
    )

    handler = AppHandler(mocker.MagicMock())

    assert handler.name == 'Unnamed API'
    assert handler.description == ''
    assert handler.version == '0.0.0'
    assert handler.audience is None
    assert handler.icon is None


def test_load_application(mocker):

    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            pass

        def get_features(self):
            pass

    mocked_app = mocker.MagicMock()
    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=mocked_app,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.base.iter_entry_points',
        return_value=iter([
            EntryPoint('testapp', None, 'connect.eaas.ext'),
        ]),
    )

    handler = AppHandler(mocker.MagicMock())

    assert handler.load_application('test_app') == mocked_app


def test_load_application_not_found(mocker):

    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            pass

        def get_features(self):
            pass

    mocker.patch.object(
        EntryPoint,
        'load',
        return_value=None,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.base.iter_entry_points',
        return_value=iter([
            EntryPoint('testapp', None, 'connect.eaas.ext'),
        ]),
    )

    handler = AppHandler(mocker.MagicMock())

    assert handler.load_application('test_app') is None


def test_reload_class(mocker):
    mocked_module = mocker.MagicMock()

    mocked_app = mocker.MagicMock()
    mocked_app.__module__ = 'my_module'

    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            return mocked_app

        def get_features(self):
            pass

    mocker.patch(
        'connect.eaas.runner.handlers.base.sys.modules',
        {'my_module': mocked_module},
    )

    mocker.patch(
        'connect.eaas.runner.handlers.base.inspect.isclass',
        return_value=True,
    )

    mocked_reload = mocker.patch(
        'connect.eaas.runner.handlers.base.importlib.reload',
    )

    handler = AppHandler(mocker.MagicMock())
    handler.reload()

    mocked_reload.assert_called_once_with(mocked_module)


def test_reload_module(mocker):
    mocked_module = mocker.MagicMock()

    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            return mocked_module

        def get_features(self):
            pass

    mocker.patch(
        'connect.eaas.runner.handlers.base.inspect.isclass',
        return_value=False,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.base.inspect.ismodule',
        return_value=True,
    )

    mocked_reload = mocker.patch(
        'connect.eaas.runner.handlers.base.importlib.reload',
    )

    handler = AppHandler(mocker.MagicMock())
    handler.reload()

    mocked_reload.assert_called_once_with(mocked_module)


def test_reload_invalid(mocker):
    mocked_module = mocker.MagicMock()

    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            return mocked_module

        def get_features(self):
            pass

    mocker.patch(
        'connect.eaas.runner.handlers.base.inspect.isclass',
        return_value=False,
    )
    mocker.patch(
        'connect.eaas.runner.handlers.base.inspect.ismodule',
        return_value=False,
    )

    handler = AppHandler(mocker.MagicMock())

    with pytest.raises(RuntimeError) as cv:
        handler.reload()

    assert str(cv.value) == 'Invalid application object.'


def test_reload_noapp(mocker):
    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            pass

        def get_features(self):
            pass

    mocked_reload = mocker.patch(
        'connect.eaas.runner.handlers.base.importlib.reload',
    )

    handler = AppHandler(mocker.MagicMock())
    handler.reload()

    mocked_reload.assert_not_called()


def test_get_descriptor(mocker):
    mocked_descriptor = mocker.MagicMock()
    mocked_app = mocker.MagicMock()
    mocked_app.get_descriptor.return_value = mocked_descriptor

    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            return mocked_app

        def get_features(self):
            pass

    handler = AppHandler(mocker.MagicMock())

    assert handler.get_descriptor() == mocked_descriptor


def test_get_descriptor_no_app(mocker):
    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            pass

        def get_features(self):
            pass

    handler = AppHandler(mocker.MagicMock())

    assert handler.get_descriptor() is None


def test_get_variables(mocker):
    mocked_variables = mocker.MagicMock()
    mocked_app = mocker.MagicMock()
    mocked_app.get_variables.return_value = mocked_variables

    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            return mocked_app

        def get_features(self):
            pass

    handler = AppHandler(mocker.MagicMock())

    assert handler.get_variables() == mocked_variables


def test_get_variables_no_app(mocker):
    class AppHandler(ApplicationHandlerBase):
        def get_application(self):
            pass

        def get_features(self):
            pass

    handler = AppHandler(mocker.MagicMock())

    assert handler.get_variables() is None
