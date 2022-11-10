from importlib.metadata import EntryPoint

from connect.eaas.core.decorators import transformation
from connect.eaas.core.extension import TransformationBase
from connect.eaas.runner.config import ConfigHelper
from connect.eaas.runner.handlers.transformations import TfnApp


def test_get_tfnapp_module(mocker, settings_payload):

    config = ConfigHelper()

    mocker.patch.object(EntryPoint, 'load')
    mocker.patch(
        'connect.eaas.runner.handlers.transformations.iter_entry_points',
        return_value=iter([
            EntryPoint('tfnapp', None, 'connect.eaas.ext'),
        ]),
    )

    handler = TfnApp(config)

    assert handler.get_tfn_module() is not None


def test_properties(mocker):

    config = ConfigHelper()

    @transformation(
        name='my transformation',
        description='The my transformation',
        edit_dialog_ui='/static/my_settings.html',
    )
    class MyExtension(TransformationBase):
        pass

    mocker.patch(
        'connect.eaas.runner.handlers.transformations.iter_entry_points',
        sidde_effect=[
            iter([EntryPoint('tfnapp', None, 'connect.eaas.ext')]),
            iter([EntryPoint('tfnapp', None, 'connect.eaas.ext')]),
            iter([EntryPoint('tfnapp', None, 'connect.eaas.ext')]),
        ],
    )
    mocker.patch.object(EntryPoint, 'load')
    mocker.patch(
        'connect.eaas.runner.handlers.transformations.inspect.getmembers',
        return_value=[('target_class', MyExtension)],
    )

    handler = TfnApp(config)
    handler._descriptor = {
        'readme_url': 'https://readme.com',
        'changelog_url': 'https://changelog.org',
        'audience': ['vendor'],
    }

    tfn = {
        'name': 'my transformation',
        'description': 'The my transformation',
        'edit_dialog_ui': '/static/my_settings.html',
        'class_fqn': 'tests.handlers.test_transformations.MyExtension',
    }

    assert handler.config == config
    assert len(handler.transformations) == 1
    assert handler.transformations == [tfn]
    assert handler.should_start is True
    assert handler.readme == 'https://readme.com'
    assert handler.changelog == 'https://changelog.org'
    assert handler.audience == ['vendor']
    assert handler.features == {'transformations': [tfn]}
