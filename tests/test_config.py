from connect.eaas.core.proto import Logging, LogMeta, SetupResponse
from connect.eaas.runner.config import ConfigHelper


def test_get_user_agent(mocker):
    mocker.patch(
        'connect.eaas.runner.config.platform.python_implementation',
        return_value='1',
    )
    mocker.patch(
        'connect.eaas.runner.config.platform.python_version',
        return_value='3.15',
    )
    mocker.patch('connect.eaas.runner.config.platform.system', return_value='Linux')
    mocker.patch('connect.eaas.runner.config.platform.release', return_value='1.0')
    mocker.patch('connect.eaas.runner.config.get_version', return_value='22.0')

    config = ConfigHelper()
    config.env = {'environment_id': '1', 'instance_id': '2'}
    expected_ua = 'connect-extension-runner/22.0 1/3.15 Linux/1.0 1/2'
    assert config.get_user_agent() == {'User-Agent': expected_ua}


def test_update_dynamic_config():
    config = ConfigHelper()
    payload = SetupResponse(
        variables={'var': 'value'},
        environment_type='production',
        logging=Logging(
            log_level='DEBUG',
            logging_api_key='logging_api_key',
            runner_log_level='INFO',
            meta=LogMeta(
                service_id='SRVC-0001',
                product_id='PRD-000',
                hub_id='HB-0000',
                account_id='VA-000',
                account_name='Acme Inc',
            ),
        ),
    )

    config.update_dynamic_config(payload)
    assert config.variables == payload.variables
    assert config.service_id == payload.logging.meta.service_id
    assert config.products == payload.logging.meta.products
    assert config.hub_id == payload.logging.meta.hub_id
    assert config.account_id == payload.logging.meta.account_id
    assert config.account_name == payload.logging.meta.account_name
    assert config.logging_api_key == payload.logging.logging_api_key
    assert config.environment_type == payload.environment_type

    payload2 = SetupResponse(
        logging=Logging(
            log_level='WARNING',
            runner_log_level='ERROR',
        ),
    )

    config.update_dynamic_config(payload2)
    assert config.variables == payload.variables
    assert config.service_id == payload.logging.meta.service_id
    assert config.products == payload.logging.meta.products
    assert config.hub_id == payload.logging.meta.hub_id
    assert config.account_id == payload.logging.meta.account_id
    assert config.account_name == payload.logging.meta.account_name
    assert config.logging_api_key == payload.logging.logging_api_key
    assert config.environment_type == payload.environment_type

    payload3 = SetupResponse(
        variables={'var2': 'value2'},
    )

    config.update_dynamic_config(payload3)
    assert config.variables == payload3.variables
    assert config.service_id == payload.logging.meta.service_id
    assert config.products == payload.logging.meta.products
    assert config.hub_id == payload.logging.meta.hub_id
    assert config.account_id == payload.logging.meta.account_id
    assert config.account_name == payload.logging.meta.account_name
    assert config.logging_api_key == payload.logging.logging_api_key
    assert config.environment_type == payload.environment_type
