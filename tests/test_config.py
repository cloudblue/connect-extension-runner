from connect.eaas.config import ConfigHelper


def test_get_user_agent(mocker):
    mocker.patch('connect.eaas.config.platform.python_implementation', return_value='1')
    mocker.patch('connect.eaas.config.platform.python_version', return_value='3.15')
    mocker.patch('connect.eaas.config.platform.system', return_value='Linux')
    mocker.patch('connect.eaas.config.platform.release', return_value='1.0')
    mocker.patch('connect.eaas.config.get_version', return_value='22.0')

    config = ConfigHelper()
    config.env = {'environment_id': '1', 'instance_id': '2'}
    expected_ua = 'connect-extension-runner/22.0 1/3.15 Linux/1.0 1/2'
    assert config.get_user_agent() == {'User-Agent': expected_ua}
