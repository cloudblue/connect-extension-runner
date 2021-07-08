import dataclasses
from typing import Any, Optional, Union


class TaskType:
    ASSET_PURCHASE_REQUEST_PROCESSING = 'asset_purchase_request_processing'
    ASSET_CHANGE_REQUEST_PROCESSING = 'asset_change_request_processing'
    ASSET_SUSPEND_REQUEST_PROCESSING = 'asset_suspend_request_processing'
    ASSET_RESUME_REQUEST_PROCESSING = 'asset_resume_request_processing'
    ASSET_CANCEL_REQUEST_PROCESSING = 'asset_cancel_request_processing'
    ASSET_ADJUSTMENT_REQUEST_PROCESSING = 'asset_adjustment_request_processing'
    ASSET_PURCHASE_REQUEST_VALIDATION = 'asset_purchase_request_validation'
    ASSET_CHANGE_REQUEST_VALIDATION = 'asset_change_request_validation'
    PRODUCT_ACTION_EXECUTION = 'product_action_execution'
    PRODUCT_CUSTOM_EVENT_PROCESSING = 'product_custom_event_processing'
    TIER_CONFIG_SETUP_REQUEST_PROCESSING = 'tier_config_setup_request_processing'
    TIER_CONFIG_CHANGE_REQUEST_PROCESSING = 'tier_config_change_request_processing'
    TIER_CONFIG_ADJUSTMENT_REQUEST_PROCESSING = 'tier_config_adjustment_request_processing'
    TIER_CONFIG_SETUP_REQUEST_VALIDATION = 'tier_config_setup_request_validation'
    TIER_CONFIG_CHANGE_REQUEST_VALIDATION = 'tier_config_change_request_validation'


class MessageType:
    CAPABILITIES = 'capabilities'
    CONFIGURATION = 'configuration'
    TASK = 'task'
    PAUSE = 'pause'
    RESUME = 'resume'
    SHUTDOWN = 'shutdown'


class TaskCategory:
    BACKGROUND = 'background'
    INTERACTIVE = 'interactive'


class ResultType:
    SUCCESS = 'success'
    RESCHEDULE = 'reschedule'
    SKIP = 'skip'
    RETRY = 'retry'
    FAIL = 'fail'


@dataclasses.dataclass
class TaskPayload:
    task_id: str
    task_category: str
    task_type: str
    object_id: str
    result: str = None
    data: Any = None
    countdown: int = 0
    output: str = None
    correlation_id: str = None
    reply_to: str = None

    def to_json(self):
        return dataclasses.asdict(self)


@dataclasses.dataclass
class ConfigurationPayload:
    configuration: dict = None
    logging_api_key: str = None
    environment_type: str = None
    log_level: str = None
    runner_log_level: str = None

    def to_json(self):
        return dataclasses.asdict(self)


@dataclasses.dataclass
class CapabilitiesPayload:
    capabilities: dict
    readme_url: str = None
    changelog_url: str = None

    def to_json(self):
        return dataclasses.asdict(self)


@dataclasses.dataclass(init=False)
class Message:
    message_type: str
    data: Optional[Union[CapabilitiesPayload, ConfigurationPayload, TaskPayload]] = None

    def __init__(self, message_type=None, data=None):
        self.message_type = message_type
        if isinstance(data, dict):
            if self.message_type == MessageType.CONFIGURATION:
                self.data = ConfigurationPayload(**data)
            elif self.message_type == MessageType.TASK:
                self.data = TaskPayload(**data)
            elif self.message_type == MessageType.CAPABILITIES:
                self.data = CapabilitiesPayload(**data)
        else:
            self.data = data

    def to_json(self):
        payload = {'message_type': self.message_type}
        if self.data:
            payload['data'] = dataclasses.asdict(self.data)
        return payload
