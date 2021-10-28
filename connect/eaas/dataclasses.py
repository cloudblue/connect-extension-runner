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
    SCHEDULED_EXECUTION = 'scheduled_execution'
    LISTING_NEW_REQUEST_PROCESSING = 'listing_new_request_processing'
    LISTING_REMOVE_REQUEST_PROCESSING = 'listing_remove_request_processing'
    TIER_ACCOUNT_UPDATE_REQUEST_PROCESSING = 'tier_account_update_request_processing'
    USAGE_FILE_REQUEST_PROCESSING = 'usage_file_request_processing'
    PART_USAGE_FILE_REQUEST_PROCESSING = 'part_usage_file_request_processing'


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
    SCHEDULED = 'scheduled'


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
    result: Optional[str] = None
    data: Optional[Any] = None
    countdown: int = 0
    output: Optional[str] = None
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None


@dataclasses.dataclass
class ConfigurationPayload:
    configuration: Optional[dict] = None
    logging_api_key: Optional[str] = None
    environment_type: Optional[str] = None
    account_id: Optional[str] = None
    account_name: Optional[str] = None
    service_id: Optional[str] = None
    log_level: Optional[str] = None
    runner_log_level: Optional[str] = None
    product_id: Optional[str] = None
    hub_id: Optional[str] = None


@dataclasses.dataclass
class CapabilitiesPayload:
    capabilities: dict
    variables: Optional[list] = None
    schedulables: Optional[list] = None
    readme_url: Optional[str] = None
    changelog_url: Optional[str] = None


@dataclasses.dataclass
class Message:
    message_type: str
    data: Optional[Union[CapabilitiesPayload, ConfigurationPayload, TaskPayload]] = None


def from_dict(cls, data):
    field_names = {f.name for f in dataclasses.fields(cls)}
    return cls(**{k: v for k, v in data.items() if k in field_names})


def parse_message(payload):
    message_type = payload['message_type']
    if message_type == MessageType.CONFIGURATION:
        data = from_dict(ConfigurationPayload, payload.get('data'))
    elif message_type == MessageType.TASK:
        data = from_dict(TaskPayload, payload.get('data'))
    elif message_type == MessageType.CAPABILITIES:
        data = from_dict(CapabilitiesPayload, payload.get('data'))
    else:
        data = payload.get('data')

    return Message(message_type=message_type, data=data)
