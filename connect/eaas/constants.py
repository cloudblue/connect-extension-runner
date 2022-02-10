#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#

from connect.eaas.dataclasses import TaskType


TASK_TYPE_EXT_METHOD_MAP = {
    TaskType.ASSET_PURCHASE_REQUEST_PROCESSING: 'process_asset_purchase_request',
    TaskType.ASSET_CHANGE_REQUEST_PROCESSING: 'process_asset_change_request',
    TaskType.ASSET_SUSPEND_REQUEST_PROCESSING: 'process_asset_suspend_request',
    TaskType.ASSET_RESUME_REQUEST_PROCESSING: 'process_asset_resume_request',
    TaskType.ASSET_CANCEL_REQUEST_PROCESSING: 'process_asset_cancel_request',
    TaskType.ASSET_ADJUSTMENT_REQUEST_PROCESSING: 'process_asset_adjustment_request',
    TaskType.ASSET_PURCHASE_REQUEST_VALIDATION: 'validate_asset_purchase_request',
    TaskType.ASSET_CHANGE_REQUEST_VALIDATION: 'validate_asset_change_request',
    TaskType.PRODUCT_ACTION_EXECUTION: 'execute_product_action',
    TaskType.PRODUCT_CUSTOM_EVENT_PROCESSING: 'process_product_custom_event',
    TaskType.TIER_CONFIG_SETUP_REQUEST_PROCESSING: 'process_tier_config_setup_request',
    TaskType.TIER_CONFIG_CHANGE_REQUEST_PROCESSING: 'process_tier_config_change_request',
    TaskType.TIER_CONFIG_ADJUSTMENT_REQUEST_PROCESSING: 'process_tier_config_adjustment_request',
    TaskType.TIER_CONFIG_SETUP_REQUEST_VALIDATION: 'validate_tier_config_setup_request',
    TaskType.TIER_CONFIG_CHANGE_REQUEST_VALIDATION: 'validate_tier_config_change_request',
    TaskType.LISTING_NEW_REQUEST_PROCESSING: 'process_new_listing_request',
    TaskType.LISTING_REMOVE_REQUEST_PROCESSING: 'process_remove_listing_request',
    TaskType.TIER_ACCOUNT_UPDATE_REQUEST_PROCESSING: 'process_tier_account_update_request',
    TaskType.USAGE_FILE_REQUEST_PROCESSING: 'process_usage_file',
    TaskType.PART_USAGE_FILE_REQUEST_PROCESSING: 'process_usage_chunk_file',
}

ASSET_REQUEST_TASK_TYPES = (
    TaskType.ASSET_PURCHASE_REQUEST_PROCESSING,
    TaskType.ASSET_CHANGE_REQUEST_PROCESSING,
    TaskType.ASSET_SUSPEND_REQUEST_PROCESSING,
    TaskType.ASSET_RESUME_REQUEST_PROCESSING,
    TaskType.ASSET_CANCEL_REQUEST_PROCESSING,
    TaskType.ASSET_ADJUSTMENT_REQUEST_PROCESSING,
    TaskType.ASSET_PURCHASE_REQUEST_VALIDATION,
    TaskType.ASSET_CHANGE_REQUEST_VALIDATION,
)

TIER_CONFIG_REQUEST_TASK_TYPES = (
    TaskType.TIER_CONFIG_SETUP_REQUEST_PROCESSING,
    TaskType.TIER_CONFIG_CHANGE_REQUEST_PROCESSING,
    TaskType.TIER_CONFIG_SETUP_REQUEST_VALIDATION,
    TaskType.TIER_CONFIG_CHANGE_REQUEST_VALIDATION,
    TaskType.TIER_CONFIG_ADJUSTMENT_REQUEST_PROCESSING,
)

LISTING_REQUEST_TASK_TYPES = (
    TaskType.LISTING_NEW_REQUEST_PROCESSING,
    TaskType.LISTING_REMOVE_REQUEST_PROCESSING,
)

BACKGROUND_TASK_TYPES = (
    TaskType.ASSET_PURCHASE_REQUEST_PROCESSING,
    TaskType.ASSET_CHANGE_REQUEST_PROCESSING,
    TaskType.ASSET_SUSPEND_REQUEST_PROCESSING,
    TaskType.ASSET_RESUME_REQUEST_PROCESSING,
    TaskType.ASSET_CANCEL_REQUEST_PROCESSING,
    TaskType.ASSET_ADJUSTMENT_REQUEST_PROCESSING,
    TaskType.TIER_CONFIG_SETUP_REQUEST_PROCESSING,
    TaskType.TIER_CONFIG_CHANGE_REQUEST_PROCESSING,
    TaskType.TIER_CONFIG_ADJUSTMENT_REQUEST_PROCESSING,
    TaskType.LISTING_NEW_REQUEST_PROCESSING,
    TaskType.LISTING_REMOVE_REQUEST_PROCESSING,
    TaskType.TIER_ACCOUNT_UPDATE_REQUEST_PROCESSING,
    TaskType.USAGE_FILE_REQUEST_PROCESSING,
    TaskType.PART_USAGE_FILE_REQUEST_PROCESSING,

)

INTERACTIVE_TASK_TYPES = (
    TaskType.ASSET_PURCHASE_REQUEST_VALIDATION,
    TaskType.ASSET_CHANGE_REQUEST_VALIDATION,
    TaskType.TIER_CONFIG_SETUP_REQUEST_VALIDATION,
    TaskType.TIER_CONFIG_CHANGE_REQUEST_VALIDATION,
    TaskType.PRODUCT_ACTION_EXECUTION,
    TaskType.PRODUCT_CUSTOM_EVENT_PROCESSING,
)

VALIDATION_TASK_TYPES = (
    TaskType.ASSET_PURCHASE_REQUEST_VALIDATION,
    TaskType.ASSET_CHANGE_REQUEST_VALIDATION,
    TaskType.TIER_CONFIG_SETUP_REQUEST_VALIDATION,
    TaskType.TIER_CONFIG_CHANGE_REQUEST_VALIDATION,
)

OTHER_INTERACTIVE_TASK_TYPES = (
    TaskType.PRODUCT_ACTION_EXECUTION,
    TaskType.PRODUCT_CUSTOM_EVENT_PROCESSING,
)

BACKGROUND_TASK_MAX_EXECUTION_TIME = 300
INTERACTIVE_TASK_MAX_EXECUTION_TIME = 120
SCHEDULED_TASK_MAX_EXECUTION_TIME = 60 * 60 * 12
RESULT_SENDER_MAX_RETRIES = 5
RESULT_SENDER_WAIT_GRACE_SECONDS = 90

MAX_RETRY_TIME_GENERIC_SECONDS = 15 * 60
MAX_RETRY_TIME_MAINTENANCE_SECONDS = 3 * 60 * 60
MAX_RETRY_DELAY_TIME_SECONDS = 5 * 60

DELAY_ON_CONNECT_EXCEPTION_SECONDS = 5

ORDINAL_SUFFIX = {
    1: 'st',
    2: 'nd',
    3: 'rd',
    11: 'th',
    12: 'th',
    13: 'th',
}
