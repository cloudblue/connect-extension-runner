#
# This file is part of the Ingram Micro CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2021 Ingram Micro. All Rights Reserved.
#

OBJ_TYPE_TASK_CATEGORY = {
    'asset_request': 'bg',
    'asset_request_validation': 'interactive',
    'tier_config_request': 'bg',
    'tier_config_request_validation': 'interactive',
}

OBJ_TYPE_EXT_METHOD_MAP = {
    'asset_request': 'process_asset_request',
    'asset_request_validation': 'validate_asset_request',
    'tier_config_request': 'process_tier_config_request',
    'tier_config_request_validation': 'validate_tier_config_request',
}
