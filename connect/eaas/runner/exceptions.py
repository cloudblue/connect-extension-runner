#
# This file is part of the CloudBlue Connect EaaS Extension Runner.
#
# Copyright (c) 2025 CloudBlue. All Rights Reserved.
#
class EaaSError(Exception):
    pass


class MaintenanceError(EaaSError):
    pass


class CommunicationError(EaaSError):
    pass


class StopBackoffError(EaaSError):
    pass
