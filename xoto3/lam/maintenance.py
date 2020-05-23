import typing as ty
import os
from functools import wraps
from logging import getLogger

from .types import LambdaEntryPoint, LambdaContext, Event


logger = getLogger(__name__)


UNDER_MAINTENANCE = os.environ.get("UNDER_MAINTENANCE", "")


class MaintenanceWindow(Exception):
    pass


def lambda_with_maintenance(lam: LambdaEntryPoint) -> LambdaEntryPoint:
    """Decorator that wraps a lambda entry point with a maintenance window capability.

    This should almost always be one of the 'outermost' wrappers
    around a Lambda, because there's no sense in having code that runs
    during maintenance windows.
    """

    @wraps(lam)
    def with_maintenance(event: Event, context: LambdaContext) -> ty.Any:
        """Passthrough to actual lambda handler unless a maintenance environment variable is set"""
        if UNDER_MAINTENANCE:
            logger.warning(
                f"{context.function_name} entry point ignoring "
                "inputs because of maintenance window: " + UNDER_MAINTENANCE
            )
            raise MaintenanceWindow(UNDER_MAINTENANCE)
        return lam(event, context)

    return with_maintenance
