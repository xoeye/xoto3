import typing as ty
from decimal import Decimal
from datetime import datetime

from .tree_map import map_tree, type_dispatched_transform
from .dec import decimal_to_number
from .dt import iso8601strict


def pre_json_dump(obj: ty.Any) -> ty.Any:
    """Encapsulates known primitive fixups to run before doing json.dump.

    Does *not* transform all possible objects. For instance, generators
    will NOT be transformed, since that would cause side effects.
    """
    return map_tree(
        type_dispatched_transform({set: list, Decimal: decimal_to_number, datetime: iso8601strict}),
        obj,
    )
