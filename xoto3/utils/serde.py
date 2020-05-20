import typing as ty
import json
import base64
from decimal import Decimal
from datetime import datetime

from xoto3.tree_map import map_tree, type_dispatched_transform
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


def obj_to_base64(d: dict) -> str:
    """Turns your pure Python dict/thing into a base64 encoded JSON blob."""
    if not d:
        return ""
    return base64.b64encode(json.dumps(pre_json_dump(d)).encode()).decode()


def base64_to_obj(b64next: str) -> dict:
    """Takes a b64 encoded JSON object and turns it back into whatever your code originally provided."""
    if not b64next:
        return dict()
    return json.loads(base64.b64decode(b64next.encode()).decode())
