from typing import (
    Any,
    Callable,
    Collection,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from typing_extensions import Protocol

from xoto3.dynamodb.types import Item, ItemKey, KeyAttributeType, TableResource

HashableItemKey = Union[KeyAttributeType, Tuple[KeyAttributeType, KeyAttributeType]]
ItemKeysByTableName = Mapping[str, Collection[ItemKey]]
ItemsByTableName = Mapping[str, Sequence[Item]]
TableNameOrResource = Union[str, TableResource]


class _TableData(NamedTuple):
    """keys and items must be parallel (same length, with corresponding indices) at all times."""

    items: Mapping[HashableItemKey, Optional[Item]]
    effects: Mapping[HashableItemKey, Union[Item, None]]
    key_attributes: Tuple[str, ...]


class VersionedTransaction(NamedTuple):
    """An opaque object representing the state of the database at the time
    the transaction began, plus all effects defined by modifier
    operations (put, delete), plus database specifications defined by
    the presume and define_table operations.

    The internals of this type does NOT constitute an API, and you
    should never modify this object except via the provided API
    functions.

    """

    tables: Mapping[str, _TableData]


TransactionBuilder = Callable[[VersionedTransaction], VersionedTransaction]


class BatchGetItem(Protocol):
    """A simplified protocol for BatchGetItem"""

    def __call__(
        self, __item_keys_by_table_name: ItemKeysByTableName, **_kwargs
    ) -> ItemsByTableName:
        ...  # pragma: nocover


class TransactWriteItems(Protocol):
    def __call__(self, *, TransactItems: List[dict], **_kwargs) -> Any:
        ...  # pragma: nocover
