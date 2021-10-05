"""
A table and type-oriented API for using versioned_transact_write_items.

All of these things are simple bolt-ons for the underlying
functionality, and may safely be ignored.
"""
from copy import deepcopy
from typing import Callable, Generic, Optional, TypeVar, Union

from .ddb_api import table_name
from .modify import delete, put
from .read import get, require
from .specify import define_table, presume
from .types import Item, ItemKey, TableNameOrResource, TransactionBuilder, VersionedTransaction

T = TypeVar("T")

Thunk = Callable[[], T]
# a protocol for a simplified form of Lazy, a.k.a. a Thunk - the thing
# that you do when you want to defer realizing a value until runtime.


class TypedTable(Generic[T]):
    """This is a table-centric API wrapper for the low-level transaction
    interaction capabilities provided by `read.py` and
    `modify.py`. There are simplifed docs here, and you can reference
    the underlying implementations for more comprehensive
    documentation.

    In all cases, these methods are ways of interacting with an opaque
    VersionedTransaction object which represents the state of the
    database before the transaction began

    In practice, it's nice to have table-aware closures of these
    functions when dealing with a table, versus providing the string
    table name to every call. This is especially valuable if you're
    dealing with only a single table over and over again, and there's
    some kind of de/serialization you want to run consistently against
    that table.

    Note that your type_deserializer is now the expected method of
    performing the standard deepcopy-on-get to prevent mutating the
    immutable items retrieved from a VersionedTransaction.  Therefore
    the standard copy-on-get will not be performed. If you have no
    need for additional type deserialization, you must provide
    `copy.deepcopy`, or use `ItemTable`, to retain the standard, safe
    behavior.

    """

    def __init__(
        self,
        table_name: Union[TableNameOrResource, Thunk[TableNameOrResource]],
        type_deserializer: Callable[[Item], T],
        # from the database to your code - on the read path
        type_serializer: Callable[[T], Item],
        # from your code to the database - on the write path
        item_name: str = "Item",
    ):
        """Construct a table-centric API utility for a given table with
        shared serialization and deserialization.

        Your type deserializer must implement a form of deep copy.
        """
        if not callable(table_name):
            self.lazy_table = lambda: table_name
        else:
            self.lazy_table = table_name
        self.item_name = item_name
        self.type_deserializer = type_deserializer
        self.type_serializer = type_serializer

    def _lazy_table_name(self) -> str:
        return table_name(self.lazy_table())

    def get(self, key: ItemKey) -> Callable[[VersionedTransaction], Optional[T]]:
        """Get an item from the database if it exists"""

        def deser_opt(item: Optional[Item]) -> Optional[T]:
            return self.type_deserializer(item) if item else None

        return lambda vt: deser_opt(
            get(vt, self._lazy_table_name(), key, copy=False, nicename=self.item_name)
        )

    def require(self, key: ItemKey) -> Callable[[VersionedTransaction], T]:
        """Return the item for this key, or raise an ItemNotFoundException if it does not"""
        return lambda vt: self.type_deserializer(
            require(vt, self._lazy_table_name(), key, copy=False, nicename=self.item_name)
        )

    def put(self, typed_item: T) -> TransactionBuilder:
        return lambda vt: put(
            vt, self._lazy_table_name(), self.type_serializer(typed_item), nicename=self.item_name
        )

    def delete(self, key: ItemKey) -> TransactionBuilder:
        return lambda vt: delete(vt, self._lazy_table_name(), key, nicename=self.item_name)

    def presume(self, key: ItemKey, value: Optional[T]) -> TransactionBuilder:
        """'To assume as true in the absence of proof to the contrary.'

        Returns a modified transaction with this value set if the value of
        the item is not already known. If a value has already been fetched
        or presumed, this will be a no-op.

        See further docs in .read.py.
        """
        return lambda vt: presume(
            vt,
            self._lazy_table_name(),
            key,
            None if value is None else self.type_serializer(value),
        )

    def define(self, *key_attributes: str) -> TransactionBuilder:
        """Idempotent definition of key attributes for a table without any I/O"""
        return lambda vt: define_table(vt, self._lazy_table_name(), *key_attributes)


def ItemTable(
    table_name: Union[Thunk[TableNameOrResource], TableNameOrResource], item_name: str = "Item"
) -> TypedTable[Item]:
    """This is just a workaround for the fact that mypy can't handle
    generics with default arguments, e.g. in the TypedTable
    constructor above.
    """

    def _item_ident(item: Item) -> Item:
        return item

    return TypedTable(table_name, deepcopy, _item_ident, item_name=item_name)


# The following are simple single-item-write helpers with various type
# signatures for different semantics around your expectations for
# whether an item already exists, what to do if it doesn't, and
# whether the item is guaranteed to exist at the end of a successful
# call. They are provided simply as a convenience - they do nothing
# that an individual application could not do on its own.


def update_if_exists(
    table: TypedTable[T], updater: Callable[[T], T], key: ItemKey,
) -> TransactionBuilder:
    """Does not call the updater function if the item does not exist in the table."""

    def _update_if_exists(vt: VersionedTransaction) -> VersionedTransaction:
        item = table.get(key)(vt)
        if item:
            return table.put(updater(item))(vt)
        return vt

    return _update_if_exists


def update_existing(
    table: TypedTable[T], updater: Callable[[T], T], key: ItemKey,
) -> TransactionBuilder:
    """Raises ItemNotFoundException if the item to be updated does not exist in the table."""

    def update_translator(vt: VersionedTransaction) -> VersionedTransaction:
        return table.put(updater(table.require(key)(vt)))(vt)

    return update_translator


def create_or_update(
    table: TypedTable[T], creator_updater: Callable[[Optional[T]], T], key: ItemKey,
) -> TransactionBuilder:
    """Provides the item if it exists, or None if it does not, but expects
    your callable to return a writeable item.
    """

    def create_or_update_trans(vt: VersionedTransaction) -> VersionedTransaction:
        return table.put(creator_updater(table.get(key)(vt)))(vt)

    return create_or_update_trans


def write_item(
    table: TypedTable[T], writer: Callable[[Optional[T]], Optional[T]], key: ItemKey
) -> TransactionBuilder:
    """The most general purpose single-item-write abstraction, with
    necessarily weak type constraints on the writer implementation.

    Note that in DynamoDB parlance, a write can be either a Put or a
    Delete, and our usage of that terminology here parallels
    theirs. Creation, Updating, and Deleting are all in view here.

    Specifically, your writer function (as with all our other helpers
    defined here) should return _exactly_ what it intends to have
    represented in the database at the end of the transaction. If you
    wish to make no change, simply return the unmodified item. If you
    wish to _delete_ an item, return None - this indicates that you
    want the value of the item to be null, i.e. deleted from the
    table.
    """

    def write_single_item(vt: VersionedTransaction) -> VersionedTransaction:
        resulting_item = writer(table.get(key)(vt))
        if resulting_item is None:
            return table.delete(key)(vt)
        else:
            return table.put(resulting_item)(vt)

    return write_single_item
