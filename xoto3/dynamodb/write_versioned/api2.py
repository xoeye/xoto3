"""
A table and type-oriented API for using versioned_transact_write_items.

All of these things are simple bolt-ons for the underlying
functionality, and may safely be ignored.
"""
from copy import deepcopy
from typing import Callable, Generic, Optional, TypeVar, Union

from .modify import define_table, delete, presume, put
from .read import get, require
from .types import Item, ItemKey, TransactionBuilder, VersionedTransaction

T = TypeVar("T")


class TypedTable(Generic[T]):
    """In practice, it's nice to have table-aware closures of these
    functions when dealing with a table, versus providing the string
    table name to every call. This is especially valuable if you're
    dealing with only a single table over and over again, and there's
    some kind of ser/de you want to run consistently against that
    table.

    Note that your type_deserializer is now the expected method of
    performing the standard deepcopy-on-get to prevent mutating the
    immutable items retrieved from a VersionedTransaction.  Therefore
    the standard copy-on-get will not be performed. If you have no
    need for additional type deserialization, you should provide
    `copy.deepcopy` to retain the standard, safe behavior.
    """

    def __init__(
        self,
        table_name: Union[Callable[[], str], str],
        type_deserializer: Callable[[Item], T],
        # from the database to your code - on the read path
        type_serializer: Callable[[T], Item],
        # from your code to the database - on the write path
        type_name: str = "Item",
    ):
        if isinstance(table_name, str):
            self.lazy_table_name = lambda: table_name
        else:
            self.lazy_table_name = table_name
        self.type_name = type_name
        self.type_deserializer = type_deserializer
        self.type_serializer = type_serializer

    def get(self, key: ItemKey) -> Callable[[VersionedTransaction], Optional[T]]:
        """Get an item from the database if it exists"""

        def deser_opt(item: Optional[Item]) -> Optional[T]:
            return self.type_deserializer(item) if item else None

        return lambda vt: deser_opt(
            get(vt, self.lazy_table_name(), key, copy=False, nicename=self.type_name)
        )

    def require(self, key: ItemKey) -> Callable[[VersionedTransaction], T]:
        """Return the item for this key, or raise an ItemNotFoundException if it does not"""
        return lambda vt: self.type_deserializer(
            require(vt, self.lazy_table_name(), key, copy=False, nicename=self.type_name)
        )

    def put(self, typed_item: T) -> TransactionBuilder:
        return lambda vt: put(
            vt, self.lazy_table_name(), self.type_serializer(typed_item), nicename=self.type_name
        )

    def delete(self, key: ItemKey) -> TransactionBuilder:
        return lambda vt: delete(vt, self.lazy_table_name(), key, nicename=self.type_name)

    def presume(self, key: ItemKey, value: Optional[T],) -> TransactionBuilder:
        return lambda vt: presume(
            vt, self.lazy_table_name(), key, None if value is None else self.type_serializer(value),
        )

    def define(self, *key_attributes: str) -> TransactionBuilder:
        return lambda vt: define_table(vt, self.lazy_table_name(), *key_attributes)


def ItemTable(
    table_name: Union[str, Callable[[], str]], item_name: str = "Item"
) -> TypedTable[Item]:
    """This is just a workaround for the fact that mypy can't handle
    generics with default arguments, e.g. in the TypedTable
    constructor above.
    """

    def _item_ident(item: Item) -> Item:
        return item

    return TypedTable(table_name, deepcopy, _item_ident, type_name=item_name)


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
