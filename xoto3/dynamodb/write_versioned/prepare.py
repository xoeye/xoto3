from typing import Collection, Dict, Iterable, List, Mapping, Sequence, Set, Tuple, TypeVar, cast

from xoto3.dynamodb.types import Item, ItemKey, KeyAttributeType

from .keys import hashable_key, key_from_item
from .types import VersionedTransaction, _TableData


def _deduplicate_and_validate_keys(keys: Collection[ItemKey]) -> Iterable[ItemKey]:
    """All keys for a given table will need to be unique, and of course
    they must all share the same attribute names (or else they do not
    match the key schema for the table."""
    seen = set()
    key_attributes: Set[KeyAttributeType] = set()
    for key in keys:
        tk = tuple(key.items())
        if key_attributes:
            assert (
                set(key.keys()) == key_attributes
            ), f"Item keys must have identical attribute names. {key_attributes} != {set(key.keys())}"
        else:
            key_attributes = set(key.keys())
        if tk not in seen:
            seen.add(tk)
            yield key


def parse_batch_get_request(
    item_keys_by_table_name: Mapping[str, Collection[ItemKey]],
) -> Dict[str, List[ItemKey]]:
    return {
        table_name: list(_deduplicate_and_validate_keys(item_keys))
        # eliminate duplicate keys
        for table_name, item_keys in item_keys_by_table_name.items()
        if item_keys
        # eliminate tables with no keys
    }


def items_and_keys_to_clean_table_data(
    key_attributes: Tuple[str, ...], item_keys: Sequence[ItemKey], items: Sequence[Item],
) -> _TableData:
    hashable_keys = {hashable_key(key) for key in item_keys} | {
        hashable_key(key_from_item(key_attributes, item)) for item in items
    }
    existing_items_by_hashable_key = {
        hashable_key(key_from_item(key_attributes, item)): item for item in items
    }
    return _TableData(
        items={
            hashable_item_key: existing_items_by_hashable_key.get(hashable_item_key)
            for hashable_item_key in hashable_keys
        },
        effects=dict(),
        key_attributes=key_attributes,
    )


def _extract_key_attributes(item_keys: Sequence[ItemKey]) -> Tuple[str, ...]:
    assert item_keys, "You can't extract key_attributes without at least one item_key."
    return tuple(sorted(item_keys[0].keys()))


D = TypeVar("D", bound=dict)


def _drop_keys_with_empty_values(d: D) -> D:
    return cast(D, {key: v for key, v in d.items() if v})


def prepare_clean_transaction(
    item_keys_by_table_name: Mapping[str, Sequence[ItemKey]],
    response_items_by_table_name: Mapping[str, Sequence[Item]],
) -> VersionedTransaction:
    return VersionedTransaction(
        tables=_drop_keys_with_empty_values(
            {
                table_name: items_and_keys_to_clean_table_data(
                    _extract_key_attributes(item_keys),
                    item_keys,
                    response_items_by_table_name[table_name],
                )
                for table_name, item_keys in item_keys_by_table_name.items()
            }
        )
    )
