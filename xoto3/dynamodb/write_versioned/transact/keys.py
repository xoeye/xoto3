"""Private implementation details for versioned_transact_write_items"""

from typing import Collection, Sequence, Union

from xoto3.dynamodb.types import Item, ItemKey

from .types import HashableItemKey


def key_from_item(key_attributes: Collection[str], item: Union[Item, ItemKey]) -> ItemKey:
    return {attr_name: item[attr_name] for attr_name in sorted(key_attributes)}


def hashable_key(key: ItemKey) -> HashableItemKey:
    """We use only the values, either the raw value of a simplex key, or a
    tuple of the values of a complex key, sorted by the attribute names
    for consistency and comparability.
    """
    if len(key) == 1:
        # this keeps the data looking simpler at the cost of minor runtime complexity
        return tuple(key.values())[0]
    attr_names = sorted(key)
    assert len(attr_names) == 2
    return (key[attr_names[0]], key[attr_names[1]])


def hashable_key_to_key(key_attributes: Sequence[str], hashable_key: HashableItemKey) -> ItemKey:
    if isinstance(hashable_key, tuple):
        assert len(key_attributes) == len(hashable_key)
        return {key_attributes[0]: hashable_key[0], key_attributes[1]: hashable_key[1]}
    assert len(key_attributes) == 1
    return {key_attributes[0]: hashable_key}
