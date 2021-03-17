import typing as ty

import pytest

from xoto3.dynamodb.types import Index, KeyAndType, SecondaryIndex, TableResource
from xoto3.dynamodb.utils.index import find_index, require_index


def _key_and_type(attr: str, key_type: str) -> dict:
    return dict(AttributeName=attr, KeyType=key_type)


def _table_resource(key_schema: ty.List[dict], lsis: ty.List[dict], gsis: ty.List[dict]):
    table = TableResource()
    table.name = "some-table"
    table.key_schema = [ty.cast(KeyAndType, k) for k in key_schema]
    table.local_secondary_indexes = [ty.cast(SecondaryIndex, l) for l in lsis]
    table.global_secondary_indexes = [ty.cast(SecondaryIndex, g) for g in gsis]
    return table


def _index(name: str, keys_and_types: ty.List[ty.Tuple[str, str]]) -> dict:
    return dict(
        IndexName=name, KeySchema=[_key_and_type(*key_and_type) for key_and_type in keys_and_types]
    )


def _find_secondary_index(
    table: TableResource, hash_key: str, range_key: str = ""
) -> SecondaryIndex:
    return ty.cast(SecondaryIndex, find_index(table, hash_key, range_key))


def test_find_index():
    primary_index = [(_key_and_type("id", "HASH"))]
    table = _table_resource(
        key_schema=primary_index,
        lsis=[
            _index("HASH-ONLY-LSI", [("hash-only-lsi", "HASH")]),
            _index("HASH-RANGE-LSI", [("some-hash-lsi", "HASH"), ("some-range-lsi", "RANGE")]),
        ],
        gsis=[
            _index("HASH-ONLY-GSI", [("hash-only-gsi", "HASH")]),
            _index("HASH-RANGE-GSI", [("some-hash-gsi", "HASH"), ("some-range-gsi", "RANGE")]),
        ],
    )

    def s(i: ty.Optional[Index]) -> SecondaryIndex:
        return ty.cast(SecondaryIndex, i)

    assert s(find_index(table, "some-hash-lsi", "some-range-lsi"))["IndexName"] == "HASH-RANGE-LSI"
    assert s(find_index(table, "hash-only-lsi"))["IndexName"] == "HASH-ONLY-LSI"
    assert find_index(table, "hash-only-lsi", "junk") is None

    assert s(find_index(table, "some-hash-gsi", "some-range-gsi"))["IndexName"] == "HASH-RANGE-GSI"
    assert s(find_index(table, "hash-only-gsi"))["IndexName"] == "HASH-ONLY-GSI"
    assert find_index(table, "hash-only-gsi", "junk") is None

    assert find_index(table, "id") == primary_index
    assert find_index(table, "junk") is None

    assert find_index(table, "junk", "junk") is None


def test_require_index():
    primary_index = [(_key_and_type("id", "HASH"))]
    table = _table_resource(primary_index, [], [])

    assert require_index(table, "id") == primary_index

    with pytest.raises(AssertionError, match="(hash)"):
        require_index(table, "hash")

    with pytest.raises(AssertionError, match="(hash, range)"):
        require_index(table, "hash", "range")


def test_require_index_no_range_key_integration_test(
    integration_test_id_table, integration_test_no_range_index_hash_key
):
    assert integration_test_id_table, "You must set NO_RANGE_INDEX_TABLE_NAME to run this test"
    assert require_index(integration_test_id_table, integration_test_no_range_index_hash_key)
