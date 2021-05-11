import pytest

from xoto3.dynamodb.write_versioned import VersionedTransaction
from xoto3.dynamodb.write_versioned.modify import TableSchemaUnknownError, delete


def test_cant_delete_non_prefetched_item_without_specifying_key():

    tx = delete(VersionedTransaction(dict()), "table1", dict(id="whatever"))

    tx = delete(tx, "table1", dict(id="yo", value=3, full_item=True))

    with pytest.raises(TableSchemaUnknownError):
        delete(tx, "table2", dict(id=4, value=7, other_value=9))
