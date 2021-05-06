import pytest

from xoto3.dynamodb.streams.records import (
    ItemCreated,
    ItemDeleted,
    ItemModified,
    current_nonempty_value,
    matches_key,
    old_and_new_dict_tuples_from_stream,
)
from xoto3.dynamodb.utils.serde import serialize_item


def _serialize_record(record: dict):
    return {k: serialize_item(image) for k, image in record.items()}


def _serialize_records(*records):
    return [dict(dynamodb=_serialize_record(rec["dynamodb"])) for rec in records]


def _fake_stream_event():
    return dict(
        Records=_serialize_records(
            dict(dynamodb=dict(NewImage=dict(id=1, val=2))),
            dict(dynamodb=dict(NewImage=dict(id=1, val=3), OldImage=dict(id=1, val=2))),
            dict(dynamodb=dict(NewImage=dict(id=2, bar=8), OldImage=dict(id=2, bar=-9))),
            dict(dynamodb=dict(NewImage=dict(id=1, val=4), OldImage=dict(id=1, val=3))),
            dict(dynamodb=dict(NewImage=dict(id=2, foo="steve"), OldImage=dict(id=2, bar=8))),
            dict(dynamodb=dict(OldImage=dict(id=1, val=4))),
        )
    )


def test_current_nonempty_value():
    list_of_images = old_and_new_dict_tuples_from_stream(_fake_stream_event())

    assert [dict(id=1, val=2), dict(id=1, val=3), dict(id=1, val=4)] == list(
        current_nonempty_value(dict(id=1))(list_of_images)
    )


def test_matches_key_fails_with_empty_key():
    with pytest.raises(ValueError):
        matches_key(dict())


def test_matches_key_works_on_new_as_well_as_old():
    assert not matches_key(dict(id=3))(ItemCreated(None, dict(id=4)))
    assert not matches_key(dict(id=3))(ItemDeleted(dict(id=4), None))
    assert not matches_key(dict(hash=1, range=3))(
        ItemModified(dict(hash=1, range=4, foo=0), dict(hash=1, range=4, foo=1))
    )
