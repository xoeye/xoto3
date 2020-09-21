import pytest

from xoto3.dynamodb.exceptions import (
    get_item_exception_type,
    raise_if_empty_getitem_response,
    ItemNotFoundException,
    ItemAlreadyExistsException,
    AlreadyExistsException,
)


def test_dynamically_named_exceptions_names_and_caches_different_types():
    media_not_found = get_item_exception_type("Media", ItemNotFoundException)

    assert media_not_found.__name__ == "MediaNotFoundException"
    assert issubclass(media_not_found, ItemNotFoundException)

    assert get_item_exception_type("Media", ItemNotFoundException) is media_not_found

    media_already_exists = get_item_exception_type("Media", ItemAlreadyExistsException)
    assert issubclass(media_already_exists, AlreadyExistsException)
    assert media_already_exists.__name__ == "MediaAlreadyExistsException"
    assert not issubclass(media_already_exists, ItemNotFoundException)


def test_raises_uses_nicename():
    with pytest.raises(ItemNotFoundException) as infe_info:
        raise_if_empty_getitem_response(dict(), nicename="Duck")
    assert infe_info.value.__class__.__name__ == "DuckNotFoundException"


def test_raises_includes_key_and_table_name():
    with pytest.raises(ItemNotFoundException) as infe_info:
        raise_if_empty_getitem_response(
            dict(), nicename="Plant", table_name="Greenhouse", key=dict(id="p0001")
        )
    assert infe_info.value.key == dict(id="p0001")
    assert infe_info.value.table_name == "Greenhouse"
