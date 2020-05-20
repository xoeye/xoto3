import typing as ty

from botocore.exceptions import ClientError
import pytest

from xoto3.dynamodb.types import ItemKey, AttrDict, Item, TableResource
from xoto3.dynamodb.update.versioned import (
    versioned_diffed_update_item,
    VersionedUpdateFailure,
    DEFAULT_MAX_ATTEMPTS_BEFORE_FAILURE,
)
import xoto3.dynamodb.update.versioned as xdv

xdv.MAX_TRANSACTION_SLEEP = 0.0  # no sleeps for the test


class FakeTableResource(TableResource):
    @property
    def name(self):
        return "Fake"


def test_versioned_diffed_update_item():

    test_item: Item = dict(id="foo", to_remove="blah")

    def test_transform(item: Item) -> Item:
        item.pop("to_remove")
        item["new"] = "value"
        return item

    called_times = [0]

    def updater_func(
        Table: TableResource,
        Key: ItemKey,
        set_attrs: ty.Optional[AttrDict] = None,
        remove_attrs: ty.Collection[str] = (),
        add_attrs: ty.Optional[AttrDict] = None,
        delete_attrs: ty.Optional[AttrDict] = None,
        **update_args,
    ) -> Item:
        """"""
        called_times[0] += 1
        assert set_attrs and "new" in set_attrs
        assert remove_attrs and "to_remove" in remove_attrs
        assert "item_version" in set_attrs  # assert that the item version actually gets included...
        assert "last_written_at" in set_attrs
        return dict()

    item_version_1 = versioned_diffed_update_item(
        FakeTableResource(),
        test_transform,
        test_item,
        get_item=lambda x, y: test_item,
        update_item=updater_func,
    )

    assert 1 == called_times[0]
    assert 1 == item_version_1["item_version"]
    called_times[0] = 0

    # update this item a second time
    def new_new_transform(item: Item) -> Item:
        item["newnew"] = "newnewnewnewnewn"
        return item

    def updater_func_2(
        Table: TableResource,
        Key: ItemKey,
        set_attrs: ty.Optional[AttrDict] = None,
        remove_attrs: ty.Collection[str] = (),
        add_attrs: ty.Optional[AttrDict] = None,
        delete_attrs: ty.Optional[AttrDict] = None,
        **update_args,
    ) -> Item:
        """"""
        called_times[0] += 1
        assert set_attrs and "newnew" in set_attrs
        assert "item_version" in set_attrs  # assert that the item version actually gets included...
        assert "last_written_at" in set_attrs
        return dict()

    item_version_2 = versioned_diffed_update_item(
        FakeTableResource(),
        new_new_transform,
        item_version_1,
        get_item=lambda x, y: item_version_1,
        update_item=updater_func_2,
    )

    assert 1 == called_times[0]
    called_times[0] = 0
    assert 2 == item_version_2["item_version"]

    def test_no_transform(item: Item) -> ty.Optional[Item]:
        return None

    versioned_diffed_update_item(
        FakeTableResource(),
        test_no_transform,
        test_item,
        get_item=lambda x, y: test_item,
        update_item=updater_func,
    )

    assert 0 == called_times[0]
    called_times[0] = 0

    def fail_first_updater_func(
        Table: TableResource,
        Key: ItemKey,
        set_attrs: ty.Optional[AttrDict] = None,
        remove_attrs: ty.Collection[str] = (),
        add_attrs: ty.Optional[AttrDict] = None,
        delete_attrs: ty.Optional[AttrDict] = None,
        **update_args,
    ) -> Item:
        """"""
        called_times[0] += 1
        if called_times[0] == 1:
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "update_item")
        return dict()

    versioned_diffed_update_item(
        FakeTableResource(),
        test_transform,
        test_item,
        get_item=lambda x, y: test_item,
        update_item=fail_first_updater_func,
    )

    assert 2 == called_times[0]

    called_times[0] = 0

    def fail_forever_updater_func(
        Table: TableResource,
        Key: ItemKey,
        set_attrs: ty.Optional[AttrDict] = None,
        remove_attrs: ty.Collection[str] = (),
        add_attrs: ty.Optional[AttrDict] = None,
        delete_attrs: ty.Optional[AttrDict] = None,
        **update_args,
    ) -> Item:
        """"""
        called_times[0] += 1
        assert set_attrs and "item_version" in set_attrs
        raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "update_item")

    with pytest.raises(VersionedUpdateFailure):
        versioned_diffed_update_item(
            FakeTableResource(),
            test_transform,
            test_item,
            get_item=lambda x, y: test_item,
            update_item=fail_forever_updater_func,
        )

    assert called_times[0] == DEFAULT_MAX_ATTEMPTS_BEFORE_FAILURE


def test_string_item_version_coercion_to_int():
    ITEM_VERSION_STR = "1"
    test_item: Item = dict(id="foo", to_remove="blah", item_version=ITEM_VERSION_STR)

    def test_transform(item: Item) -> Item:
        item.pop("to_remove")
        item["new"] = "value"
        return item

    called_times = [0]

    def updater_func(
        Table: TableResource,
        Key: ItemKey,
        set_attrs: ty.Optional[AttrDict] = None,
        remove_attrs: ty.Collection[str] = (),
        add_attrs: ty.Optional[AttrDict] = None,
        delete_attrs: ty.Optional[AttrDict] = None,
        **update_args,
    ) -> Item:
        """"""
        called_times[0] += 1
        assert update_args["ExpressionAttributeValues"][":cur_item_version"] == ITEM_VERSION_STR
        return dict()

    item_version_1 = versioned_diffed_update_item(
        FakeTableResource(),
        test_transform,
        test_item,
        get_item=lambda x, y: test_item,
        update_item=updater_func,
    )

    assert 1 == called_times[0]
    assert 2 == item_version_1["item_version"]
    # called_times[0] = 0


def test_update_only_versioning_expression():
    assert xdv.versioned_item_expression(2, id_that_exists="id") == dict(
        ExpressionAttributeNames={"#item_version": "item_version", "#id_that_exists": "id"},
        ExpressionAttributeValues={":cur_item_version": 2},
        ConditionExpression="#item_version = :cur_item_version OR ( attribute_not_exists(#item_version) AND attribute_exists(#id_that_exists) )",
    )


def test_allow_create_versioning_expression():
    assert xdv.versioned_item_expression(0) == dict(
        ExpressionAttributeNames={"#item_version": "item_version"},
        ExpressionAttributeValues={":cur_item_version": 0},
        ConditionExpression="#item_version = :cur_item_version OR attribute_not_exists(#item_version)",
    )


def test_make_prefetched_get_item():
    item = dict(got=False)

    def refetch(tb, key):
        return dict(got=True)

    prefetched_getter = xdv.make_prefetched_get_item(item, refetch)

    assert prefetched_getter(None, dict()) == item
    assert prefetched_getter(None, dict()) == dict(got=True)
    assert prefetched_getter(None, dict()) == dict(got=True)
