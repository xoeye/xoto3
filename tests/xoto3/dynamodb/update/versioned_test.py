import typing as ty

import pytest
from botocore.exceptions import ClientError

import xoto3.dynamodb.update.versioned as xdv
from xoto3.dynamodb.exceptions import ItemNotFoundException, get_item_exception_type
from xoto3.dynamodb.types import AttrDict, Item, ItemKey, TableResource
from xoto3.dynamodb.update.versioned import (
    DEFAULT_MAX_ATTEMPTS_BEFORE_FAILURE,
    VersionedUpdateFailure,
    versioned_diffed_update_item,
)

xdv.MAX_TRANSACTION_SLEEP = 0.0  # no sleeps for the test


class FakeTableResource(TableResource):
    @property
    def name(self):
        return "Fake"


def test_versioned_diffed_update_item():

    test_item: Item = dict(id="foo", to_remove="blah")

    def test_transform(item: Item) -> Item:
        item.pop("to_remove")
        item["new"] = "abcxyz"
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

    with pytest.raises(VersionedUpdateFailure) as ve_info:
        versioned_diffed_update_item(
            FakeTableResource(),
            test_transform,
            test_item,
            get_item=lambda x, y: dict(test_item, item_version=called_times[0]),
            update_item=fail_forever_updater_func,
        )
    ve = ve_info.value
    assert ve.table_name == "Fake"
    assert ve.key == test_item
    assert ve.update_arguments["remove_attrs"] == {"to_remove"}
    assert ve.update_arguments["set_attrs"]["item_version"] == 25
    assert ve.update_arguments["set_attrs"]["new"] == "abcxyz"

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
        assert update_args["ExpressionAttributeValues"][":curItemVersion"] == ITEM_VERSION_STR
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
        ExpressionAttributeNames={"#itemVersion": "item_version", "#idThatExists": "id",},
        ExpressionAttributeValues={":curItemVersion": 2},
        ConditionExpression="#itemVersion = :curItemVersion OR ( attribute_not_exists(#itemVersion) AND attribute_exists(#idThatExists) )",
    )


def test_allow_create_versioning_expression():
    assert xdv.versioned_item_expression(0) == dict(
        ExpressionAttributeNames={"#itemVersion": "item_version"},
        ExpressionAttributeValues={":curItemVersion": 0},
        ConditionExpression="#itemVersion = :curItemVersion OR attribute_not_exists(#itemVersion)",
    )


def test_make_prefetched_get_item():
    item = dict(got=False)

    def refetch(tb, key):
        return dict(got=True)

    prefetched_getter = xdv.make_prefetched_get_item(item, refetch)

    assert prefetched_getter(None, dict()) == item
    assert prefetched_getter(None, dict()) == dict(got=True)
    assert prefetched_getter(None, dict()) == dict(got=True)


def test_nicename_getter(integration_test_id_table):
    TestItemNotFoundException = get_item_exception_type("TestItem", ItemNotFoundException)

    def no_up(item):
        return item

    with pytest.raises(TestItemNotFoundException):
        versioned_diffed_update_item(
            integration_test_id_table, no_up, dict(id="should-never-exist"), nicename="TestItem",
        )
