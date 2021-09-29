"""Imagine you had the following business logic and you wanted to test
it. Here's how you might go about that, using the
xoto3.dynamodb.write_versioned API itself to set everything up and
prove that it worked.

The example here is the same as the code we have in the README.
"""
import pytest

import xoto3.dynamodb.write_versioned as wv

# threadsafe because they are pure values.
task_table = wv.ItemTable("TaskTable", item_name="Task")
user_table = wv.ItemTable("UserTable", item_name="User")

# imagine these were passed in via an API
task_key = dict(id="task1")
new_task = dict(task_key, name="plant tree", num_subtasks=3, user_id="steve")

# this transaction builder is a pure function that constructs
# a value representing DynamoDB effects to be transactionally/atomically applied.
def create_and_link_task_unless_exists(t: wv.VersionedTransaction) -> wv.VersionedTransaction:
    t = task_table.presume(task_key, None)(t)
    # ^ we presume that the task does not exist, i.e. has the value None
    # This is a no-op if a value has already been fetched.
    # This is also purely an optimization to avoid an initial read;
    # the whole transaction would result in the same data in the table without it.

    existing_task = task_table.get(task_key)(t)
    if existing_task:
        return t
        # ^ perform no action as long as the item already exists

    t = task_table.put(new_task)(t)
    # ^ put this item into this table as long as the item does not exist
    user = user_table.require(dict(id=new_task["user_id"]))(t)  # type: ignore
    # ^ fetch the user and raise an exception if it does not exist
    user["task_ids"] = (user.get("task_ids") or list()) + [task_key["id"]]
    t = user_table.put(user)(t)
    # ^ make sure that the user knows about its task
    return t
    # ^ return the built transaction to be executed.


def test_task_and_user_are_not_written_if_task_already_exists():
    t = wv.VersionedTransaction(dict())
    existing_task = dict(task_key, name="water flowers", is_done=True, user_id="felicity")
    t = task_table.presume(task_key, existing_task)(t)
    # presume lets us set up a fixture in the fake table, represented by the VersionedTransaction

    t = create_and_link_task_unless_exists(t)

    assert existing_task == task_table.require(task_key)(t)
    # the task remains unchanged

    with pytest.raises(wv.ItemUndefinedException):
        user_table.get(dict(id="felicity"))(t)
        # the user was not even fetched, much less written


def test_task_and_user_are_both_written_if_task_does_not_exist():
    t = wv.VersionedTransaction(dict())
    t = task_table.presume(task_key, None)(t)
    # declare that the task does not exist - essentially this is a 'fixture', but without I/O
    user_key = dict(id="steve")
    user = dict(user_key, name="Actually Steve")
    t = user_table.presume(user_key, user)(t)

    t = create_and_link_task_unless_exists(t)

    resulting_task = task_table.require(task_key)(t)
    resulting_user = user_table.require(user_key)(t)
    assert resulting_task == new_task
    assert resulting_user["name"] == "Actually Steve"
    assert resulting_user["task_ids"] == ["task1"]
