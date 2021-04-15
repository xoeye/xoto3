# versioned_transact_write_items

This is a general-purpose system for writing retrying DynamoDB
transactional operations for 1 to
[25](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-dynamodb-transactions)
items, using pure functions.

The magic sauce here is the versioning - we use ConditionExpressions
based on a specified attribute, by default `item_version`, to detect
whether any intervening writes have been made, such that an `update`
can safely be expressed as a simple `put`. This makes the
[ABA problem](https://en.wikipedia.org/wiki/ABA_problem) impossible,
so you can write your business logic free of worry about the
complexity of transactions in the real world.

The API follows a consistent pattern of using provided functions to
modify an opaque, immutable VersionedTransaction object. All writes to
the database are performed by calling a function with the current
state of the versioned transaction, resulting in a new
VersionedTransaction object that may be further evolved using other
provided functions. Crucially, the API exposes no 'state' about the
transaction except for via simple read and modify operations,
resulting in a fully composable transaction system - any transaction
builder can be composed with any other in whatever logical order is
necessary.

This API provides partial-application-by-default, because that makes
certain use cases much cleaner to express. This means that you'll be
writing things like `table.get(key)(transaction)` instead of
`table.get(key, transaction)`, i.e. using more parentheses rather than
commas.

### put unless exists

As a simple but non-trivial example, you can perform a put-unless-exists like so:

```python
import xoto3.dynamodb.write_versioned as wv

# construct these at a module level and share them - they are
# threadsafe because they are pure values.
task_table =
wv.ItemTable('TaskTable', nicename='Task') user_table =
wv.ItemTable('UserTable', nicename='User')

# imagine these were passed in via an API
user_key = dict(id='steve')
task_key = dict(id='task1')
new_task = dict(task_key, name='plant tree', is_done=False, user_id='steve')

# this transaction builder is a pure function that constructs
# a value representing DynamoDB effects to be transactionally applied.
# It will be called one to many times until its logic is successfully
# applied to the items, or a (configurable) timeout is reached.
def create_task_unless_exists(vt: wv.VersionedTransaction) -> wv.VersionedTransaction:
    vt = task_table.presume(task_key, None)(vt)
    # ^ we presume that the task does not exist
    # - this is a no-op if a value has already been fetched
    existing_task = task_table.get(task_key)(vt)
    if existing_task is None:
        # ^ item is None means the task does not exist
        vt = task_table.put(new_task)(vt)
        # ^ put this item into this table as long as the item transactionally does not exist
        user = user_table.require(user_key)(vt)
        # ^ fetch the user and raise an exception if it does not exist
        user['task_ids'].append(task_key['id'])
        vt = user_table.put(user)(vt)
        # ^ make sure that the user knows about its task
        return vt
        # ^ return the built transaction to be executed.
        # if anything has changed in the meantime,
        # our transaction builder function will be run again with the latest data.
    return vt
    # ^ perform no action as long as the item transactionally already exists

# this call will cause the transaction builder function above
# to be transactionally/atomically applied to the database.
wv.versioned_transact_write_items(
    create_task_unless_exists,
    attempts_iterator=wv.timed_retry(timedelta(seconds=22.3)
)
```

### Prefetching items to transact

In many cases you will wish you prefetch some items that you do expect
to exist, since you plan to update them. Items that have not been
prefetched will be fetched at the time of the call to `wv.get` or
`wv.require` in a running transaction, but these fetches will be done
serially, so it is more efficient to provide their keys
upfront. `wv.put` and `wv.delete` will not perform a
read-before-write, but will instead optimistically attempt the write,
assuming that the item does not exist until proven otherwise.

To perform those prefetches, simply provide a set of keys mapped onto
by table names, as so:

```python
wv.versioned_transact_write_items(
    transfer_task_ownership,
    dict(
        TaskTable=[task_key1, task_key2],
        UserTable=[current_owner_key, new_owner_key],
    )
)
```
