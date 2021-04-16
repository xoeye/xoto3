## query

Meant to make writing DynamoDB queries easy and composable.

As an example, given a table named "Content" with an index HASH: `mediaType`, RANGE: `datetime`:

```
import boto3

import xoto3.dynamodb.utils.index as di
import xoto3.dynamodb.query as dq
import xoto3.dynamodb.paginate as dp


content_table = boto3.resource('dynamodb').Table('Content')

partition, in_range = dq.in_index(
    di.require_index(content_table, 'mediaType', 'datetime')
)

last_500_pngs = dq.pipe(
    dq.descending,
    dq.limit(500),
    in_range(gte='2020-03'),
)(partition('image/png'))

for item in dp.yield_items(content_table.query, last_500_pngs):
    print(item)
```

`yield_items` will automatically paginate all items for you.

## conditions

A few composable builders for common DynamoDB conditions are made available.

- `add_condition_attribute_exists`

```
ddb_args = dict(
    ConditionExpression="#itemVersion = :itemVersion",
    ExpressionAttributeNames={"#itemVersion": "item_version"},
    ExpressionAttributeValues={":itemVersion": 3},
)
add_condition_attribute_exists("id")(ddb_args) == {
  'ConditionExpression': '#itemVersion = :itemVersion AND attribute_exists(#_anc_name)',
  'ExpressionAttributeNames': {'#itemVersion': 'item_version', '#_anc_name': 'id'},
  'ExpressionAttributeValues': {':itemVersion': 3}
}
```

## batch_get

Auto-paginates a BatchGet.

```
import boto3
import xoto3.dynamodb.batch_get as bg

table = boto3.resource('dynamodb').Table('Content')

very_long_iterable_of_content_ids = (.....)

for key, item in bg.BatchGetItem(table, very_long_iterable_of_content_ids):
    if item:
        print(item)
    else:
        print('Item {key} not present')
```

## versioned_transact_write_items

You can write a transaction with boto3 directly, but what do you do when it fails?

This utility allows you to express your multi-item write operation as
a pure function with a simple API such that your code is pure business
logic, leaving the implementation details of fetching, attempting,
refetching, and eventually giving up to the utility.

```python
import xoto3.dynamodb.write_versioned as wv

user_table = wv.ItemTable('User')
group_table = wv.ItemTable('Group')
# ^ the above are convenience APIs and are pure utilities; they perform no
# IO and contain no mutable state or clients.

user_key = dict(id="bob")
group_key = dict(pk="team42")


def add_user_to_new_or_existing_group(t: wv.VersionedTransaction) -> wv.VersionedTransaction:
    user = user_table.require(user_key)(t)
    assert user, "require will raise ItemNotFoundException if the item does not exist"
    group = group_table.get(group_key)(t)

    if group_key not in user["groups"]:
        user["groups"].append(group_key)
        t = user_table.put(user)(t)

    if not group:
        group = dict(group_key, members=list())
    if user_key not in group["members"]:
        group["members"].append(user_key)

    if group != group_table.get(group_key):
        # if there was a change to the group
        t = wv.put(t, "Group", group)
    return t

wv.versioned_transact_write_items(
    add_user_to_new_or_existing_group,
    {
        "User": [user_key],
        "Group": [group_key],
    },
)
```

The above code will ensure that the 'state' of the collection of items
as defined by your function is fully realized without intervening
transactions, or retried with the new state of those items if the
transaction is beaten or otherwise interfered with.

For further documentation on this utility, see the full [readme](./write_versioned/README.md)
