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
