### 1.10.1

- Fixed bug in `versioned_transact_write_items` where un-effected
  items would have an incorrect ConditionExpression generated, causing
  the transaction to fail.

## 1.10.0

- `versioned_transact_write_items` now allows the transaction builder
  itself to lazily load items from any table via `get`/`require`,
  instead of requiring all transacted items to be explicitly declared
  before the builder is invoked.
- Experimental support for optimistic creations and deletions as
  well - if an item being written has not been prefetched, it will be
  assumed to not exist for the purposes of the condition check in the
  Put or Delete within the transaction. If that optimism proves
  invalid, the transaction will be retried after a refetch of the
  actual item. The limitation is that we need to be able to somehow
  derive a key for your item, so either you need to have prefetched a
  different item from the same table, or your environment must allow
  access to the DescribeTable action for the DynamoDB table, so that
  we can directly check its key schema.

## 1.9.0

- New `versioned_transact_write_items` wrapper that presents a general
  purpose interface for versioned transactional Puts and Deletes
  across tables and items, reverting to simple versioned writes for
  cost optimization when only a single item is transacted.

## 1.8.0

- Improved utilities and types for CloudWatch Metrics.

### 1.7.1

- Safer long-polling default for the SQS poll utility.

## 1.7.0

- `versioned_diffed_update_item` more fully supports the use of the
  `nicename` keyword argument, providing more specific Exceptions in
  the case of the item not existing before the update.
- Improved typing for various query helpers in `xoto3.dynamodb.query`.

## 1.6.0

- FailedRecordExceptions now contain the entire exception for each
  failed record. This is a very minor feature.
- Removed some unnecessarily verbose logging to put library users back
  in control.

## 1.5.0

- `map_tree` now supports postorder transformations via keyword argument.

### 1.4.0

- Improved DynamoDB Item-related Exceptions for `GetItem`,
  `put_but_raise_if_exists`, and `versioned_diffed_update_item`.

### 1.3.3

- Allow any characters for attribute names in `add_variables_to_expression`.
  - We have a lot of snake_cased attribute names. We should be able to use this function with those.

### 1.3.2

- Addressed theoretical weakness in expression attribute naming by
  appending hashes of the raw attribute name rather than incrementing
  counts.

### 1.3.1

- addressed bug in `add_variables_to_expression` which was resulting in
  the generation of invalid queries.
- reverted behavior from `add_variables_to_expression` which might have
  resulted in queries being created with unreferenced dynamically suffixed
  AWS placeholders should an existing placeholder with the same name exist
  -- replaced with code to validate names instead.

### 1.3.0

- added `put_or_return_existing` utility.

### 1.2.1

- DynamoDB utilities now correctly use alphanumeric
  ExpressionAttributeNames. It would appear that DynamoDB only
  enforces this restriction selectively; we ran into it when trying to
  REMOVE an attribute during an update; SET does not seem to
  experience these issues.

## 1.2.0

- `build_update_diff` now also performs automatic type coercion on the
  'new' item in the case of datetimes, tuples, floats, and removing
  empty sets.

### 1.1.2

- Fixed bug in `versioned_diffed_update_item` preventing it from being
  used to create items that did not previously exist.
- Improved docstrings and documentation.

### 1.1.1

Within `xoto3.dynamodb`:

- Fixed `batch_write` imports.
- `versioned_diffed_update_item` now performs standard
  `boto3`-required data fixups on transformed items by default, but
  this behavior can be customized.
- Fixed return type of `put_unless_exists`
- Fixed cases where reserved words used as table primary key attribute
  names could cause `put_unless_exists` to fail with a
  ValidationException.

## 1.1.0

- Added `require_index` utility to `dynamodb.query`
- New, clearer name `page` to replace `From` in `dynamodb.query`. The
  previous name remains but is a deprecated alias.
- Made `dynamodb.batch_get.items_only` a bit more generic.

### 1.0.3

Fixed install_requires for Python > 3.6

### 1.0.2

Within `xoto3.dynamodb`:

- Fixed return value for `put_unless_exists`.
- Fixed type for `query.From`

### 1.0.1

Corrected naming in utility.

# 1.0.0

Initial release
