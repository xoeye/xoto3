### 1.1.0

- Added `require_index` utility to `dynamodb.query`
- New, clearer name `page` to replace `From` in `dynamodb.query`. The
  previous name remains but is a deprecated alias.
- Made `dynamodb.batch_get.items_only` a bit more generic.

### 1.0.3

Fixed install_requires for Python > 3.6

### 1.0.2

- Fixed return value for `put_unless_exists`.
- Fixed type for `xoto3.dynamodb.query.From`

### 1.0.1

Corrected naming in utility.

# 1.0.0

Initial release
