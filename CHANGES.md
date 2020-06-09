### 1.2.0

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
