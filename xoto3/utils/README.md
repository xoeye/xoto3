## Utilities not requiring `boto3`

### dec

Convert `decimal.Decimal` to and from `int`/`float`

### dt

More consistent string ISO8601 datetime format

### lazy

General purpose Lazy-loading wrapper with static type checking.

### pipe_multiprocessing

A Process Pool for places where Python's built-in shared memory-dependent Pool
does not work, such as AWS Lambdas. Sure, you can only get ~2 VCPUs
with max RAM, but 2 is better than 1 if you need to crunch some
numbers (or generate lots of presigned URLs)!

### tree_map

General purpose recursive map over the four standard Python builtin
collection types (`dict`, `list`, `set`, `tuple`).
