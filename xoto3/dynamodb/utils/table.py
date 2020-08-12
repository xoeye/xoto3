from typing import Tuple
from xoto3.dynamodb.types import TableResource


def table_primary_keys(table: TableResource) -> Tuple[str, ...]:
    return tuple([key["AttributeName"] for key in table.key_schema])
