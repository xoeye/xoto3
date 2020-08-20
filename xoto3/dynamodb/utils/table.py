from typing import Tuple
from xoto3.dynamodb.types import TableResource, Item, ItemKey


def table_primary_keys(table: TableResource) -> Tuple[str, ...]:
    return tuple([key["AttributeName"] for key in table.key_schema])


def extract_key_from_item(table: TableResource, item: Item) -> ItemKey:
    return {attr_name: item[attr_name] for attr_name in table_primary_keys(table)}
