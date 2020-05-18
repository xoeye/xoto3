"""These are partial types for parts of boto3 itself"""
import typing as ty
from decimal import Decimal
from typing_extensions import TypedDict, Literal

KeyAttributeType = ty.Union[int, str, float, Decimal]
ItemKey = ty.Mapping[str, KeyAttributeType]

AttrInput = ty.Mapping[str, ty.Any]
DynamoInputItem = AttrInput


SchemaKeyAttibute = TypedDict("SchemaKeyAttibute", {"AttributeName": str})


class KeyAndType(TypedDict):
    AttributeName: str
    KeyType: ty.Union[Literal["HASH"], Literal["RANGE"]]


class DynamoIndex(TypedDict):
    IndexName: str
    KeySchema: ty.List[KeyAndType]


class TableResource:
    """A stub for a boto3 DynamoDB Table Resource.

    This can be updated as we use more methods from the type."""

    name: str

    key_schema: ty.List[SchemaKeyAttibute]

    global_secondary_indexes: ty.Optional[ty.List[DynamoIndex]]

    local_secondary_indexes: ty.Optional[ty.List[DynamoIndex]]

    def get_item(self, Key: ItemKey, **kwargs) -> dict:
        ...

    def update_item(self, TableName: str, Key: ItemKey, **kwargs) -> dict:
        ...

    def put_item(self, Item: DynamoInputItem, **kwargs) -> dict:
        ...

    def batch_writer(self, overwrite_by_pkeys: ty.Optional[ty.List[str]]) -> ty.ContextManager:
        ...

    def delete_item(self, Key: ItemKey, **kwargs) -> dict:
        ...

    def query(self, *args, **kwargs) -> dict:
        ...

    def scan(self, *args, **kwargs) -> dict:
        ...
