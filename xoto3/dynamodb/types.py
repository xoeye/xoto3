"""Basic types for DynamoDB data and operations"""
import typing as ty
from decimal import Decimal
from typing_extensions import TypedDict, Literal


KeyAttributeType = ty.Union[int, str, float, Decimal]
ItemKey = ty.Mapping[str, KeyAttributeType]

AttrInput = ty.Mapping[str, ty.Any]
InputItem = AttrInput

SchemaKeyAttibute = TypedDict("SchemaKeyAttibute", {"AttributeName": str})


class KeyAndType(TypedDict):
    AttributeName: str
    KeyType: ty.Union[Literal["HASH"], Literal["RANGE"]]


KeySchema = ty.List[KeyAndType]
PrimaryIndex = KeySchema


class SecondaryIndex(TypedDict):
    IndexName: str
    KeySchema: KeySchema


Index = ty.Union[PrimaryIndex, SecondaryIndex]


# pylint: disable=unused-argument,no-self-use


class TableResource:
    """A stub for a boto3 DynamoDB Table Resource.

    This can be updated as we use more methods from the type."""

    name: str

    key_schema: KeySchema

    global_secondary_indexes: ty.Optional[ty.List[SecondaryIndex]]

    local_secondary_indexes: ty.Optional[ty.List[SecondaryIndex]]

    def get_item(self, Key: ItemKey, **kwargs) -> dict:
        ...

    def update_item(self, TableName: str, Key: ItemKey, **kwargs) -> dict:
        ...

    def put_item(self, Item: InputItem, **kwargs) -> dict:
        ...

    def batch_writer(self, overwrite_by_pkeys: ty.Optional[ty.List[str]]) -> ty.ContextManager:
        ...

    def delete_item(self, Key: ItemKey, **kwargs) -> dict:
        ...

    def query(self, *args, **kwargs) -> dict:
        ...

    def scan(self, *args, **kwargs) -> dict:
        ...


AttrDict = ty.Dict[str, ty.Any]
Item = AttrDict

KeyTuple = ty.Tuple[KeyAttributeType, ...]

TableQuery = ty.Dict[str, ty.Any]
