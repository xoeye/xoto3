import typing as ty

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer


__ds = TypeDeserializer()
__sr = TypeSerializer()


def deserialize_item(d: dict) -> dict:
    """Dynamo has crazy serialization and they don't always get rid of it for us."""
    return {k: __ds.deserialize(d[k]) for k in d}


def serialize_item(d: dict) -> dict:
    return {k: __sr.serialize(d[k]) for k in d}


def old_dynamodb_stringset_fix(Set: set) -> ty.Optional[set]:
    """DynamoDB used to disallow the empty string within a StringSet"""
    if all(map(lambda x: isinstance(x, str) or x is None, Set)):
        # DynamoDB will not accept the empty string or None in a StringSet
        return {s for s in Set if s} or None  # don't ever return the empty set
    return Set


def dynamodb_prewrite_set_transform(Set: set) -> ty.Optional[set]:
    """DynamoDB will not accept Sets with empty strings, or empty Sets."""
    if not Set:
        # DynamoDB expects None if your set is empty
        return None
    # no guarantees your set is well-formed, but this covers some of the simple cases
    return Set


def dynamodb_prewrite_empty_str_in_dict_to_null_transform(d: dict) -> dict:
    """DynamoDB will break if you try to provide an empty string as a
    String value of a key that is used as an index. It requires you to
    provide these attributes as None rather than the empty string.

    It _used_ to break if any attribute in any Map (nested or not) was the
    empty String. This behavior seems to have changed relatively recently.

    This function guards against this issue by simply replacing the
    empty string with None.

    """
    return {k: (v if not (isinstance(v, str) and not v) else None) for k, v in d.items()}
