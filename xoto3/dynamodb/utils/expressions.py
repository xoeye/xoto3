from typing import Collection, Iterable, Callable
import string


def _filter_alphanum(s: str) -> str:
    return "".join(c for c in s if c in string.ascii_letters or c in string.digits)


def _yield_randomly_extended_expr_attr_keys(start: str) -> Iterable[str]:
    start = _filter_alphanum(start)
    yield start
    count = 0
    while True:
        yield start + str(count)
        count += 1


def make_unique_expr_attr_key(
    attr_name: str,
    current_names: Collection[str] = (),
    tx_name: Callable[[str], str] = lambda x: ("#" + x),
) -> str:
    current = set(current_names)
    for attempt in _yield_randomly_extended_expr_attr_keys(attr_name):
        if tx_name(attempt) not in current:
            return attempt
    raise RuntimeError("Went off the end of an infinite generator")


def add_variables_to_expression(query_dict: dict, variables: dict) -> dict:
    """Attempt to make it easier to develop a query"""
    ea_names = query_dict.get("ExpressionAttributeNames", {})
    ea_values = query_dict.get("ExpressionAttributeValues", {})
    for k, v in variables.items():
        ea_key = make_unique_expr_attr_key(k, set(ea_names) | set(ea_values))
        name = f"#{ea_key}"
        if name in ea_names:
            raise ValueError(
                f"Cannot add a duplicate expression attribute "
                f"name {name} to your query {query_dict}"
            )
        ea_names[name] = name
        name = f":{ea_key}"
        if name in ea_values:
            raise ValueError(
                f"Cannot add a duplicate expression attribute "
                f"value {name} to your query {query_dict}"
            )
        ea_values[name] = v
    query_dict["ExpressionAttributeNames"] = ea_names
    query_dict["ExpressionAttributeValues"] = ea_values
    return query_dict
