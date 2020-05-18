

def add_variables_to_expression(query_dict: dict, variables: dict) -> dict:
    """Attempt to make it easier to develop a query"""
    ea_names = query_dict.get("ExpressionAttributeNames", {})
    ea_values = query_dict.get("ExpressionAttributeValues", {})
    for k, v in variables.items():
        name = f"#{k}"
        if name in ea_names:
            raise ValueError(
                f"Cannot add a duplicate expression attribute "
                f"name {name} to your query {query_dict}"
            )
        ea_names[name] = k
        name = f":{k}"
        if name in ea_values:
            raise ValueError(
                f"Cannot add a duplicate expression attribute "
                f"value {name} to your query {query_dict}"
            )
        ea_values[name] = v
    query_dict["ExpressionAttributeNames"] = ea_names
    query_dict["ExpressionAttributeValues"] = ea_values
    return query_dict
