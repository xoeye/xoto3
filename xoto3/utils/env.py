import os


def is_aws_env() -> bool:
    return "AWS_EXECUTION_ENV" in os.environ
