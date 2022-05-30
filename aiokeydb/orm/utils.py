
"""
Module containing common utilities

Copied from https://github.com/andrewthetechie/pydantic-aioredis/blob/main/pydantic_aioredis/utils.py
"""


def bytes_to_string(data: bytes):
    """Converts data to string"""
    return str(data, "utf-8") if isinstance(data, bytes) else data