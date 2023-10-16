
"""
Base Serializer Class that all other serializers should inherit from.
"""

import typing

class BaseSerializer:

    @staticmethod
    def dumps(obj: typing.Any, **kwargs) -> bytes:
        """
        Serialize the object to bytes
        """
        raise NotImplementedError
    
    @staticmethod
    def loads(data: typing.Union[str, bytes, typing.Any], **kwargs) -> typing.Any:
        """
        Deserialize the object from bytes
        """
        raise NotImplementedError

