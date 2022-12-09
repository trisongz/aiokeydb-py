
"""
Implements the MsgPack class.

- uses the `msgpack` module to serialize and deserialize data
"""

import typing

from aiokeydb.client.serializers.base import BaseSerializer

try:
    import msgpack
    _msgpack_avail = True
except ImportError:
    msgpack = object
    _msgpack_avail = False

if _msgpack_avail:

    class MsgPackSerializer(BaseSerializer):
        
        @staticmethod
        def dumps(
            obj: typing.Union[typing.Dict[typing.Any, typing.Any], typing.Any], 
            *args, 
            **kwargs
        ) -> typing.Union[bytes, str]:
            return msgpack.packb(obj, *args, **kwargs)
        
        @staticmethod
        def loads(
            data: typing.Union[str, bytes], 
            *args, 
            raw: bool = False,
            **kwargs
        ) -> typing.Any:
            return msgpack.unpackb(data, *args, raw = raw, **kwargs)

else:
    # Fallback to JSON
    from aiokeydb.client.serializers._json import JsonSerializer

    MsgPackSerializer = JsonSerializer