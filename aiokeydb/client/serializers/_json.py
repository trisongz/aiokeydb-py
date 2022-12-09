
"""
Implements the JsonSerializer class.

- uses the json module to serialize and deserialize data
"""


import json
import typing
import datetime
import dataclasses
import contextlib

from aiokeydb.client.serializers.base import BaseSerializer

try:
    import numpy as np
except ImportError:
    np = None


try:
    import orjson
    _orjson_avail = True
except ImportError:
    orjson = object
    _orjson_avail = False

def object_serializer(obj: typing.Any) -> typing.Any:
    if isinstance(obj, dict):
        return {k: object_serializer(v) for k, v in obj.items()}

    if isinstance(obj, bytes):
        return obj.decode('utf-8')

    if isinstance(obj, (str, list, dict, int, float, bool, type(None))):
        return obj

    if dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)

    if hasattr(obj, 'dict'): # test for pydantic models
        return obj.dict()
    
    if hasattr(obj, 'get_secret_value'):
        return obj.get_secret_value()
    
    if hasattr(obj, 'as_posix'):
        return obj.as_posix()
    
    if hasattr(obj, "numpy"):  # Checks for TF tensors without needing the import
        return obj.numpy().tolist()
    
    if hasattr(obj, 'tolist'): # Checks for torch tensors without importing
        return obj.tolist()
    
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    
    if np is not None:
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64)):
            return int(obj)
        
        if isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        
    else:
        # Try to convert to a primitive type
        with contextlib.suppress(Exception):
            return int(obj)
        with contextlib.suppress(Exception):
            return float(obj)

    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class ObjectEncoder(json.JSONEncoder):
    
    def default(self, obj: typing.Any):   # pylint: disable=arguments-differ,method-hidden
        with contextlib.suppress(Exception):
            return object_serializer(obj)
        return json.JSONEncoder.default(self, obj)


class JsonSerializer(BaseSerializer):

    @staticmethod
    def dumps(
        obj: typing.Union[typing.Dict[typing.Any, typing.Any], typing.Any], 
        *args, 
        default: typing.Union[typing.Dict[typing.Any, typing.Any], typing.Any] = None, 
        cls: typing.Type[json.JSONEncoder] = ObjectEncoder, 
        **kwargs
    ) -> str:
        return json.dumps(obj, *args, default = default, cls = cls, **kwargs)

    @staticmethod
    def loads(
        data: typing.Union[str, bytes], 
        *args, 
        **kwargs
    ) -> typing.Union[typing.Dict[typing.Any, typing.Any], typing.List[str], typing.Any]:
        return json.loads(data, *args, **kwargs)

if _orjson_avail:

    class OrJsonSerializer(BaseSerializer):
        
        @staticmethod
        def dumps(
            obj: typing.Union[typing.Dict[typing.Any, typing.Any], typing.Any], 
            *args, 
            default: typing.Union[typing.Dict[typing.Any, typing.Any], typing.Any] = None, 
            _serializer: typing.Callable = object_serializer, 
            **kwargs
        ) -> str:
            """
            We encode the data first using the object_serializer function
            """
            if _serializer: # pragma: no cover
                obj = _serializer(obj)
            return orjson.dumps(obj, default=default, *args, **kwargs).decode()
        
        @staticmethod
        def loads(
            data: typing.Union[str, bytes], 
            *args, 
            **kwargs
        ) -> typing.Union[typing.Dict[typing.Any, typing.Any], typing.List[str], typing.Any]:
            return orjson.loads(data, *args, **kwargs)

else:
    OrJsonSerializer = JsonSerializer


