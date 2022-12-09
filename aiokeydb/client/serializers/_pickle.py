
"""
Implements the PickleSerializer class.

- uses the pickle module to serialize and deserialize data
- will use `dill` if it is installed
"""
import sys
import pickle
import typing
import contextlib

if sys.version_info.minor < 8:
    with contextlib.suppress(ImportError):
        import pickle5 as pickle

try:
    import dill
    _dill_avail = True
except ImportError:
    dill = object
    _dill_avail = False

from aiokeydb.client.serializers.base import BaseSerializer

class DefaultProtocols:
    default: int = 4
    pickle: int = pickle.HIGHEST_PROTOCOL
    dill: int = dill.HIGHEST_PROTOCOL

class PickleSerializer(BaseSerializer):

    @staticmethod
    def dumps(obj: typing.Any, protocol: int = DefaultProtocols.pickle, *args, **kwargs) -> bytes:
        return pickle.dumps(obj, protocol = protocol, *args, **kwargs)

    @staticmethod
    def loads(data: typing.Union[str, bytes, typing.Any], *args, **kwargs) -> typing.Any:
        return pickle.loads(data, *args, **kwargs)
    
if _dill_avail:
    class DillSerializer(BaseSerializer):

        @staticmethod
        def dumps(obj: typing.Any, protocol: int = DefaultProtocols.dill, *args, **kwargs) -> bytes:
            return dill.dumps(obj, protocol = protocol, *args, **kwargs)

        @staticmethod
        def loads(data: typing.Union[str, bytes, typing.Any], *args, **kwargs) -> typing.Any:
            return dill.loads(data, *args, **kwargs)

else:
    DillSerializer = PickleSerializer
