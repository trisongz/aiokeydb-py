
"""
Implements the PickleSerializer class.

- uses the pickle module to serialize and deserialize data
- will use `dill` if it is installed
"""
import sys
import pickle
import typing
import binascii
import contextlib
from io import BytesIO
from aiokeydb.types.serializer import BaseSerializer
from pickle import DEFAULT_PROTOCOL, Pickler, Unpickler


if sys.version_info.minor < 8:
    with contextlib.suppress(ImportError):
        import pickle5 as pickle

try:
    import dill
    from dill import DEFAULT_PROTOCOL as DILL_DEFAULT_PROTOCOL
    _dill_avail = True
except ImportError:
    dill = object
    _dill_avail = False


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

class PickleSerializerv2(BaseSerializer):

    @staticmethod
    def dumps(obj: typing.Any, protocol: int = DEFAULT_PROTOCOL, *args, **kwargs) -> str:
        """
        v2 Encoding
        """
        f = BytesIO()
        p = Pickler(f, protocol = protocol)
        p.dump(obj)
        return f.getvalue().hex()

    @staticmethod
    def loads(data: typing.Union[str, typing.Any], *args, **kwargs) -> typing.Any:
        """
        V2 Decoding
        """
        return Unpickler(BytesIO(binascii.unhexlify(data))).load()

if _dill_avail:
    from dill import Pickler as DillPickler, Unpickler as DillUnpickler

    class DillSerializer(BaseSerializer):

        @staticmethod
        def dumps(obj: typing.Any, protocol: int = DefaultProtocols.dill, *args, **kwargs) -> bytes:
            return dill.dumps(obj, protocol = protocol, *args, **kwargs)

        @staticmethod
        def loads(data: typing.Union[str, bytes, typing.Any], *args, **kwargs) -> typing.Any:
            return dill.loads(data, *args, **kwargs)
        
    class DillSerializerv2(BaseSerializer):

        @staticmethod
        def dumps(obj: typing.Any, protocol: int = DILL_DEFAULT_PROTOCOL, *args, **kwargs) -> str:
            """
            v2 Encoding
            """
            f = BytesIO()
            p = DillPickler(f, protocol = protocol)
            p.dump(obj)
            return f.getvalue().hex()

        @staticmethod
        def loads(data: typing.Union[str, typing.Any], *args, **kwargs) -> typing.Any:
            """
            V2 Decoding
            """
            return DillUnpickler(BytesIO(binascii.unhexlify(data))).load()

else:
    DillSerializer = PickleSerializer
    DillSerializerv2 = PickleSerializerv2

try:
    import cloudpickle
    from types import ModuleType

    class CloudPickleSerializer(BaseSerializer):

        @staticmethod
        def dumps(obj: typing.Any, protocol: int = cloudpickle.DEFAULT_PROTOCOL, *args, **kwargs) -> bytes:
            """
            Dumps an object to bytes
            """
            return cloudpickle.dumps(obj, protocol = protocol, *args, **kwargs)

        @staticmethod
        def loads(data: typing.Union[str, bytes, typing.Any], *args, **kwargs) -> typing.Any:
            """
            Loads an object from bytes
            """
            return cloudpickle.loads(data, *args, **kwargs)
        
        @staticmethod
        def register_module(module: ModuleType):
            """
            Registers a module with cloudpickle
            """
            cloudpickle.register_pickle_by_value(module)

        @staticmethod
        def unregister_module(module: ModuleType):
            """
            Registers a class with cloudpickle
            """
            cloudpickle.unregister_pickle_by_value(module)


except ImportError:
    CloudPickleSerializer = PickleSerializer