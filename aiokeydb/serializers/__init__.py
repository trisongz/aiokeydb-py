from __future__ import absolute_import

from enum import Enum
from typing import Type, TypeVar
from aiokeydb.types import BaseSerializer
from aiokeydb.serializers._json import JsonSerializer, OrJsonSerializer
from aiokeydb.serializers._pickle import PickleSerializer, DillSerializer, DillSerializerv2, PickleSerializerv2, CloudPickleSerializer
from aiokeydb.serializers._msgpack import MsgPackSerializer


SerializerT = TypeVar('SerializerT', bound = Type[BaseSerializer])

class SerializerType(str, Enum):
    """
    Enum for the available serializers
    """
    json = 'json'
    orjson = 'orjson'
    pickle = 'pickle'
    dill = 'dill'
    msgpack = 'msgpack'
    default = 'default'

    picklev2 = 'picklev2'
    dillv2 = 'dillv2'

    cloudpickle = 'cloudpickle'

    def get_serializer(self) -> SerializerT:
        """
        Default Serializer = Dill
        """

        if self == SerializerType.json:
            return JsonSerializer
        elif self == SerializerType.orjson:
            return OrJsonSerializer
        elif self == SerializerType.pickle:
            return PickleSerializer
        elif self == SerializerType.dill:
            return DillSerializer
        elif self == SerializerType.picklev2:
            return PickleSerializerv2
        elif self == SerializerType.dillv2:
            return DillSerializerv2
        elif self == SerializerType.cloudpickle:
            return CloudPickleSerializer
        elif self == SerializerType.msgpack:
            return MsgPackSerializer
        elif self == SerializerType.default:
            return DillSerializer
        else:
            raise ValueError(f'Invalid serializer type: {self}')


