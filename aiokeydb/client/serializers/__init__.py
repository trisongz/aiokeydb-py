from __future__ import absolute_import

from enum import Enum
from typing import Type
from aiokeydb.client.serializers.base import BaseSerializer
from aiokeydb.client.serializers._json import JsonSerializer, OrJsonSerializer
from aiokeydb.client.serializers._pickle import PickleSerializer, DillSerializer
from aiokeydb.client.serializers._msgpack import MsgPackSerializer

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

    def get_serializer(self) -> Type[BaseSerializer]:
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
        elif self == SerializerType.msgpack:
            return MsgPackSerializer
        elif self == SerializerType.default:
            return DillSerializer
        else:
            raise ValueError(f'Invalid serializer type: {self}')


