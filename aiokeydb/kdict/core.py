# Forked from [redis-pydict](https://github.com/Narasimha1997/redis-pydict)

import anyio
import json
import aiokeydb
import functools
from typing import Optional, Coroutine, Any, Dict
from anyio._core._eventloop import threadlocals

try:
    import dill as pickle
except ImportError:
    import pickle

NONE_TYPE_MAGIC = b'KDictNoneType'

TYPE_MAGIC = {
    ord('n'): "NoneType",
    ord('i'): "int",
    ord('f'): "float",
    ord('s'): "str",
    ord('d'): "dict",
    ord('l'): "list",
    ord('c'): "custom",
}

TYPE_ENCODE_FUNCTIONS = {
    None: lambda _: b'n',
    int: lambda i: b'i' + str(i).encode('utf-8'),
    float: lambda f: b'f' + str(f).encode('utf-8'),
    str: lambda s: b's' + str(s).encode("utf-8"),
    dict: lambda d: b'd' + json.dumps(d).encode('utf-8'),
    list: lambda l: b'l' + json.dumps(l).encode('utf-8'),
    "custom": lambda c: b'c' + pickle.dumps(c)
}

TYPE_DECODE_FUNCTIONS = {
    ord('n'): lambda _: None,
    ord('i'): lambda i: int(i.decode('utf-8')),
    ord('f'): lambda f: float(f.decode('utf-8')),
    ord('s'): lambda s: s.decode('utf-8'),
    ord('d'): lambda d: json.loads(d),
    ord('l'): lambda l: json.loads(l),
    ord('c'): lambda c: pickle.loads(c)
}


class DataFunctions:

    @staticmethod
    def encode(data):
        type_ = type(data)
        if type_ not in TYPE_ENCODE_FUNCTIONS:
            return TYPE_ENCODE_FUNCTIONS["custom"](data)
        return TYPE_ENCODE_FUNCTIONS[type_](data)

    @staticmethod
    def decode(data):
        type_magic = data[0]

        rest_of_data = data[1:]
        return TYPE_DECODE_FUNCTIONS[type_magic](rest_of_data)

    @staticmethod
    def define_key(namespace, key):
        return namespace + "&&" + key

    @staticmethod
    def unpack_key(key):
        key_string = key.decode('utf-8')
        return key_string.split("&&")[1]


class KDict:

    def __init__(
        self,
        host: str = 'localhost',
        port: int = 6379,
        db: int = 0,
        username: str = None,
        password: str = None,
        namespace: str = "",
        uri: str = None,
        keydb: Optional[aiokeydb.KeyDB] = None,
        **kwargs
        ):
        self.keydb = keydb or (
            aiokeydb.from_url(uri, **kwargs) if uri else aiokeydb.KeyDB(
                host = host, port = port, db = db, username = username, password = password, **kwargs
            )
        )
        #self.redis = custom if custom else redis.Redis(
        #    host=host, port=port, db=db, password=password)
        self.key_namespace = namespace
        self.enable_publish = False
        self.notification_id = ""

    def set_notification_mode(self, enable):
        self.enable_publish = enable

    def set_notification_id(self, id):
        self.notification_id = id
    
    @classmethod
    def get_async_module(cls):
        return getattr(threadlocals, "current_async_module", None)

    def _run_sync(self, func: Coroutine, *args, **kwargs) -> Any:
        """
        Runs the Async Function as Sync
        """
        current_async_module = self.get_async_module()
        partial_f = functools.partial(func, *args, **kwargs)
        if current_async_module is None:
            return anyio.run(partial_f)
        return anyio.from_thread.run(partial_f)

    def _scan_iter(self, pattern):
        formatted_pattern = DataFunctions.define_key(self.key_namespace, pattern)
        return self._run_sync(self.keydb.scan_iter, formatted_pattern)
        #return self.redis.scan_iter(formatted_pattern)

    # basic GET and SET functions

    def get(self, key, default):
        key = DataFunctions.define_key(self.key_namespace, key)
        data = self._run_sync(self.keydb.get, key)
        #data = self.redis.get(key)
        if not data:
            return default
        return DataFunctions.decode(data)

    def __setitem__(self, key, value):
        key = DataFunctions.define_key(self.key_namespace, key)
        data = DataFunctions.encode(value)
        self._run_sync(self.keydb.set, key, data)
        #self.redis.set(key, data)

        # push notification if enabled
        if self.enable_publish:
            self._run_sync(self.keydb.publish, f"event_{key}", self.notification_id)
            #self.redis.publish("event_" + key, self.notification_id)

    def __getitem__(self, key):
        key_formatted = DataFunctions.define_key(self.key_namespace, key)
        #data = self.redis.get(key_formatted)
        data = self._run_sync(self.keydb.get, key_formatted)

        if not data:
            raise KeyError(key)
        return DataFunctions.decode(data)

    def __delitem__(self, key):
        key_formatted = DataFunctions.define_key(self.key_namespace, key)
        return_value = self._run_sync(self.keydb.delete, key_formatted)
        #return_value = self.redis.delete(key_formatted)
        if return_value == 0:
            raise KeyError(key)

    def __contains__(self, key):
        key_formatted = DataFunctions.define_key(self.key_namespace, key)
        return self._run_sync(self.keydb.exists, key_formatted)

    # iteration functions

    def _iter_internal(self, pattern):
        scan_iter = self._scan_iter(pattern)
        return (DataFunctions.unpack_key(key) for key in scan_iter)

    def _iter_items(self, pattern):
        return [(key, self[key]) for key in self._iter_internal(pattern)]

    def __iter__(self):
        return self._iter_internal('*')

    def items(self):
        return self._iter_items('*')

    def iter_matching(self, pattern='*'):
        return self._iter_internal(pattern)

    def items_matching(self, pattern='*'):
        return self._iter_items(pattern)

    # mulit-data operations
    def keys(self):
        return self._iter_items('*')

    def clear(self):
        keys = list(self._scan_iter('*'))
        if len(keys) != 0:
            self._run_sync(self.keydb.delete, *keys)
            #self.redis.delete(*keys)

    def values(self):
        keys = list(self._scan_iter('*'))
        return [
            DataFunctions.decode(value) for value in self._run_sync(self.keydb.mget, *keys)
        ]

    def update(self, from_dic: Dict[Any, Any]):
        pipeline = self.keydb.pipeline()
        for key, value in from_dic.items():
            formatted_key, encoded_value = DataFunctions.define_key(
                self.key_namespace, key), DataFunctions.encode(value)
            self._run_sync(pipeline.set, formatted_key, encoded_value)
            #pipeline.set(formatted_key, encoded_value)
        self._run_sync(pipeline.execute)

    def to_dict(self):
        return {key: value for key, value in self.items()}

    def __len__(self):
        return len(list(self._scan_iter('*')))

    def __str__(self) -> str:
        return str(self.to_dict())

    def subscribe_to_key_events(self, pattern="*"):
        channel_name = DataFunctions.define_key(f"event_{self.key_namespace}", pattern)
        pubsub = self.keydb.pubsub()
        #self._run_sync(pusub.subscribe, channel_name)
        #pusub = self.redis.pubsub()
        #pusub.subscribe(channel_name)
        async def yield_from_pubsub():
            await pubsub.subscribe(channel_name)
            async for message in pubsub.listen():
                if message and type(message) == dict and message['type'] == 'message':
                    notification_id = message['data'].decode('utf-8')
                    channel_complete_name = message['channel'].decode('utf-8')
                    if not channel_complete_name.startswith('event_'): pass

                    channel = channel_complete_name[6:]
                    value = self[channel]
                    yield (channel, notification_id, value)
        
        self._run_sync(yield_from_pubsub)
    