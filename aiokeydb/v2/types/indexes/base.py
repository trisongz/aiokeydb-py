from __future__ import annotations

import contextlib
from abc import ABC

from typing import (
    Optional, 
    Any, 
    List, 
    Tuple, 
    Type,  
    Dict, 
    Callable, 
    TypeVar, 
    Generator, 
    AsyncGenerator, 
    Coroutine, 
    Set, 
    Union, 
    Iterable, 
    Iterator, 
    AsyncIterator,
    TYPE_CHECKING
)

AmountT = Union[float, int]

_base_keys = [
    'primary_key',
    'kdb',
    'kdb_type',
    'expiration',
    'is_root',
    'is_numeric',
    'is_lookup_index',
    'auto_expire',
    'serializer',
    'serialization_enabled',
    'name_prefix_enabled',
    'name_match_key',
    'name_count_key',
    'name_prefix',
    'settings',
    'name',
    'srl',
    'enc',
    'idx',
    'log',
    '_keys',
]



if TYPE_CHECKING:
    from aiokeydb.v2 import KeyDBSession
    from aiokeydb.v2.types.serializer import BaseSerializer


DT = TypeVar('DT')
AmountT = Union[float, int]
Numeric = Union[int, float]

_logger = None

def get_logger():
    global _logger
    if _logger is None:
        from aiokeydb.v2.utils.logs import logger
        _logger = logger
    return _logger

class BaseKDBIndex(ABC):
    """
    Base KDB Index Class
    """
    # Index Name
    name: Optional[str] = None
    name_prefix: Optional[str] = 'index'

    kdb: Optional[KeyDBSession] = None
    kdb_type: Optional[str] = 'idx'
    expiration: Optional[int] = 60 * 60 * 2 # 2 hours - add a default expiration for the kdb

    name_prefix_enabled: Optional[bool] = True

    def __init__(
        self, 
        name: Optional[str] = None,
        name_prefix: Optional[str] = None,
        kdb: Optional[KeyDBSession] = None, 
        kdb_type: Optional[str] = None,
        expiration: Optional[int] = None, 
        name_prefix_enabled: Optional[bool] = None,
        serializer: Optional[Type['BaseSerializer']] = None,
        serialization_enabled: Optional[bool] = True, 
        **kwargs
    ):
        """
        Initialize the KDB index
        """
        if kdb is not None: self.kdb = kdb
        if kdb_type is not None: self.kdb_type = kdb_type
        if expiration is not None:
            if expiration > 0: self.expiration = expiration
            elif expiration < 0: self.expiration = None

        if name_prefix_enabled is not None: self.name_prefix_enabled = name_prefix_enabled
        if name is not None: self.name = name
        if name_prefix is not None: self.name_prefix = name_prefix
        if self.kdb is None: self.kdb = self.get_session(name = f'{self.name_key}:kdb')
        self.log = get_logger()
        self.srl = None
        self.serialization_enabled = serialization_enabled
        # if self.serialization_enabled:
        if serializer is None: serializer = self.kdb.serializer
        self.srl = serializer
        # self.srl = self.kdb.serializer if serialization_enabled else None
        self.enc = self.kdb.encoder
        
        self._idx = 0
        self._keys: List[str] = []
        self.postinit(**kwargs)

    @classmethod
    def get_session(cls, name: Optional[str] = None, **kwargs) -> 'KeyDBSession':
        """
        Returns the session for the given name
        """
        from aiokeydb.v2 import KeyDBClient
        return KeyDBClient.get_session(name = name, **kwargs)


    def postinit(self, **kwargs):
        """
        Post init hook
        """
        pass

    @property
    def index_name(self) -> str:
        """
        Returns the index name

        ex:
          - app.index -> app_index
        """
        return self.name.replace('.', '_')

    @property
    def name_key(self) -> str:
        """
        Returns the key for the index
        """
        return f'{self.name_prefix}.{self.kdb_type}.{self.name}'
    
    @property
    def name_match_key(self) -> str:
        """
        Returns the match key for the index - this is used to match all keys for the index
        """
        return f'{self.name_prefix}.{self.kdb_type}.{self.name}:*' if self.name_prefix_enabled else '*'
    
    @property
    def name_count_key(self) -> str:
        """
        Returns the count key
        """
        return f'{self.name_prefix}.{self.kdb_type}.{self.name}:count'
    
    def get_key(self, key: str) -> str:
        """
        Returns the key for the given key
        """
        return f'{self.name_key}:{key}' if self.name_prefix_enabled and self.name_key not in key else key

    def is_primitive(self, value: Any, include_bytes: Optional[bool] = True) -> bool:
        """
        Returns whether the given value is a primitive
        """
        if include_bytes and isinstance(value, bytes): return True
        return isinstance(value, (str, int, float))

    def encode(self, value: Any) -> bytes:
        """
        Encodes the value
        """
        if self.is_primitive(value): return value
        with contextlib.suppress(Exception):
            return self.srl.dumps(value) if self.serialization_enabled else self.enc.encode(value)
        with contextlib.suppress(Exception):
            return self.enc.encode(value)
        
        self.log.warning(f"Failed to Encode value: ({type(value)}) {value}")
        return value
        
        # try:
        #     return self.srl.dumps(value) if self.serialization_enabled else self.enc.encode(value)
        # except Exception as e:
        #     self.log.warning(f"Failed to Encode value: ({type(value)}) {value}: {e}")
        #     raise e
        # val = self.srl.dumps(value) if self.serialization_enabled else self.enc.encode(value)
        # self.log.warning(f"Encoded value: ({type(value)}) {value} -> ({type(val)}) {val}")
        # return val
    
    def decode(self, value: bytes) -> Any:
        """
        Decodes the value
        """
        if self.is_primitive(value, False): return value
        with contextlib.suppress(Exception):
            return self.srl.loads(value) if self.serialization_enabled else self.enc.decode(value)
        with contextlib.suppress(Exception):
            return self.enc.decode(value)

        # try:
        #     return self.srl.loads(value) if self.serialization_enabled else self.enc.decode(value)
        # except Exception as e:
        self.log.warning(f"Failed to Decode value: ({type(value)}) {value}")
        return value

        # return val
        # return self.srl.loads(value) if self.serialization_enabled else self.enc.decode(value)


    def rm(self, key: str) -> None:
        """
        Removes the key
        """
        self.kdb.delete(self.get_key(key))

    async def arm(self, key: str) -> None:
        """
        Removes the key
        """
        await self.kdb.async_delete(self.get_key(key))

    def set(self, key: str, value: Any, expiration: Optional[int] = None, **kwargs) -> None:
        """
        Sets the key
        """
        expiration = kwargs.pop('ex', expiration)
        self.kdb.set(self.get_key(key), value, ex = expiration or self.expiration, **kwargs)

    async def aset(self, key: str, value: Any, expiration: Optional[int] = None, **kwargs) -> None:
        """
        Sets the key
        """
        expiration = kwargs.pop('ex', expiration)
        await self.kdb.async_set(self.get_key(key), value, ex = expiration or self.expiration, **kwargs)
    
    def get(self, key: str, **kwargs) -> Any:
        """
        Returns the value for the given key
        """
        return self.kdb.get(self.get_key(key), **kwargs)
    
    async def aget(self, key: str, **kwargs) -> Any:
        """
        Returns the value for the given key
        """
        return await self.kdb.async_get(self.get_key(key), **kwargs)
    
    def iter(self, match: Optional[str] = None, **kwargs) -> Iterator[Tuple[str, Any]]:
        """
        Returns an iterator for the keys
        """
        match = match or self.name_match_key
        for key in self.kdb.scan_iter(match = match, **kwargs):
            yield key, self.kdb.get(key)

    async def aiter(self, match: Optional[str] = None, **kwargs) -> AsyncIterator[Tuple[str, Any]]:
        """
        Returns an iterator for the keys
        """
        match = match or self.name_match_key
        async for key in self.kdb.async_scan_iter(match = match, **kwargs):
            yield key, await self.kdb.async_get(key)

    def iterkeys(self, match: Optional[str] = None, **kwargs) -> List[str]:
        """
        Returns the keys for the given match
        """
        match = match or self.name_match_key
        for key in self.kdb.scan_iter(match = match, **kwargs):
            yield key.decode()
        # yield from self.kdb.scan_iter(match = match, **kwargs)

    async def aiterkeys(self, match: Optional[str] = None, **kwargs) -> List[str]:
        """
        Returns the keys for the given match
        """
        match = match or self.name_match_key
        async for key in self.kdb.async_scan_iter(match = match, **kwargs):
            yield key.decode()

    def itervalues(self, match: Optional[str] = None, **kwargs) -> List[Any]:
        """
        Returns the values for the given match
        """
        match = match or self.name_match_key
        for key in self.kdb.scan_iter(match = match, **kwargs):
            yield self.kdb.get(key)

    async def aitervalues(self, match: Optional[str] = None, **kwargs) -> List[Any]:
        """
        Returns the values for the given match
        """
        match = match or self.name_match_key
        async for key in self.kdb.async_scan_iter(match = match, **kwargs):
            yield await self.kdb.async_get(key)

    def values(self, match: Optional[str] = None, **kwargs) -> List[Any]:
        """
        Returns the values for the given match
        """
        return list(self.itervalues(match = match, **kwargs))
    
    async def avalues(self, match: Optional[str] = None, **kwargs) -> List[Any]:
        """
        Returns the values for the given match
        """
        return list(await self.aitervalues(match = match, **kwargs))

    def keys(self, match: Optional[str] = None, **kwargs) -> List[str]:
        """
        Returns the keys for the given match
        """
        return list(self.iterkeys(match = match, **kwargs))

    async def akeys(self, match: Optional[str] = None, **kwargs) -> List[str]:
        """
        Returns the keys for the given match
        """
        return list(await self.aiterkeys(match = match, **kwargs))

    def items(self, match: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """
        Returns the items for the given match
        """
        return dict(self.iter(match = match, **kwargs))
    
    async def aitems(self, match: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """
        Returns the items for the given match
        """
        return dict(await self.aiter(match = match, **kwargs))
    
    def clear(self, match: Optional[str] = None, **kwargs):
        """
        Clears the keys
        """
        match = match or self.name_match_key
        for key in self.kdb.scan_iter(match = match, **kwargs):
            self.kdb.delete(key)

    async def aclear(self, match: Optional[str] = None, **kwargs):
        """
        Clears the keys
        """
        match = match or self.name_match_key
        async for key in self.kdb.async_scan_iter(match = match, **kwargs):
            await self.kdb.async_delete(key)
    
    def expire(self, match: Optional[str] = None, expiration: Optional[int] = 1, **kwargs):
        """
        Expires the keys
        """
        match = match or self.name_match_key
        for key in self.kdb.scan_iter(match = match, **kwargs):
            self.kdb.expire(key, time = expiration)

    async def aexpire(self, match: Optional[str] = None, expiration: Optional[int] = 1, **kwargs):
        """
        Expires the keys
        """
        match = match or self.name_match_key
        async for key in self.kdb.async_scan_iter(match = match, **kwargs):
            await self.kdb.async_expire(key, time = expiration)

    def incr(self, amount: AmountT = 1, key: Optional[str] = None) -> AmountT:
        """
        Increments the given key
        """
        key = self.get_key(key) if key else self.name_count_key
        func = self.kdb.incrbyfloat if isinstance(amount, float) else self.kdb.incrby
        return func(key, amount = amount)
    
    async def aincr(self, amount: AmountT = 1, key: Optional[str] = None) -> AmountT:
        """
        Increments the given key
        """
        key = self.get_key(key) if key else self.name_count_key
        func = self.kdb.async_incrbyfloat if isinstance(amount, float) else self.kdb.async_incrby
        # try:
        return await func(key, amount = amount)
        # except Exception as e:
        #     value = await self.kdb.async_get(key)
        #     self.log.trace(f"Failed to increment: {key} ({value})", e)
        #     raise e
    
    def decr(self, amount: AmountT = 1, key: Optional[str] = None) -> AmountT:
        """
        Decrements the given key
        """
        key = self.get_key(key) if key else self.name_count_key
        return self.kdb.decr(key, amount = amount)
    
    async def adecr(self, amount: AmountT = 1, key: Optional[str] = None) -> AmountT:
        """
        Decrements the given key
        """
        key = self.get_key(key) if key else self.name_count_key
        return await self.kdb.async_decr(key, amount = amount)
    

    def hset(self, key: str, field: str, value: Any) -> int:
        """
        Sets the value for the given key
        """
        return self.kdb.hset(self.get_key(key), field, self.encode(value))
    
    async def ahset(self, key: str, field: str, value: Any) -> int:
        """
        Sets the value for the given key
        """
        return await self.kdb.async_hset(self.get_key(key), field, self.encode(value))
    
    def hget(self, key: str, field: str, default: Optional[Any] = None) -> Any:
        """
        Returns the value for the given key
        """
        value = self.kdb.hget(self.get_key(key), field)
        return self.decode(value) if value is not None else default
    
    async def ahget(self, key: str, field: str, default: Optional[Any] = None) -> Any:
        """
        Returns the value for the given key
        """
        value = await self.kdb.async_hget(self.get_key(key), field)
        return self.decode(value) if value is not None else default

    
    def hdel(self, key: str, field: str) -> int:
        """
        Deletes the given key
        """
        return self.kdb.hdel(self.get_key(key), field)
    
    async def ahdel(self, key: str, field: str) -> int:
        """
        Deletes the given key
        """
        return await self.kdb.async_hdel(self.get_key(key), field)  
    
    def hincr(self, key: str, field: str, amount: AmountT = 1) -> AmountT:
        """
        Increments the given key
        """
        func = self.kdb.hincrby if isinstance(amount, int) else self.kdb.hincrbyfloat
        return func(self.get_key(key), field, amount = amount)
    
    async def ahincr(self, key: str, field: str, amount: AmountT = 1) -> AmountT:
        """
        Increments the given key
        """
        func = self.kdb.async_hincrby if isinstance(amount, int) else self.kdb.async_hincrbyfloat
        return await func(self.get_key(key), field, amount = amount)
    
    
    def hdecr(self, key: str, field: str, amount: AmountT = 1) -> AmountT:
        """
        Decrements the given key
        """
        return self.hincr(key = key, field = field, amount = -amount)
    
    async def ahdecr(self, key: str, field: str, amount: AmountT = 1) -> AmountT:
        """
        Decrements the given key
        """
        return await self.ahincr(key = key, field = field, amount = -amount)
    
    
    def hkeys(self, key: str) -> List[str]:
        """
        Returns the keys for the given key
        """
        keys = self.kdb.hkeys(self.get_key(key))
        return [self.decode(key) for key in keys] if keys else []
        # return self.kdb.hkeys(self.get_key(key))
    
    async def ahkeys(self, key: str) -> List[str]:
        """
        Returns the keys for the given key
        """
        keys = await self.kdb.async_hkeys(self.get_key(key))
        return [self.decode(key) for key in keys] if keys else []
        # return await self.kdb.async_hkeys(self.get_key(key))
    
    def hvals(self, key: str) -> List[Any]:
        """
        Returns the values for the given key
        """
        values = self.kdb.hvals(self.get_key(key))
        return [self.decode(value) for value in values] if values else []
        # return [self.decode(value) for value in self.kdb.hvals(self.get_key(key))]
    
    async def ahvals(self, key: str) -> List[Any]:
        """
        Returns the values for the given key
        """
        values = await self.kdb.async_hvals(self.get_key(key))
        return [self.decode(value) for value in values] if values else []
        # return [self.decode(value) for value in await self.kdb.async_hvals(self.get_key(key))]
    
    def hgetall(self, key: str) -> Dict[str, Any]:
        """
        Returns the values for the given key
        """
        items = self.kdb.hgetall(self.get_key(key))
        return {field: self.decode(value) for field, value in items.items()} if items else {}
        # return {field: self.decode(value) for field, value in self.kdb.hgetall(self.get_key(key)).items()}
    
    async def ahgetall(self, key: str) -> Dict[str, Any]:
        """
        Returns the values for the given key
        """
        items = await self.kdb.async_hgetall(self.get_key(key))
        return {field: self.decode(value) for field, value in items.items()} if items else {}
    
    def hlen(self, key: str) -> int:
        """
        Returns the length of the given key
        """
        return self.kdb.hlen(self.get_key(key))
    
    async def ahlen(self, key: str) -> int:
        """
        Returns the length of the given key
        """
        return await self.kdb.async_hlen(self.get_key(key))
    
    def hexists(self, key: str, field: str) -> bool:
        """
        Returns the length of the given key
        """
        return self.kdb.hexists(self.get_key(key), field)
    
    async def ahexists(self, key: str, field: str) -> bool:
        """
        Returns the length of the given key
        """
        return await self.kdb.async_hexists(self.get_key(key), field)
    
    def hsetnx(self, key: str, field: str, value: Any) -> int:
        """
        Sets the value for the given key
        """
        return self.kdb.hsetnx(self.get_key(key), field, self.encode(value))
    
    async def ahsetnx(self, key: str, field: str, value: Any) -> int:
        """
        Sets the value for the given key
        """
        return await self.kdb.async_hsetnx(self.get_key(key), field, self.encode(value))
    
    def hmset(self, key: str, mapping: Dict[str, Any]) -> int:
        """
        Sets the value for the given key
        """
        return self.kdb.hmset(self.get_key(key), {field: self.encode(value) for field, value in mapping.items()})
    
    async def ahmset(self, key: str, mapping: Dict[str, Any]) -> int:
        """
        Sets the value for the given key
        """
        return await self.kdb.async_hmset(self.get_key(key), {field: self.encode(value) for field, value in mapping.items()})
    
    def hmget(self, key: str, fields: List[str]) -> List[Any]:
        """
        Returns the value for the given key
        """
        values = self.kdb.hmget(self.get_key(key), fields)
        return [self.decode(value) for value in values] if values else []

    
    async def ahmget(self, key: str, fields: List[str]) -> List[Any]:
        """
        Returns the value for the given key
        """
        values = await self.kdb.async_hmget(self.get_key(key), fields)
        return [self.decode(value) for value in values] if values else []
    
    def clear(self, match: Optional[str] = None, key: Optional[str] = None, **kwargs) -> None:
        """
        Clears the key
        """
        if key is None:
            for key in self.keys(match = match, **kwargs):
                self.kdb.delete(key)
            self._keys = []
        else:
            self.kdb.delete(self.get_key(key))
            if key in self._keys: self._keys.remove(key)

    async def aclear(self, match: Optional[str] = None, key: Optional[str] = None, **kwargs) -> None:
        """
        Clears the key
        """
        if key is None:
            for key in await self.akeys(match = match, **kwargs):
                await self.kdb.async_delete(key)
            self._keys = []
        else:
            await self.kdb.async_delete(self.get_key(key))
            if key in self._keys: self._keys.remove(key)
        

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Set the attribute
        """
        if name in _base_keys:
            super().__setattr__(name, value)
        else:
            return self.kdb.set(self.get_key(name), value, ex = self.expiration)
    
    def __getattr__(self, name: str) -> Any:
        """
        Get the attribute
        """
        if name in _base_keys:
            return super().__getattr__(name)
        return self.kdb.get(self.get_key(name))

    def __getitem__(self, key: str) -> Any:
        """
        Returns the value for the given key
        """
        return self.kdb.get(self.get_key(key))
    
    def __setitem__(self, key: str, value: Any) -> None:
        """
        Sets the value for the given key
        """
        return self.kdb.set(self.get_key(key), value, ex = self.expiration)
    
    def __delitem__(self, key: str) -> None:
        """
        Deletes the given key
        """
        return self.kdb.delete(self.get_key(key))
    
    def __contains__(self, key: str) -> bool:
        """
        Returns whether the given key exists
        """
        return self.kdb.exists(self.get_key(key))

    def __iter__(self):
        """
        Returns an iterator for the keys
        """
        self._idx = 0
        self._keys = self.keys()
        return self
    
    def __next__(self) -> Tuple[str, Any]:
        """
        Returns the next key
        """
        if self._idx >= len(self._keys):
            raise StopIteration
        key = self._keys[self._idx]
        self._idx += 1
        return key, self.kdb.get(key)
    
    def __aiter__(self):
        """
        Returns an async iterator for the keys
        """
        self._idx = 0
        self._keys = self.keys()
        return self
    
    async def __anext__(self) -> Tuple[str, Any]:
        """
        Returns the next key
        """
        if self._idx >= len(self._keys):
            raise StopAsyncIteration
        key = self._keys[self._idx]
        self._idx += 1
        return key, await self.kdb.async_get(key)
    
    def __len__(self) -> int:
        """
        Returns the number of keys
        """
        return len(self._keys or self.keys())
    
    def __repr__(self) -> str:
        """
        Returns the representation of the object
        """
        return f'<{self.__class__.__name__} name={self.name}, index_name={self.index_name}, name_key={self.name_key}, match_key={self.name_match_key}, builder_prefix_enabled={self.name_prefix_enabled}, keys={len(self)}, uri={self.kdb.uri}>'
    
    def close(self) -> None:
        """
        Closes the connection
        """
        self.kdb.close()

    async def aclose(self) -> None:
        """
        Closes the connection
        """
        await self.kdb.aclose()

    def __enter__(self) -> 'KDBIndexT':
        """
        Returns the object
        """
        return self
    
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """
        Closes the connection
        """
        self.close()

    async def __aenter__(self) -> 'KDBIndexT':
        """
        Returns the object
        """
        return self
    
    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """
        Closes the connection
        """
        await self.aclose()

    # Pydantic methods
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v: Union['KDBIndexT', Dict[str, Any], str, Iterable[Any], Any]) -> KDBIndexT:
        """
        Validates the given value
        """
        if isinstance(v, cls):
            return v
        elif isinstance(v, dict):
            return cls(**v)
        elif isinstance(v, str):
            return cls(builder = v)
        elif isinstance(v, Iterable):
            return cls(*v)
        return cls(v)
    
    @classmethod
    def create_default_factory(cls, builder: Optional[str] = None, **kwargs) -> Callable[..., KDBIndexT]:
        """
        Returns a default factory for the given class
        """
        def _default_factory() -> KDBIndexT:
            return cls(builder = builder, **kwargs)
        return _default_factory

KDBIndexT = TypeVar('KDBIndexT', bound = 'BaseKDBIndex')
KDBIndexType = Type[BaseKDBIndex]
Numeric = Union[int, float]

class KDBIndex(BaseKDBIndex):
    """
    KDB Index Class
    """
    pass

