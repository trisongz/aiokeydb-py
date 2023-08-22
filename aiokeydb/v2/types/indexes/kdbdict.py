from __future__ import annotations

from .base import *


class KDBDict(BaseKDBIndex):
    """
    Dict Class based on KeyDB

    - uses hset primarily
    """
    name_prefix: Optional[str] = 'datadict'

    kdb_type: Optional[str] = 'dict'
    primary_key: Optional[str] = 'index'
    is_numeric: Optional[bool] = None
    is_lookup_index: Optional[bool] = None
    
    auto_expire: Optional[bool] = True
    
    def postinit(
        self, 
        primary_key: Optional[str] = None, 
        is_numeric: Optional[bool] = None, 
        is_lookup_index: Optional[bool] = None, 
        auto_expire: Optional[bool] = None, 
        **kwargs
    ):
        """
        Post init hook
        """
        if primary_key is not None: 
            self.primary_key = primary_key
        if is_numeric is not None:
            self.is_numeric = is_numeric
        if is_lookup_index is not None:
            self.is_lookup_index = is_lookup_index
        if auto_expire is not None:
            self.auto_expire = auto_expire
        # if is_root is not None:
        #     self.is_root = is_root
            # print('setting postinit', primary_key, self.primary_key)

    @property
    def child_key(self) -> Optional[str]:
        """
        Returns the child key for the index
        """
        return self.primary_key.split('.', 1)[-1] \
            if '.' in self.primary_key else None

    @property
    def parent_key(self) -> str:
        """
        Returns the parent key for the index
        """
        return self.primary_key.split('.', 1)[0] \
            if '.' in self.primary_key else self.primary_key
    
    @property
    def is_root(self) -> bool:
        """
        Returns whether the index is a root index
        """
        return self.parent_key == self.primary_key
    

    @property
    def name_count_key(self) -> str:
        """
        Returns the count key
        """
        return f'{self.name_prefix}.{self.kdb_type}.{self.name}:{self.primary_key}:count'

    @property
    def name_lookup_key(self) -> str:
        """
        Returns the lookup key
        """
        return f'{self.name_prefix}.{self.kdb_type}.{self.name}:{self.primary_key}:lookup'

    @property
    def name_key(self) -> str:
        """
        Returns the key for the index
        """
        return f'{self.name_prefix}.{self.kdb_type}.{self.name}:{self.primary_key}'
    
    @property
    def name_match_key(self) -> str:
        """
        Returns the match key for the index - this is used to match all keys
        """
        # if self.is_root:
        return f'{self.name_prefix}.{self.kdb_type}.{self.name}:{self.primary_key}:*' \
            if self.name_prefix_enabled else '*'


    def get_key(self, key: Optional[str] = None, lookup: Optional[bool] = None) -> str:
        """
        Returns the key for the given key
        """
        if lookup: return self.get_lookup_key(key)
        if key is None:
            return self.name_key
        key = str(key)
        return f'{self.name_key}:{key}' if self.name_prefix_enabled and self.name_key not in key else key

    def get_lookup_key(self, key: Optional[str] = None) -> str:
        """
        Returns the lookup key for the given key
        """
        if key is None:
            return self.name_lookup_key
        key = str(key)
        return f'{self.name_lookup_key}:{key}' if self.name_prefix_enabled and self.name_lookup_key not in key else key

    def get_match_key(self, key: Optional[str] = None) -> str:
        """
        Returns the match key for the given key
        """
        if key is None: return self.name_match_key
        key = str(key)
        if '*' not in key: key = f'{key}*'
        return f'{self.name_key}:{key}' if self.name_prefix_enabled and self.name_key not in key else key


    def hset(self, field: str, value: Any, key: Optional[str] = None, ex: Optional[int] = None) -> int:
        """
        Sets the value for the given key
        """
        _key = self.get_key(key)
        r = self.kdb.hset(_key, field, self.encode(value))
        if self.auto_expire or ex is not None:
            self.kdb.expire(_key, ex or self.expiration)
        return r
    
    async def ahset(self, field: str, value: Any, key: Optional[str] = None, ex: Optional[int] = None) -> int:
        """
        Sets the value for the given key
        """
        _key = self.get_key(key)
        r = await self.kdb.async_hset(_key, field, self.encode(value))
        if self.auto_expire or ex is not None:
            await self.kdb.async_expire(_key, ex or self.expiration)
        return r
        # return await self.kdb.async_hset(self.get_key(key), field, self.encode(value))
    
    def hget(self, field: str, default: Optional[DT] = None, key: Optional[str] = None) -> DT:
        """
        Returns the value for the given key
        """
        value = self.kdb.hget(self.get_key(key), field)
        return self.decode(value) if value is not None else default
    
    async def ahget(self, field: str, default: Optional[DT] = None, key: Optional[str] = None) -> DT:
        """
        Returns the value for the given key
        """
        value = await self.kdb.async_hget(self.get_key(key), field)
        return self.decode(value) if value is not None else default
    
    def hdel(self, field: str, key: Optional[str] = None) -> int:
        """
        Deletes the given key
        """
        return self.kdb.hdel(self.get_key(key), field)
    
    async def ahdel(self, field: str, key: Optional[str] = None) -> int:
        """
        Deletes the given key
        """
        return await self.kdb.async_hdel(self.get_key(key), field)  
    
    def hincr(self, field: str, amount: AmountT = 1, key: Optional[str] = None) -> AmountT:
        """
        Increments the given key
        """
        func = self.kdb.hincrby if isinstance(amount, int) else self.kdb.hincrbyfloat
        return func(self.get_key(key), field, amount = amount)
    
    async def ahincr(self, field: str, amount: AmountT = 1, key: Optional[str] = None) -> AmountT:
        """
        Increments the given key
        """
        func = self.kdb.async_hincrby if isinstance(amount, int) else self.kdb.async_hincrbyfloat
        return await func(self.get_key(key), field, amount = amount)
    
    def hkeys(self, key: Optional[str] = None) -> List[str]:
        """
        Returns the keys for the given key
        """
        keys = self.kdb.hkeys(self.get_key(key))
        return [] if keys is None else [k.decode() for k in keys]
    
    async def ahkeys(self, key: Optional[str] = None) -> List[str]:
        """
        Returns the keys for the given key
        """
        keys = await self.kdb.async_hkeys(self.get_key(key))
        return [] if keys is None else [k.decode() for k in keys]
    
    def hvals(self, key: Optional[str] = None) -> List[Any]:
        """
        Returns the values for the given key
        """
        values = self.kdb.hvals(self.get_key(key))
        return [] if values is None else [self.decode(value) for value in values]
    
    async def ahvals(self, key: Optional[str] = None) -> List[Any]:
        """
        Returns the values for the given key
        """
        values = await self.kdb.async_hvals(self.get_key(key))
        return [] if values is None else [self.decode(value) for value in values]
    
    def hgetall(self, key: Optional[str] = None) -> Dict[str, Any]:
        """
        Returns the values for the given key
        """
        items = self.kdb.hgetall(self.get_key(key))
        return {} if items is None else {field.decode(): self.decode(value) for field, value in items.items()}
    
    async def ahgetall(self, key: Optional[str] = None) -> Dict[str, Any]:
        """
        Returns the values for the given key
        """
        items = await self.kdb.async_hgetall(self.get_key(key))
        return {} if items is None else {field.decode(): self.decode(value) for field, value in items.items()}
    
    def hlen(self, key: Optional[str] = None) -> int:
        """
        Returns the length of the given key
        """
        return self.kdb.hlen(self.get_key(key))
    
    async def ahlen(self, key: Optional[str] = None) -> int:
        """
        Returns the length of the given key
        """
        return await self.kdb.async_hlen(self.get_key(key))
    
    def hexists(self, field: str, key: Optional[str] = None) -> bool:
        """
        Returns the length of the given key
        """
        return self.kdb.hexists(self.get_key(key), field)
    
    async def ahexists(self, field: str, key: Optional[str] = None) -> bool:
        """
        Returns the length of the given key
        """
        return await self.kdb.async_hexists(self.get_key(key), field)
    
    def hsetnx(self, field: str, value: Any, key: Optional[str] = None, ex: Optional[int] = None) -> int:
        """
        Sets the value for the given key
        """
        _key = self.get_key(key)
        r = self.kdb.hsetnx(_key, field, self.encode(value))
        if self.auto_expire:
            self.kdb.expire(_key, ex or self.expiration)
        return r
        # return self.kdb.hsetnx(self.get_key(key), field, self.encode(value))
    
    async def ahsetnx(self, field: str, value: Any, key: Optional[str] = None, ex: Optional[int] = None) -> int:
        """
        Sets the value for the given key
        """
        _key = self.get_key(key)
        r = await self.kdb.async_hsetnx(_key, field, self.encode(value))
        if self.auto_expire:
            await self.kdb.async_expire(_key, ex or self.expiration)
        return r
    
    def hmset(self, mapping: Dict[str, Any], key: Optional[str] = None, ex: Optional[int] = None) -> int:
        """
        Sets the value for the given key
        """
        _key = self.get_key(key)
        r = self.kdb.hmset(_key, {field: self.encode(value) for field, value in mapping.items()})
        if self.auto_expire or ex is not None:
            self.kdb.expire(_key, ex or self.expiration)
        return r
    
    async def ahmset(self, mapping: Dict[str, Any], key: Optional[str] = None, ex: Optional[int] = None) -> int:
        """
        Sets the value for the given key
        """
        _key = self.get_key(key)
        r = await self.kdb.async_hmset(_key, {field: self.encode(value) for field, value in mapping.items()})
        if self.auto_expire or ex is not None:
            await self.kdb.async_expire(_key, ex or self.expiration)
        return r
        # return await self.kdb.async_hmset(self.get_key(key), {field: self.encode(value) for field, value in mapping.items()})
    
    def hmget(self, fields: List[str], key: Optional[str] = None) -> List[Any]:
        """
        Returns the value for the given key
        """
        return [self.decode(value) for value in self.kdb.hmget(self.get_key(key), fields)]
    
    async def ahmget(self, fields: List[str], key: Optional[str] = None) -> List[Any]:
        """
        Returns the value for the given key
        """
        return [self.decode(value) for value in await self.kdb.async_hmget(self.get_key(key), fields)]
    
    def hpop(self, field: str, key: Optional[str] = None) -> Any:
        """
        Returns the value for the given key
        """
        key = self.get_key(key)
        value = self.kdb.hget(key, field)
        self.kdb.hdel(key, field)
        return self.decode(value)
    
    async def ahpop(self, field: str, default: Optional[str] = None, key: Optional[str] = None) -> Any:
        """
        Returns the value for the given key
        """
        key = self.get_key(key)
        value = await self.kdb.async_hget(key, field)
        if value is None: return default
        await self.kdb.async_hdel(key, field)
        return self.decode(value)

    """
    Dict methods
    """

    def keys(self) -> List[str]:
        """
        Returns the keys for the given key
        """
        return self.hkeys()
    
    async def akeys(self) -> List[str]:
        """
        Returns the keys for the given key
        """
        return await self.ahkeys()

    def values(self) -> List[Any]:
        """
        Returns the values for the given key
        """
        return self.hvals()
    
    async def avalues(self) -> List[Any]:
        """
        Returns the values for the given key
        """
        return await self.ahvals()
    
    def items(self) -> Dict[str, Any]:
        """
        Returns the values for the given key
        """
        return self.hgetall().items()
    
    async def aitems(self) -> Dict[str, Any]:
        """
        Returns the values for the given key
        """
        return (await self.ahgetall()).items()
    
    def pop(self, field: str, default: Optional[str] = None, key: Optional[str] = None) -> Any:
        """
        Returns the value for the given key
        """
        return self.hpop(field, default, key)

    def apop(self, field: str, default: Optional[str] = None, key: Optional[str] = None) -> Any:
        """
        Returns the value for the given key
        """
        return self.ahpop(field, default, key)

    def nested(self, child_key: Optional[str] = None, key: Optional[str] = None, **kwargs) -> 'KDBDict':
        """
        Creates a new nested instance of the dict

        data['key']['child_key'] = {'kew': 'value'}
        data['key'] = {'child_key': {'kew': 'value'}}
        child_key = data.nested('child_key', 'key')
        """
        key = key or self.primary_key
        new_primary_key = f'{key}.{child_key}' if child_key else key

        # Check expiration
        expiration = kwargs.pop('expiration', None) or self.expiration
        if expiration and self.expiration and expiration != self.expiration:
            # Use the greater expiration
            expiration = max(expiration, self.expiration)
        name_prefix = kwargs.pop('name_prefix', None) or self.name_prefix
        serialization_enabled = kwargs.pop('serialization_enabled', self.serialization_enabled)
        # self.serialization_enabled
        serializer = kwargs.pop('serializer', self.srl)
        #  or self.srl
        return self.__class__(
            kdb = self.kdb, 
            kdb_type = self.kdb_type,
            primary_key = new_primary_key,
            name = self.name,
            name_prefix = name_prefix,
            name_prefix_enabled = self.name_prefix_enabled,
            expiration = expiration,
            serialization_enabled = serialization_enabled,
            serializer = serializer,
            **kwargs
        )


    def copy(self, child_key: Optional[str] = None, as_dict: Optional[bool] = False, key: Optional[str] = None) -> Union['KDBDict', Dict[str, Any]]:
        """
        Copies the dict 
        """
        return self.hgetall(key)

    def get(self, field: str, default: Optional[str] = None, key: Optional[str] = None) -> Any:
        """
        Returns the value for the given key
        """
        return self.hget(field, default, key)
    
    async def aget(self, field: str, default: Optional[str] = None, key: Optional[str] = None) -> Any:
        """
        Returns the value for the given key
        """
        return await self.ahget(field, default, key)
    
    def iter(self) -> Iterator[Tuple[str, Any]]:
        """
        Returns an iterator for the dict
        """
        yield from iter(self.items())
    
    async def aiter(self) -> AsyncIterator[Tuple[str, Any]]:
        """
        Returns an iterator for the dict
        """
        async for item in iter(await self.aitems()):
            yield item

    def clear(self, key: Optional[str] = None, lookup: Optional[bool] = None) -> None:
        """
        Clears the given key
        """
        if key is not None or lookup is not None:
            return self.kdb.delete(self.get_key(key, lookup = lookup))
        # Clear all keys
        self._keys = []
        if all_keys := self.kdb.keys(self.name_match_key):
            return self.kdb.delete(all_keys)
    
    async def aclear(self, key: Optional[str] = None, lookup: Optional[bool] = None) -> None:
        """
        Clears the given key
        """
        if key is not None or lookup is not None:
            return await self.kdb.async_delete(self.get_key(key, lookup = lookup))
        self._keys = []
        if all_keys := await self.kdb.async_keys(self.name_match_key):
            return await self.kdb.async_delete(all_keys)

    def getorset(self, field: str, default: Optional[Any] = None, key: Optional[str] = None, ex: Optional[int] = None) -> Any:
        """
        Returns the value for the given key or sets it to the default value
        """
        if not self.hexists(field, key):
            self.hset(field, default, key, ex = ex)
            return default
        return self.hget(field, default, key)
    
    async def agetorset(self, field: str, default: Optional[Any] = None, key: Optional[str] = None, ex: Optional[int] = None) -> Any:
        """
        Returns the value for the given key or sets it to the default value
        """
        if not await self.ahexists(field, key):
            await self.ahset(field, default, key, ex = ex)
            return default
        return await self.ahget(field, default, key)

    def setcount(self, value: Numeric, ex: Optional[int] = None) -> Numeric:
        """
        Sets the count for the given key
        """
        self.kdb.set(self.name_count_key, value, ex = ex or self.expiration, _serializer = True)
        return value
    
    def getcount(self) -> Numeric:
        """
        Returns the count for the given key
        """
        return int(self.kdb.get(self.name_count_key, 0, _return_raw_value = True, _serializer = True))
    
    async def asetcount(self, value: Numeric, ex: Optional[int] = None) -> Numeric:
        """
        Sets the count for the given key
        """
        await self.kdb.async_set(self.name_count_key, value, ex = ex or self.expiration, _serializer = True)
        return value
    
    async def agetcount(self) -> Numeric:
        """
        Returns the count for the given key
        """
        return int(await self.kdb.async_get(self.name_count_key, 0, _return_raw_value = True, _serializer = True))
    
    @property
    def idx(self) -> int:
        """
        Returns the index for the given key
        """
        try:
            return self.kdb.incr(self.name_count_key)
        except Exception as e:
            # Probably an error with serialization
            _idx = int(self.kdb.get(self.name_count_key, 0, _return_raw_value = True, _serializer = True))
            self.kdb.set(self.name_count_key, _idx + 1, ex = self.expiration, _serializer = True)
            return _idx + 1
    
    @property
    async def aidx(self) -> int:
        """
        Returns the index for the given key
        """
        try:
            return await self.kdb.async_incr(self.name_count_key)
        except Exception as e:
            # Probably an error with serialization
            _idx = int(await self.kdb.async_get(self.name_count_key, 0, _return_raw_value = True, _serializer = True))
            await self.kdb.async_set(self.name_count_key, _idx + 1, ex = self.expiration, _serializer = True)
            return _idx + 1
    
    @property
    def count(self) -> int:
        """
        Returns the count for the given key
        """
        return int(self.kdb.get(self.name_count_key, 0, _return_raw_value = True, _serializer = True))
    
    @property
    async def acount(self) -> int:
        """
        Returns the count for the given key
        """
        return int(await self.kdb.async_get(self.name_count_key, 0, _return_raw_value = True,  _serializer = True))

    def __getitem__(self, key: str) -> Any:
        """
        Returns the value for the given key
        """
        if key == 'count':
            # logger.info('getting count')
            return int(self.kdb.get(self.name_count_key, 0, _return_raw_value = True, _serializer = True))
        return self.hget(key)
    
    def __setitem__(self, key: str, value: Any) -> None:
        """
        Sets the value for the given key
        """
        if key == 'count':
            self.kdb.set(self.name_count_key, value, ex = self.expiration, _serializer = True)
        else: 
            self.hset(key, value)
    
    def __delitem__(self, key: str) -> None:
        """
        Deletes the given key
        """
        if key == 'count':
            return self.kdb.delete(self.name_count_key)
        if self.is_lookup_index:
            self.kdb.hdel(self.get_key(lookup = True), key)
        return self.hdel(key)
    
    def __contains__(self, key: str) -> bool:
        """
        Returns whether the given key is in the database
        """
        if key == 'count':
            return self.kdb.exists(self.name_count_key)
        return self.hexists(key)
    
    
    def __len__(self) -> int:
        """
        Returns the length of the given key
        """
        return self.hlen()
    
    @property
    def length(self) -> int:
        """
        Returns the length of the given key
        """
        return self.hlen()

    @property
    async def alength(self) -> int:
        """
        Returns the length of the given key
        """
        return await self.ahlen()

    def __iter__(self):
        """
        Returns an iterator for the keys
        """
        self._idx = 0
        self._keys = self.keys()
        return self
    
    def __next__(self) -> Any:
        """
        Returns the next key
        """
        if self._idx >= len(self._keys): raise StopIteration
        key = self._keys[self._idx]
        self._idx += 1
        return key, self.hget(key)
    
    def __aiter__(self):
        """
        Returns an async iterator for the keys
        """
        self._idx = 0
        self._keys = self.keys()
        return self
    
    async def __anext__(self):
        """
        Returns the next key
        """
        if self._idx >= len(self._keys): raise StopAsyncIteration
        key = self._keys[self._idx]
        self._idx += 1
        return key, await self.ahget(key)

    def dict(
        self, 
        key: Optional[str] = None,
        exclude: Optional[List[str]] = None,
        include: Optional[List[str]] = None,
        exclude_none: bool = False,
        exclude_unset: bool = False,
        sort_keys: Optional[bool] = None,
        sorted_key: Optional[str] = None,
        sorted_func: Optional[Callable] = None,
        values_as_keys: Optional[bool] = False,
        reverse: Optional[bool] = False,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Returns the dict
        """
        data = self.hgetall(key)
        if not data: return {}
        # is_numeric = all(check_numeric(k) for k in data.keys())
        # if all(check_numeric(k) for k in data.keys()):
        if self.is_numeric:
            data = {int(v): k for k, v in data.items()} if values_as_keys else {int(k): v for k, v in data.items()}
        if exclude is not None:
            data = {key: data[key] for key in exclude if key in data}
        if include is not None:
            data = {key: data[key] for key in include if key in data}
        if exclude_none:
            data = {key: data[key] for key in data if data[key] is not None}
        if exclude_unset:
            data = {key: data[key] for key in data if data[key]}
        if sorted_key is not None:
            if sorted_func:
                data = dict(sorted(data.items(), key = lambda item: sorted_func(item[1][sorted_key]), reverse = reverse))
            else:
                data = dict(sorted(data.items(), key = lambda item: item[1][sorted_key], reverse = reverse))
        elif sort_keys:
            if sorted_func:
                data = dict(sorted(data.items(), key = lambda item: sorted_func(item[0]), reverse = reverse))
            else:
                data = dict(sorted(data.items(), key = lambda item: item[0], reverse = reverse))
        return data

    
    async def adict(
        self, 
        key: Optional[str] = None,
        exclude: Optional[List[str]] = None,
        include: Optional[List[str]] = None,
        exclude_none: bool = False,
        exclude_unset: bool = False,
        sort_keys: Optional[bool] = None,
        sorted_key: Optional[str] = None,
        sorted_func: Optional[Callable] = None,
        values_as_keys: Optional[bool] = False,
        reverse: Optional[bool] = False,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Returns the dict
        """
        data = await self.ahgetall(key)
        if not data: return {}
        # Gets too expensive.
        # if all(check_numeric(k) for k in data.keys()):
        if self.is_numeric:
            data = {int(v): k for k, v in data.items()} if values_as_keys else {int(k): v for k, v in data.items()}
        if exclude is not None:
            data = {key: data[key] for key in exclude if key in data}
        if include is not None:
            data = {key: data[key] for key in include if key in data}
        if exclude_none:
            data = {key: data[key] for key in data if data[key] is not None}
        if exclude_unset:
            data = {key: data[key] for key in data if data[key]}
        if sorted_key is not None:
            if sorted_func:
                data = dict(sorted(data.items(), key = lambda item: sorted_func(item[1][sorted_key]), reverse = reverse))
            else:
                data = dict(sorted(data.items(), key = lambda item: item[1][sorted_key], reverse = reverse))
        elif sort_keys:
            if sorted_func:
                data = dict(sorted(data.items(), key = lambda item: sorted_func(item[0]), reverse = reverse))
            else:
                data = dict(sorted(data.items(), key = lambda item: item[0], reverse = reverse))
        return data

    
    def update(self, other: Union['KDBDict', Dict[str, Any]], key: Optional[str] = None, ex: Optional[int] = None, **kwargs) -> None:
        """
        Updates the dict
        """
        if isinstance(other, KDBDict) and hasattr(other, 'dict'):
            self.hmset(other.dict(), key, ex = ex)
        elif isinstance(other, dict):
            self.hmset(other, key, ex = ex)
        if kwargs:
            self.hmset(kwargs, key, ex = ex)
    
    async def aupdate(self, other: Union['KDBDict', Dict[str, Any]], key: Optional[str] = None, ex: Optional[int] = None, **kwargs) -> None:
        """
        Updates the dict
        """
        if isinstance(other, KDBDict) and hasattr(other, 'adict'):
            await self.ahmset(other.adict(), key, ex = ex)
        elif isinstance(other, dict):
            await self.ahmset(other, key, ex = ex)
        if kwargs:
            await self.ahmset(kwargs, key, ex = ex)

    def union(self, other: Union['KDBDict', Dict[str, Any]]) -> Dict[str, Any]:
        """
        Returns the union of the two objects
        """
        data = self.copy()
        if isinstance(other, KDBDict) and hasattr(other, 'dict'):
            data.update(other.dict())
        elif isinstance(other, dict):
            data.update(other)
        return data
    
    def _update_value(self, src_value: Any, value: Any) -> Any:
        if isinstance(src_value, list):
            if isinstance(value, (list, set)):
                src_value.extend(value)
            else:
                src_value.append(value)
            return src_value
        
        if isinstance(src_value, dict):
            src_value.update(value)
            return src_value
        
        if isinstance(src_value, set):
            if isinstance(value, (list, set)):
                src_value.update(value)
            else:
                src_value.add(value)
            return src_value
        
        if isinstance(src_value, tuple):
            src_value = list(src_value)
            src_value.append(value)
            return src_value
        
        if isinstance(src_value, (float, int)):
            return src_value + value
        
        if isinstance(src_value, str):
            return src_value + value
        
        raise TypeError(f"Cannot update value of type {type(src_value)} with {type(value)}")


    def up(self, field: str, value: Any, default: Optional[Any] = None, key: Optional[str] = None, ex: Optional[int] = None) -> None:
        """
        Updates the key
        """
        _value = self.hget(field, default, key)
        if _value is None:
            self.hset(field, value, key, ex = ex)
            return
        _value = self._update_value(_value, value)
        self.hset(field, _value, key, ex = ex)
    
    async def aup(self, field: str, value: Any, default: Optional[Any] = None, key: Optional[str] = None, ex: Optional[int] = None) -> None:
        """
        Updates the key
        """
        _value = await self.ahget(field, default, key)
        if _value is None:
            await self.ahset(field, value, key, ex = ex)
            return
        _value = self._update_value(_value, value)
        await self.ahset(field, _value, key, ex = ex)


    def __or__(self, other: Union['KDBDict', Dict[str, Any]]) -> Dict[str, Any]:
        """
        Returns the union of the two objects
        """
        return self.union(other)
    

    def __repr__(self) -> str:
        """
        Returns the representation of the object
        """
        return f'<{self.__class__.__name__} builder={self.name}, name_key={self.name_key}, primary_key={self.primary_key}, index_key={self.name_key}, match_key={self.name_match_key}, name_prefix_enabled={self.name_prefix_enabled}, keys={len(self)}, uri={self.kdb.uri}>'
    
    @classmethod
    def validate(cls, v: Union['KDBDict', Dict[str, Any], str, Iterable[Any], Any]) -> KDBDict:
        """
        Validates the given value
        """
        if isinstance(v, cls):
            return v
        elif isinstance(v, dict):
            return cls(**v)
        elif isinstance(v, str):
            return cls(primary_key = v)
        elif isinstance(v, Iterable):
            return cls(*v)
        return cls(v)


class AsyncKDBDict(KDBDict):
    
    """
    Async KDBDict with async magic methods
    """

    # Implement reverse lookup methods

    async def set(self, field: Union[str, int], value: Any, key: Optional[str] = None, ex: Optional[int] = None) -> int:
        """
        Sets the value for the given key
        """
        _key = self.get_key(key)
        r = await self.kdb.async_hset(_key, field, self.encode(value))
        if self.auto_expire:
            await self.kdb.async_expire(_key, ex or self.expiration)
        
        if self.is_lookup_index and isinstance(value, str) and isinstance(field, int):
            _lkey = self.get_key(key, lookup = True)
            # logger.info(f'adding lookup index: {_lkey} {value} {field}')
            await self.kdb.async_hset(_lkey, value, field)
        return r
    
    async def ahset(self, field: Union[str, int], value: Any, key: Optional[str] = None, ex: Optional[int] = None) -> int:
        """
        Sets the value for the given key
        """
        return await self.set(field, value, key, ex)
    
    async def get(self, field: Union[str, int], default: Optional[DT] = None, key: Optional[str] = None) -> DT:
        """
        Returns the value for the given key
        """
        if self.is_lookup_index and isinstance(field, str):
            _lkey = self.get_key(key, lookup = True)
            # logger.info(f'getting lookup index: {_lkey} {field}')
            value = await self.kdb.async_hget(_lkey, field)
            if value is not None: return int(value.decode())
    
        value = await self.kdb.async_hget(self.get_key(key), field)
        return self.decode(value) if value is not None else default
    
    
    async def ahget(self, field: Union[str, int], default: Optional[DT] = None, key: Optional[str] = None) -> DT:
        """
        Returns the value for the given key
        """
        return await self.get(field, default, key)
    

    async def append(self, field: Union[str, int], value: Any, key: Optional[str] = None, ex: Optional[int] = None) -> int:
        """
        Appends the value to the list
        """
        if not await self.exists(field = field, key = key):
            return await self.ahset(field, [value], key, ex = ex)
        return await self.ahset(field = field, value = (await self.ahget(field = field, key = key) + [value]), key = key, ex = ex)

    async def exists(self, field: Union[str, int], key: Optional[str] = None) -> bool:
        """
        Returns the length of the given key
        """
        if self.is_lookup_index and isinstance(field, str):
            _lkey = self.get_key(key, lookup = True)
            # logger.info(f'exists lookup index: {_lkey} {field}')
            return await self.kdb.async_hexists(_lkey, field)

        return await self.kdb.async_hexists(self.get_key(key), field)
    
    async def exists_all(self, *fields: Union[str, int], key: Optional[str] = None) -> bool:
        """
        Returns whether all the given keys exist
        """
        _key = self.get_key(key, lookup = self.is_lookup_index)
        all_keys = [k.decode() for k in await self.kdb.async_hkeys(_key)]
        fields = [str(field) for field in fields]
        return all(field in all_keys for field in fields)


    async def ahexists(self, field: Union[str, int], key: Optional[str] = None) -> bool:
        """
        Returns the length of the given key
        """
        return await self.exists(field, key)
    
    async def __getitem__(self, key: Union[str, int]) -> Any:
        """
        Returns the value for the given key
        # Works for Async
        """
        if key == 'count':
            return int(await self.kdb.async_get(self.name_count_key, 0, _return_raw_value = True, _serializer = True))
        return await self.ahget(key)
    
    async def delete(self, field: Union[str, int], key: Optional[str] = None) -> int:
        """
        Deletes the given key
        """
        if self.is_lookup_index and isinstance(field, str):
            _lkey = self.get_key(key, lookup = True)
            await self.kdb.async_hdel(_lkey, field)
        return await self.kdb.async_hdel(self.get_key(key), field)
    
    async def has(self, key: Union[str, int, List[Union[str, int]]]) -> bool:
        """
        Returns whether the given key is in the database
        """
        if key == 'count':
            return await self.kdb.async_exists(self.name_count_key)
        return await self.exists_all(*key) if isinstance(key, list) else await self.exists(key)

    async def aincr(self, field: Union[str, int], amount: int = 1, key: Optional[str] = None) -> int:
        """
        Increments the given key
        """
        return await self.ahincr(field = field, amount = amount, key = key)
    
    async def adecr(self, field: Union[str, int], amount: int = 1, key: Optional[str] = None) -> int:
        """
        Decrements the given key
        """
        return await self.ahdecr(field = field, amount = amount, key = key)
    

    @property
    async def length(self) -> int:
        """
        Returns the length of the given key
        """
        return await self.ahlen()

    @property
    async def idx(self) -> int:
        """
        Returns the index for the given key
        """
        try:
            return await self.kdb.async_incr(self.name_count_key)
        except Exception as e:
            # Probably an error with serialization
            _idx = int(await self.kdb.async_get(self.name_count_key, 0, _return_raw_value = True, _serializer = True))
            await self.kdb.async_set(self.name_count_key, _idx + 1, ex = self.expiration, _serializer = True)
            return _idx + 1
    
    async def get_idx(self, key: str) -> int:
        """
        Returns the index for the given key
        """
        return await self.ahget(key) if await self.has(key) else await self.idx
    

    @property
    async def dkeys(self) -> List[str]:
        """
        Returns the keys for the given key
        """
        return await self.ahkeys()
    
    @property
    async def dvalues(self) -> List[Any]:
        """
        Returns the values for the given key
        """
        return await self.ahvals()
    
    """
    Lookup props
    """
    @property
    async def lkeys(self) -> List[str]:
        """
        Returns the lookup keys
        """
        if not self.is_lookup_index: return []
        keys = await self.kdb.async_hkeys(self.get_key(lookup = True))
        return [key.decode() for key in keys]

    @property
    async def lvalues(self) -> List[int]:
        """
        Returns the lookup values
        """
        if not self.is_lookup_index: return []
        return [int(value.decode()) for value in await self.kdb.async_hvals(self.get_key(lookup = True))]
    
    @property
    async def litems(self) -> Dict[str, int]:
        """
        Returns the lookup items
        """
        if not self.is_lookup_index: return {}
        items = await self.kdb.async_hgetall(self.get_key(lookup = True))
        return {key.decode(): int(value.decode()) for key, value in items.items()}
    
    async def lhas(self, *keys: Union[str, int]) -> bool:
        """
        Checks whether the lookup key exists
        """
        if not self.is_lookup_index: return False
        lookup_keys = []
        if any(isinstance(key, str) for key in keys):
            lookup_keys += await self.lkeys
        if any(isinstance(key, int) for key in keys):
            lookup_keys += await self.lvalues
        return all(key in lookup_keys for key in keys)

    async def get_keys(self, key: Optional[str] = None, **kwargs) -> List[str]:
        """
        Returns the keys for the given key
        """
        match = self.get_match_key(key)
        keys = []
        # from lazyops.utils import logger
        # logger.warning(f'getting keys: {match}')
        async for key in self.kdb.async_scan_iter(match = match, **kwargs):
            keys.append(key.decode())
        return keys
    
    async def get_items(self, key: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """
        Returns the keys for the given key
        """
        match = self.get_match_key(key)
        items = {}
        async for key in self.kdb.async_scan_iter(match = match, **kwargs):
            items[key.decode()] = await self.kdb.async_get(key)
        return items
    

KDBDict.register(dict)
AsyncKDBDict.register(dict)