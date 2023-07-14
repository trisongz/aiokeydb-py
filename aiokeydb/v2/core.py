from __future__ import annotations

import typing
import logging
import datetime
from redis.commands import (
    CoreCommands,
    AsyncCoreCommands,
    RedisModuleCommands,
    SentinelCommands,
    AsyncSentinelCommands,
)
from redis.client import (
    #AbstractRedis,
    Redis,
    Pipeline,
    PubSub,
    CaseInsensitiveDict,
    parse_scan,
    
)
from redis.asyncio.client import (
    Redis as AsyncRedis,
    PubSub as AsyncPubSub,
    Pipeline as AsyncPipeline,
)

from aiokeydb.v2.connection import (
    ConnectionPool,
    AsyncConnectionPool,
)

from typing import Any, Iterable, Mapping, Callable, Union, overload, TYPE_CHECKING
from typing_extensions import Literal

if TYPE_CHECKING:
    from redis.typing import ChannelT
    from redis.client import _Key, _Value, _StrType

logger = logging.getLogger(__name__)


"""
Retryable Components for AsyncKeyDB
"""
from .utils.helpers import get_retryable_wrapper

retryable_wrapper = get_retryable_wrapper()


class RetryablePubSub(PubSub):
    """
    Retryable PubSub
    """

    @retryable_wrapper
    def subscribe(self, *args: 'ChannelT', **kwargs: typing.Callable):
        """
        Subscribe to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via ``listen()`` or
        ``get_message()``.
        """
        return super().subscribe(*args, **kwargs)
    
    @retryable_wrapper
    def unsubscribe(self, *args):
        """
        Unsubscribe from the supplied channels. If empty, unsubscribe from
        all channels
        """
        return super().unsubscribe(*args)

    @retryable_wrapper
    def listen(self) -> typing.Iterator:
        """Listen for messages on channels this client has been subscribed to"""
        yield from super().listen()


class RetryablePipeline(Pipeline):
    """
    Retryable Pipeline
    """

    @retryable_wrapper
    def execute(self, raise_on_error: bool = True) -> typing.List[typing.Any]:
        """Execute all the commands in the current pipeline"""
        return super().execute(raise_on_error = raise_on_error)


PubSubT = Union[PubSub, RetryablePubSub]
PipelineT = Union[Pipeline, RetryablePipeline]

class KeyDB(Redis):
    """
    Implementation of the KeyDB protocol.

    This abstract class provides a Python interface to all KeyDB commands
    and an implementation of the KeyDB protocol.

    Pipelines derive from this, implementing how
    the commands are sent and received to the KeyDB server. Based on
    configuration, an instance will either use a ConnectionPool, or
    Connection object to talk to keydb.

    It is not safe to pass PubSub or Pipeline objects between threads.
    """

    @property
    def is_async(self):
        return False

    @classmethod
    def from_url(
        cls, 
        url, 
        pool_class: typing.Optional[typing.Type[ConnectionPool]] = ConnectionPool,
        **kwargs
    ):
        """
        Return a Redis client object configured from the given URL

        For example::

            keydb://[[username]:[password]]@localhost:6379/0
            keydbs://[[username]:[password]]@localhost:6379/0
            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0
            unix://[username@]/path/to/socket.sock?db=0[&password=password]

        Five URL schemes are supported:

        - `keydb://` creates a TCP socket connection.
        - `keydbs://` creates a SSL wrapped TCP socket connection.
        - `redis://` creates a TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/redis>
        - `rediss://` creates a SSL wrapped TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/rediss>
        - ``unix://``: creates a Unix Domain Socket connection.

        The username, password, hostname, path and all querystring values
        are passed through urllib.parse.unquote in order to replace any
        percent-encoded values with their corresponding characters.

        There are several ways to specify a database number. The first value
        found will be used:

            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// or rediss:// schemes, the path argument
               of the url, e.g. redis://localhost/0
            3. A ``db`` keyword argument to this function.

        If none of these options are specified, the default db=0 is used.

        All querystring options are cast to their appropriate Python types.
        Boolean arguments can be specified with string values "True"/"False"
        or "Yes"/"No". Values that cannot be properly cast cause a
        ``ValueError`` to be raised. Once parsed, the querystring arguments
        and keyword arguments are passed to the ``ConnectionPool``'s
        class initializer. In the case of conflicting arguments, querystring
        arguments always win.

        """
        if not pool_class: pool_class = ConnectionPool
        connection_pool = pool_class.from_url(url, **kwargs)
        return cls(connection_pool=connection_pool)


    def pubsub(
        self, 
        retryable: typing.Optional[bool] = False,
        **kwargs,
    ) -> PubSubT:
        """
        Return a Publish/Subscribe object. With this object, you can
        subscribe to channels and listen for messages that get published to
        them.
        """
        pubsub_class = RetryablePubSub if retryable else PubSub
        return pubsub_class(connection_pool = self.connection_pool, **kwargs)
    

    def pipeline(
        self, 
        transaction: bool = True, 
        shard_hint: typing.Optional[str] = None,
        retryable: typing.Optional[bool] = False,
    ) -> PipelineT:
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        pipeline_class = RetryablePipeline if retryable else Pipeline
        return pipeline_class(
            connection_pool = self.connection_pool, 
            response_callbacks = self.response_callbacks, 
            transaction = transaction, 
            shard_hint = shard_hint
        )



class AsyncRetryablePubSub(AsyncPubSub):
    """
    Retryable PubSub
    """

    @retryable_wrapper
    async def subscribe(self, *args: 'ChannelT', **kwargs: typing.Callable):
        """
        Subscribe to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via ``listen()`` or
        ``get_message()``.
        """
        return await super().subscribe(*args, **kwargs)
    
    @retryable_wrapper
    def unsubscribe(self, *args) -> typing.Awaitable:
        """
        Unsubscribe from the supplied channels. If empty, unsubscribe from
        all channels
        """
        return super().unsubscribe(*args)

    @retryable_wrapper
    async def listen(self) -> typing.AsyncIterator:
        """Listen for messages on channels this client has been subscribed to"""
        async for response in super().listen():
            yield response

class AsyncRetryablePipeline(AsyncPipeline):
    """
    Retryable Pipeline
    """

    @retryable_wrapper
    async def execute(self, raise_on_error: bool = True):
        """Execute all the commands in the current pipeline"""
        return await super().execute(raise_on_error = raise_on_error)
    
    if typing.TYPE_CHECKING:
        # This is only for type checking purposes, and will be removed in the future.
        def exists(self, *names: _Key) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def expire(  # type: ignore[override]
            self, name: _Key, time: int | datetime.timedelta, nx: bool = False, xx: bool = False, gt: bool = False, lt: bool = False
        ) -> 'AsyncRetryablePipeline': ...
        def expireat(self, name, when, nx: bool = False, xx: bool = False, gt: bool = False, lt: bool = False) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def get(self, name: _Key) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def getdel(self, name: _Key) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def getex(  # type: ignore[override]
            self,
            name,
            ex: Any | None = None,
            px: Any | None = None,
            exat: Any | None = None,
            pxat: Any | None = None,
            persist: bool = False,
        ) -> 'AsyncRetryablePipeline': ...
        def set(  # type: ignore[override]
            self,
            name: _Key,
            value: _Value,
            ex: None | int | datetime.timedelta = None,
            px: None | int | datetime.timedelta = None,
            nx: bool = False,
            xx: bool = False,
            keepttl: bool = False,
            get: bool = False,
            exat: Any | None = None,
            pxat: Any | None = None,
        ) -> 'AsyncRetryablePipeline': ...
        def setbit(self, name: _Key, offset: int, value: int) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def setex(self, name: _Key, time: int | datetime.timedelta, value: _Value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def setnx(self, name: _Key, value: _Value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def setrange(self, name, offset, value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def stralgo(  # type: ignore[override]
            self,
            algo,
            value1,
            value2,
            specific_argument: str = "strings",
            len: bool = False,
            idx: bool = False,
            minmatchlen: Any | None = None,
            withmatchlen: bool = False,
            **kwargs: Any,
        ) -> 'AsyncRetryablePipeline': ...
        def strlen(self, name) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def substr(self, name, start, end: int = -1) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def touch(self, *args) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def ttl(self, name: _Key) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        @overload
        def blpop(self, keys: _Value | Iterable[_Value], timeout: Literal[0] | None = 0) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        @overload
        def blpop(self, keys: _Value | Iterable[_Value], timeout: float) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        @overload
        def brpop(self, keys: _Value | Iterable[_Value], timeout: Literal[0] | None = 0) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        @overload
        def brpop(self, keys: _Value | Iterable[_Value], timeout: float) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def brpoplpush(self, src, dst, timeout: int | None = 0) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def lindex(self, name: _Key, index: int) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def linsert(  # type: ignore[override]
            self, name: _Key, where: Literal["BEFORE", "AFTER", "before", "after"], refvalue: _Value, value: _Value
        ) -> 'AsyncRetryablePipeline': ...
        def llen(self, name: _Key) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def lpop(self, name, count: int | None = None) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def lpush(self, name: _Value, *values: _Value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def lpushx(self, name, value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def lrange(self, name: _Key, start: int, end: int) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def lrem(self, name: _Key, count: int, value: _Value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def lset(self, name: _Key, index: int, value: _Value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def ltrim(self, name: _Key, start: int, end: int) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def rpop(self, name, count: int | None = None) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def rpoplpush(self, src, dst) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def rpush(self, name: _Value, *values: _Value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def rpushx(self, name, value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]

        def zadd(  # type: ignore[override]
        self,
        name: _Key,
        mapping: Mapping[_Key, _Value],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        incr: bool = False,
        gt: Any | None = False,
        lt: Any | None = False,
        ) -> 'AsyncRetryablePipeline': ...
        def zcard(self, name: _Key) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zcount(self, name: _Key, min: _Value, max: _Value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zdiff(self, keys, withscores: bool = False) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zdiffstore(self, dest, keys) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zincrby(self, name: _Key, amount: float, value: _Value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zinter(self, keys, aggregate: Any | None = None, withscores: bool = False) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zinterstore(self, dest: _Key, keys: Iterable[_Key], aggregate: Literal["SUM", "MIN", "MAX"] | None = None) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zlexcount(self, name: _Key, min: _Value, max: _Value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zpopmax(self, name: _Key, count: int | None = None) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zpopmin(self, name: _Key, count: int | None = None) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zrandmember(self, key, count: Any | None = None, withscores: bool = False) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        @overload  # type: ignore[override]
        def bzpopmax(self, keys: _Key | Iterable[_Key], timeout: Literal[0] = 0) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def bzpopmax(self, keys: _Key | Iterable[_Key], timeout: float) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def bzpopmin(self, keys: _Key | Iterable[_Key], timeout: Literal[0] = 0) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def bzpopmin(self, keys: _Key | Iterable[_Key], timeout: float) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def zrange(
            self,
            name: _Key,
            start: int,
            end: int,
            desc: bool,
            withscores: Literal[True],
            score_cast_func: Callable[[_StrType], Any],
            byscore: bool = False,
            bylex: bool = False,
            offset: int | None = None,
            num: int | None = None,
        ) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def zrange(
            self,
            name: _Key,
            start: int,
            end: int,
            desc: bool,
            withscores: Literal[True],
            score_cast_func: Callable[[_StrType], float] = ...,
            byscore: bool = False,
            bylex: bool = False,
            offset: int | None = None,
            num: int | None = None,
        ) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def zrange(
            self,
            name: _Key,
            start: int,
            end: int,
            *,
            withscores: Literal[True],
            score_cast_func: Callable[[_StrType], None],
            byscore: bool = False,
            bylex: bool = False,
            offset: int | None = None,
            num: int | None = None,
        ) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def zrange(
            self,
            name: _Key,
            start: int,
            end: int,
            *,
            withscores: Literal[True],
            score_cast_func: Callable[[_StrType], float] = ...,
            byscore: bool = False,
            bylex: bool = False,
            offset: int | None = None,
            num: int | None = None,
        ) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def zrange(
            self,
            name: _Key,
            start: int,
            end: int,
            desc: bool = False,
            withscores: bool = False,
            score_cast_func: Callable[[_StrType], Any] = ...,
            byscore: bool = False,
            bylex: bool = False,
            offset: int | None = None,
            num: int | None = None,
        ) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def zrevrange(
            self, name: _Key, start: int, end: int, withscores: Literal[True], score_cast_func: Callable[[_StrType], None]
        ) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def zrevrange(self, name: _Key, start: int, end: int, withscores: Literal[True]) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def zrevrange(
            self, name: _Key, start: int, end: int, withscores: bool = False, score_cast_func: Callable[[Any], Any] = ...
        ) -> 'AsyncRetryablePipeline': ...
        def zrangestore(  # type: ignore[override]
            self,
            dest,
            name,
            start,
            end,
            byscore: bool = False,
            bylex: bool = False,
            desc: bool = False,
            offset: Any | None = None,
            num: Any | None = None,
        ) -> 'AsyncRetryablePipeline': ...
        def zrangebylex(self, name: _Key, min: _Value, max: _Value, start: int | None = None, num: int | None = None) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zrevrangebylex(self, name: _Key, max: _Value, min: _Value, start: int | None = None, num: int | None = None) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        @overload  # type: ignore[override]
        def zrangebyscore(
            self,
            name: _Key,
            min: _Value,
            max: _Value,
            start: int | None = None,
            num: int | None = None,
            *,
            withscores: Literal[True],
            score_cast_func: Callable[[_StrType], None],
        ) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def zrangebyscore(
            self, name: _Key, min: _Value, max: _Value, start: int | None = None, num: int | None = None, *, withscores: Literal[True]
        ) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def zrangebyscore(
            self,
            name: _Key,
            min: _Value,
            max: _Value,
            start: int | None = None,
            num: int | None = None,
            withscores: bool = False,
            score_cast_func: Callable[[_StrType], Any] = ...,
        ) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def zrevrangebyscore(
            self,
            name: _Key,
            max: _Value,
            min: _Value,
            start: int | None = None,
            num: int | None = None,
            *,
            withscores: Literal[True],
            score_cast_func: Callable[[_StrType], Any],
        ) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def zrevrangebyscore(
            self, name: _Key, max: _Value, min: _Value, start: int | None = None, num: int | None = None, *, withscores: Literal[True]
        ) -> 'AsyncRetryablePipeline': ...
        @overload  # type: ignore[override]
        def zrevrangebyscore(
            self,
            name: _Key,
            max: _Value,
            min: _Value,
            start: int | None = None,
            num: int | None = None,
            withscores: bool = False,
            score_cast_func: Callable[[_StrType], Any] = ...,
        ) -> 'AsyncRetryablePipeline': ...
        def zrank(self, name: _Key, value: _Value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zrem(self, name: _Key, *values: _Value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zremrangebylex(self, name: _Key, min: _Value, max: _Value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zremrangebyrank(self, name: _Key, min: int, max: int) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zremrangebyscore(self, name: _Key, min: _Value, max: _Value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zrevrank(self, name: _Key, value: _Value, withscore: bool = False) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zscore(self, name: _Key, value: _Value) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zunion(self, keys, aggregate: Any | None = None, withscores: bool = False) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zunionstore(self, dest: _Key, keys: Iterable[_Key], aggregate: Literal["SUM", "MIN", "MAX"] | None = None) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]
        def zmscore(self, key, members) -> 'AsyncRetryablePipeline': ...  # type: ignore[override]



AsyncPipelineT = Union[AsyncPipeline, AsyncRetryablePipeline]
AsyncPubSubT = Union[AsyncPubSub, AsyncRetryablePubSub]

class AsyncKeyDB(AsyncRedis):
    """
    Implementation of the KeyDB protocol.

    This abstract class provides a Python interface to all KeyDB commands
    and an implementation of the KeyDB protocol.

    Pipelines derive from this, implementing how
    the commands are sent and received to the KeyDB server. Based on
    configuration, an instance will either use a AsyncConnectionPool, or
    AsyncConnection object to talk to redis.
    """

    @property
    def is_async(self):
        return True
    
    @classmethod
    def from_url(
        cls, 
        url: str, 
        pool_class: typing.Optional[typing.Type[AsyncConnectionPool]] = AsyncConnectionPool,
        single_connection_client: bool = False,
        **kwargs
    ):
        """
        Return a KeyDB client object configured from the given URL

        For example::
            keydb://[[username]:[password]]@localhost:6379/0
            keydbs://[[username]:[password]]@localhost:6379/0
            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0
            unix://[[username]:[password]]@/path/to/socket.sock?db=0

        Five URL schemes are supported:
        - `keydb://` creates a TCP socket connection.
        - `keydbs://` creates a SSL wrapped TCP socket connection.
        - `redis://` creates a TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/redis>
        - `rediss://` creates a SSL wrapped TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/rediss>
        - ``unix://``: creates a Unix Domain Socket connection.

        The username, password, hostname, path and all querystring values
        are passed through urllib.parse.unquote in order to replace any
        percent-encoded values with their corresponding characters.

        There are several ways to specify a database number. The first value
        found will be used:
            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// or rediss:// schemes, the path argument
               of the url, e.g. redis://localhost/0
            3. A ``db`` keyword argument to this function.

        If none of these options are specified, the default db=0 is used.

        All querystring options are cast to their appropriate Python types.
        Boolean arguments can be specified with string values "True"/"False"
        or "Yes"/"No". Values that cannot be properly cast cause a
        ``ValueError`` to be raised. Once parsed, the querystring arguments
        and keyword arguments are passed to the ``AsyncConnectionPool``'s
        class initializer. In the case of conflicting arguments, querystring
        arguments always win.

        """
        if pool_class is None: pool_class = AsyncConnectionPool
        connection_pool = pool_class.from_url(url, **kwargs)
        return cls(
            connection_pool = connection_pool,
            single_connection_client = single_connection_client,
        )


    def pubsub(
        self, 
        retryable: typing.Optional[bool] = False,
        **kwargs,
    ) -> AsyncPubSubT:
        """
        Return a Publish/Subscribe object. With this object, you can
        subscribe to channels and listen for messages that get published to
        them.
        """
        pubsub_class = AsyncRetryablePubSub if retryable else AsyncPubSub
        return pubsub_class(connection_pool = self.connection_pool, **kwargs)
    

    def pipeline(
        self, 
        transaction: bool = True, 
        shard_hint: typing.Optional[str] = None,
        retryable: typing.Optional[bool] = False,
    ) -> AsyncPipelineT:
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        pipeline_class = AsyncRetryablePipeline if retryable else AsyncPipeline
        return pipeline_class(
            connection_pool = self.connection_pool, 
            response_callbacks = self.response_callbacks, 
            transaction = transaction, 
            shard_hint = shard_hint
        )
