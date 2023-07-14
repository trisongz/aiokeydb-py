from __future__ import annotations

import os
import json
import socket
import contextlib
import logging

from typing import Optional, Dict, Any, Union, Type, Mapping, Callable, List

from lazyops.utils.logs import default_logger as logger

import aiokeydb.v2.exceptions as exceptions
from aiokeydb.v2.types import BaseSettings, validator, root_validator, lazyproperty, KeyDBUri
from aiokeydb.v2.serializers import SerializerType, BaseSerializer
from aiokeydb.v2.utils import import_string
from aiokeydb.v2.configs.worker import KeyDBWorkerSettings

from aiokeydb.v2.backoff import default_backoff


_default_db_mapping = {
    'default': 0,
    'worker': 2,
    'cache': 5,
}


class KeyDBSettings(BaseSettings):
    """
    KeyDB Settings

    - url: The KeyDB URL
    - host: The KeyDB Host
    - port: The KeyDB Port
    - db: The KeyDB DB
    - username: The KeyDB Username
    - password: The KeyDB Password
    - ssl: Whether to use SSL
    - cache_ttl: The default TTL for cached items
    - serializer: The default serializer
    - db_mapping: The default DB mapping

    """

    url: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    db: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None

    socket_timeout: Optional[float] = None
    socket_connect_timeout: Optional[float] = None
    socket_keepalive: Optional[bool] = None
    socket_keepalive_options: Optional[Mapping[int, Union[int, bytes]]] = None
    unix_socket_path: Optional[str] = None

    pool_class: Optional[str] = None
    pool_max_connections: Optional[int] = None

    async_pool_class: Optional[str] = None
    async_pool_max_connections: Optional[int] = None

    auto_close_connection_pool: Optional[bool] = True
    auto_close_async_connection_pool: Optional[bool] = True

    redis_connect_func: Optional[Union[str, Callable]] = None
    async_redis_connect_func: Optional[Union[str, Callable]] = None

    
    ssl: Optional[bool] = False
    ssl_keyfile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_cert_reqs: str = "required"
    ssl_ca_certs: Optional[str] = None
    ssl_ca_data: Optional[str] = None
    ssl_check_hostname: bool = False

    cache_ttl: Optional[int] = 60 * 60 * 24  # 1 day
    cache_prefix: Optional[str] = 'cache_'
    cache_enabled: Optional[bool] = True
    # cache_db_id: Optional[int] = None

    worker_enabled: Optional[bool] = True
    # worker_db_id: Optional[int] = None
    

    # socket_timeout: Optional[float] = 60.0
    # socket_connect_timeout: Optional[float] = 60.0
    # connection_timeout: Optional[int] = 300
    # socket_keepalive: Optional[bool] = True
    # retry_on_timeout: Optional[bool] = True

    retry_on_timeout: Optional[bool] = True
    retry_on_error: Optional[list] = None
    retry_on_connection_error: Optional[bool] = True
    retry_on_connection_reset_error: Optional[bool] = True
    retry_on_response_error: Optional[bool] = True
    retry_enabled: Optional[bool] = True

    retry_client_enabled: Optional[bool] = True
    retry_client_max_attempts: Optional[int] = 15
    retry_client_max_delay: Optional[int] = 60
    retry_client_logging_level: Optional[int] = logging.INFO

    single_connection_client: Optional[bool] = False
    health_check_interval: Optional[int] = 15

    encoding: Optional[str] = 'utf-8'
    encoding_errors: Optional[str] = 'strict'
    serializer: Optional[SerializerType] = SerializerType.default
    db_mapping: Optional[Union[str, Dict[str, int]]] = _default_db_mapping

    config_kwargs: Optional[Union[str, Dict[str, Any]]] = {}
    
    log_level: Optional[str] = 'DEBUG'
    debug_enabled: Optional[bool] = False

    is_leader_process: Optional[bool] = None

    class Config:
        case_sensitive = False
        env_prefix = 'KEYDB_'
    
    @lazyproperty
    def build_id(self):
        return os.getenv('BUILD_ID', 'N/A')
    
    @lazyproperty
    def app_name(self):
        return os.getenv('APP_NAME', None)
    
    @lazyproperty
    def app_version(self):
        return os.getenv('APP_VERSION', 'N/A')
    
    @property
    def version(self):
        from aiokeydb.version import VERSION
        return VERSION

    @lazyproperty
    def worker(self) -> KeyDBWorkerSettings:
        conf = KeyDBWorkerSettings()
        if conf.db is None: conf.db = self.get_db_id('worker')
        return conf
    
    @validator('config_kwargs', pre = True, always = True)
    def validate_config_kwargs(cls, v, values: Dict) -> Dict[str, Any]:
        if not v: return {}
        return json.loads(v) if isinstance(v, str) else v

    @validator('serializer', pre = True, always = True)
    def validate_serializer(cls, v, values: Dict) -> SerializerType:
        return SerializerType(v) if isinstance(v, str) else v
    
    @validator('host')
    def validate_host(cls, v, values: Dict) -> str:
        # Use keydb Host if set, otherwise do some validation
        if not v:
            with contextlib.suppress(socket.gaierror):
                # Try to catch keydb in docker-compose
                return socket.gethostbyname('keydb')[0]
            return 'localhost'
        return v
    
    @validator('port')
    def validate_port(cls, v, values: Dict) -> int:
        return v or 6379
    
    @validator('db_mapping', pre = True, always = True)
    def validate_db_mapping(cls, v, values: Dict) -> Dict[str, int]:
        return json.loads(v) if isinstance(v, str) else v

    @property
    def protocol(self):
        return 'keydbs' if self.ssl else 'keydb'

    @property
    def root_uri(self) -> str:
        """
        Returns the Root URI
        -> host:port
        """
        if not self.url: return f'{self.host}:{self.port}'
        base = self.url
        if '/' in base[-4:]: base = base.rsplit('/', 1)[0]
        if '://' in base: base = base.split('://', 1)[-1]
        return base
    
    @property
    def base_uri(self) -> str:
        """
        Returns the Base URI
        -> scheme://host:port
        """
        if not self.url: return f'{self.protocol}://{self.host}:{self.port}'
        base = self.url
        if '/' in base[-4:]: base = base.rsplit('/', 1)[0]
        return base

    @property
    def loglevel(self) -> int:
        """
        Returns the log level
        """
        return getattr(logging, self.log_level.upper(), logging.INFO)

    @property
    def host_uri(self):
        return f'{self.host}:{self.port}' if self.host else self.base_uri.split('://', 1)[-1]
    
    @property
    def hostname_uri(self):
        """
        Returns ``{scheme}://{host}``
        """
        return f'{self.protocol}://{self.host}'

    @property
    def auth_str(self):
        """
        returns the auth string in the format of 
        ``username:password`` or ``:password``

        """
        if self.username and self.password:
            return f'{self.username}:{self.password}'
        return f':{self.password}' if self.password else None
    
    @property
    def db_id(self) -> int:
        """
        Returns the default DB ID

        if ``self.db`` is set, it will return that value
        """
        if self.db is not None: return self.db
        if self.db_mapping and self.db_mapping.get('default') is not None:
            return self.db_mapping['default']
        
        _db_id = 0
        _base_uri = self.url or self.host_uri
        if '/' in _base_uri[-4:]:
            split = _base_uri.rsplit('/', 1)
            _db_id = int(split[1])
        if not self.db_mapping: self.db_mapping = {'default': _db_id}
        return _db_id
    
    @property
    def cache_db_id(self) -> int:
        """
        returns the `cache` DB ID
        - if it's found in ``self.db_mapping``
        - otherwise it will set to the next db id
        """
        if self.db_mapping and self.db_mapping.get('cache') is not None:
            return self.db_mapping['cache']
        db_id = (len(self.db_mapping) if self.db_mapping else 1) + 1
        if not self.db_mapping: self.db_mapping = {}
        self.db_mapping['cache'] = db_id
        return db_id
    
    def get_db_id(
        self,
        name: str,
        db: Optional[int] = None,
    ):
        """
        Returns the DB ID for the given name
        or the next available DB ID
        """
        if db is not None: return db
        if self.db_mapping and self.db_mapping.get(name) is not None:
            return self.db_mapping[name]
        db_id = (len(self.db_mapping) if self.db_mapping else 1) + 1
        if not self.db_mapping: self.db_mapping = {}
        self.db_mapping[name] = db_id
        return db_id

    @lazyproperty
    def connection_uri_no_auth(self) -> str:
        """
        Returns the Connection URI without Auth
        """
        return f'{self.protocol}://{self.root_uri}/{self.db_id}'
    
    @lazyproperty
    def connection_uri(self) -> str:
        """
        Returns the full connection URI with Auth
        """
        base_uri = self.root_uri
        if self.auth_str: base_uri = f'{self.auth_str}@{base_uri}'
        return f'{self.protocol}://{base_uri}/{self.db_id}'
    
    def get_connection_uri(
        self, 
        db_id: int = None,
        with_auth: bool = True,
    ) -> str:
        """
        Returns the full connection URI with Auth
        """
        base_uri = self.base_uri
        if self.auth_str and with_auth: base_uri = f'{self.auth_str}@{base_uri}'
        return f'{self.protocol}://{base_uri}/{db_id or self.db_id}'

    def create_uri(
        self,
        name: str = None,
        uri: str = None,
        host: str = None,
        port: int = None,
        db_id: int = None,
        username: str = None,
        password: str = None,
        protocol: str = None,
        with_auth: bool = True,
    ) -> KeyDBUri:
        """
        Creates a URI from the given parameters
        """
        if not uri and not host:
            uri = self.base_uri
        elif host:
            uri = f'{protocol or self.protocol}://{host}:{port or self.port}'
        
        if with_auth and '@' not in uri:
            if username and password:
                uri = f'{username}:{password}@{uri}'
            elif self.auth_str:
                uri = f'{self.auth_str}@{uri}'

        if '/' in uri[-4:]:
            split = uri.rsplit('/', 1)
            if db_id is None: db_id = int(split[1])
            uri = split[0]
        
        if db_id is None and name: 
            db_id = self.db_mapping.get(name)
        
        db_id = db_id or self.db_id
        uri = f'{uri}/{db_id}'
        
        return KeyDBUri(dsn = uri)

    def get_serializer(self) -> Type[BaseSerializer]:
        if isinstance(self.serializer, str):
            self.serializer = SerializerType[self.serializer]
        return self.serializer.get_serializer()
    
    def get_connect_func(self, _is_async: bool = False):
        if self.async_redis_connect_func and _is_async:
            return import_string(self.async_redis_connect_func)
        elif self.redis_connect_func and not _is_async:
            return import_string(self.redis_connect_func)
        return None
    
    def get_pool_class(self, _is_async: bool = False):
        if self.async_pool_class and _is_async:
            if isinstance(self.async_pool_class, str):
                if 'blocking' in self.async_pool_class.lower():
                    from aiokeydb.v2.connection import AsyncBlockingConnectionPool
                    return AsyncBlockingConnectionPool
                from aiokeydb.v2.connection import AsyncConnectionPool
                return AsyncConnectionPool
            return self.async_pool_class
        
        elif self.pool_class and not _is_async:
            if isinstance(self.pool_class, str):
                if 'blocking' in self.pool_class.lower():
                    from aiokeydb.v2.connection import BlockingConnectionPool
                    return BlockingConnectionPool
                from aiokeydb.v2.connection import ConnectionPool
                return ConnectionPool
            return self.pool_class
        return None

    def get_default_job_timeout(self) -> int:
        """
        Returns the default job timeout
        """
        return self.worker.job_timeout
    
    def get_default_job_ttl(self) -> int:
        """
        Returns the default job TTL
        """
        return self.worker.job_ttl
    
    def get_default_job_retries(self) -> int:
        """
        Returns the default job retries
        """
        return self.worker.job_retries
    
    def get_default_job_retry_delay(self) -> float:
        """
        Returns the default job retry delay
        """
        return self.worker.job_retry_delay

    @lazyproperty
    def _retry_exceptions(self) -> List[Type[Exception]]:
        """
        Returns the list of retry exceptions
        """
        _retries = []
        if self.retry_on_timeout:
            self.retry_on_timeout = False
            _retries.extend([TimeoutError, exceptions.TimeoutError])
        if self.retry_on_connection_error:
            _retries.extend([ConnectionError, exceptions.ConnectionError])
        if self.retry_on_connection_reset_error:
            _retries.append(ConnectionResetError)
        if self.retry_on_response_error:
            _retries.extend([exceptions.ResponseError, exceptions.BusyLoadingError])
        # if self.retry_on_error:
        #     for exc in self.retry_on_error:
        #         if isinstance(exc, str):
        #             exc = import_string(exc)
        #         _retries.append(exc)
        # logger.info(f'Retry Exceptions: {_retries}')
        return list(set(_retries))

    def get_retry_arg(self, _is_async: bool = False) -> Dict[str, Any]:
        """
        Returns the retry argument
        """
        if _is_async:
            from redis.asyncio.retry import Retry
        else:
            from redis.retry import Retry
        return {
            'retry': Retry(default_backoff(), retries = 3, supported_errors = (exceptions.ConnectionError, exceptions.TimeoutError, exceptions.BusyLoadingError)) \
        } if self.retry_enabled else {}


    @property
    def config_args(
        self
    ):
        return {
            'socket_timeout': self.socket_timeout,
            'socket_connect_timeout': self.socket_connect_timeout,
            'socket_keepalive': self.socket_keepalive,
            'retry_on_error': self._retry_exceptions,
            'retry_on_timeout': self.retry_on_timeout,
            'health_check_interval': self.health_check_interval,
            # "unix_socket_path": self.unix_socket_path,
            # "ssl": self.ssl,
            # "ssl_ca_certs": self.ssl_ca_certs,
            # "ssl_certfile": self.ssl_certfile,
            # "ssl_keyfile": self.ssl_keyfile,
            # "ssl_cert_reqs": self.ssl_cert_reqs,
            # "ssl_check_hostname": self.ssl_check_hostname,
            # "auto_close_connection_pool": self.auto_close_connection_pool,

            # 'connection_timeout': self.connection_timeout,
            **self.config_kwargs
        }

    def get_config(
        self,
        **kwargs
    ):
        config = self.config_args.copy()
        if kwargs: config.update(**kwargs)
        return config
    

    def configure(
        self,
        **kwargs,
    ):
        """
        Configures the Connection
        """
        for key, value in kwargs.items():
            if key in {"cache_db_id", "worker_db_id"}:
                map_key = key.split('_', 1)[0]
                self.db_mapping[map_key] = value
            elif key in {"db", "db_id"}:
                self.db = value
                self.db_mapping["default"] = value
            elif hasattr(self, key):
                setattr(self, key, value)
            elif key == "queue_db":
                self.worker.db = value
            elif hasattr(self.worker, key):
                setattr(self.worker, key, value)

        