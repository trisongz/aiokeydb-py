import os
import json
import socket
import contextlib
import logging
from typing import Optional, Dict, Any, Union, Type, Tuple

from aiokeydb.client.types import BaseSettings, validator, lazyproperty, KeyDBUri
from aiokeydb.client.serializers import SerializerType, BaseSerializer


_default_db_mapping = {
    'default': 0,
    'workers': 2,
    'cache': 5,
}


class KeyDBWorkerSettings(BaseSettings):
    """
    KeyDB Worker Settings

    - db: The KeyDB DB                                  | Default: 2
    - prefix: the absolute prefix                       | Default: 'queue'
    - queue_name: The queue name                        | Default: 'workers'
    - job_key_method: The UUID Method                   | Default: 'uuid4'
    - job_serializer: The default serializer            | Default: 'dill'
    - concurrency: The concurrency per worker           | Default: 100
    - max_concurrency: The queue's max concurrency      | Default: 100
    - management_url: The management URL                | Default: None
    - management_enabled: Whether to enable management  | Default: True

    """
    db: int = 2
    name: Optional[str] = None
    prefix: Optional[str] = 'queue'
    queue_name: Optional[str] = 'workers'
    job_key_method: str = 'uuid4'
    job_serializer: Optional[SerializerType] = SerializerType.dill # Optional[Union[str, SerializerType]] = SerializerType.dill
    job_prefix: Optional[str] = 'job'
    concurrency: Optional[int] = 100
    max_concurrency: Optional[int] = 100
    threadpool_size: Optional[int] = 100

    management_url: Optional[str] = None
    management_enabled: Optional[bool] = True
    management_register_api_path: Optional[str] = '/api/register/queues'

    token: Optional[str] = ''
    debug_enabled: Optional[bool] = False


    class Config:
        case_sensitive = False
        env_prefix = 'KEYDB_WORKER_'
    

    @validator('job_serializer', pre = True, always = True)
    def validate_job_serializer(cls, v, values: Dict) -> SerializerType:
        return SerializerType(v) if isinstance(v, str) else v
    
    @lazyproperty
    def management_endpoint(self) -> str:
        """
        Returns the management endpoint
        """
        if self.management_url is None:
            return None
        from urllib.parse import urljoin
        return urljoin(self.management_url, self.management_register_api_path)

    def configure(
        self, 
        db: Optional[int] = None,
        name: Optional[str] = None,
        prefix: Optional[str] = None,
        queue_name: Optional[str] = None,
        job_key_method: Optional[str] = None,
        job_serializer: Optional[SerializerType] = None,
        job_prefix: Optional[str] = None,
        concurrency: Optional[int] = None,
        max_concurrency: Optional[int] = None,
        threadpool_size: Optional[int] = None,
        management_url: Optional[str] = None,
        management_enabled: Optional[bool] = None,
        management_register_api_path: Optional[str] = None,
        token: Optional[str] = None,
        debug_enabled: Optional[bool] = None,
        **kwargs
    ):
        """
        Configures the settings
        """
        if db is not None:
            self.db = db
        if name is not None:
            self.name = name
        if prefix is not None:
            self.prefix = prefix
        if queue_name is not None:
            self.queue_name = queue_name
        if job_key_method is not None:
            self.job_key_method = job_key_method
        if job_serializer is not None:
            self.job_serializer = job_serializer
        if job_prefix is not None:
            self.job_prefix = job_prefix
        if concurrency is not None:
            self.concurrency = concurrency
        if max_concurrency is not None:
            self.max_concurrency = max_concurrency
        if threadpool_size is not None:
            self.threadpool_size = threadpool_size
        if management_url is not None:
            self.management_url = management_url
        if management_enabled is not None:
            self.management_enabled = management_enabled
        if management_register_api_path is not None:
            self.management_register_api_path = management_register_api_path
        if token is not None:
            self.token = token
        if debug_enabled is not None:
            self.debug_enabled = debug_enabled




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
    ssl: Optional[bool] = False

    cache_ttl: Optional[int] = 60 * 60 * 24  # 1 day
    cache_prefix: Optional[str] = 'cache_'
    cache_enabled: Optional[bool] = True

    worker_enabled: Optional[bool] = True
    
    serializer: Optional[SerializerType] = SerializerType.default
    db_mapping: Optional[Union[str, Dict[str, int]]] = _default_db_mapping

    socket_timeout: Optional[float] = 60.0
    socket_connect_timeout: Optional[float] = 60.0
    socket_keepalive: Optional[bool] = True
    connection_timeout: Optional[int] = 300
    encoding: Optional[str] = 'utf-8'
    encoding_errors: Optional[str] = 'strict'
    config_kwargs: Optional[Union[str, Dict[str, Any]]] = {}
    
    log_level: Optional[str] = 'INFO'
    debug_enabled: Optional[bool] = False

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

    @lazyproperty
    def worker(self) -> KeyDBWorkerSettings:
        return KeyDBWorkerSettings()
    
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
        if not self.url:
            return f'{self.host}:{self.port}'
        base = self.url
        if '/' in base[-4:]:
            base = base.rsplit('/', 1)[0]
        if '://' in base:
            base = base.split('://', 1)[-1]
        return base

    @property
    def loglevel(self) -> int:
        """
        Returns the log level
        """
        return getattr(logging, self.log_level.upper(), logging.INFO)

    @property
    def base_uri(self) -> str:
        """
        Returns the Base URI
        -> scheme://host:port
        """
        if not self.url:
            return f'{self.protocol}://{self.host}:{self.port}'
        base = self.url
        if '/' in base[-4:]:
            base = base.rsplit('/', 1)[0]
        return base
    
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
        if not self.db_mapping:
            self.db_mapping = {'default': _db_id}
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

    @property
    def config_args(
        self
    ):
        return {
            'socket_timeout': self.socket_timeout,
            'socket_connect_timeout': self.socket_connect_timeout,
            'socket_keepalive': self.socket_keepalive,
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
        url: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        db: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        ssl: Optional[bool] = None,
        cache_ttl: Optional[int] = None,
        cache_prefix: Optional[str] = None,
        cache_enabled: Optional[bool] = None,
        worker_enabled: Optional[bool] = None,
        serializer: Optional[SerializerType] = None,
        db_mapping: Optional[Union[str, Dict[str, int]]] = None,
        socket_timeout: Optional[float] = None,
        socket_connect_timeout: Optional[float] = None,
        socket_keepalive: Optional[bool] = None,
        connection_timeout: Optional[int] = None,
        encoding: Optional[str] = None,
        encoding_errors: Optional[str] = None,
        config_kwargs: Optional[Union[str, Dict[str, Any]]] = None,
        log_level: Optional[str] = None,
        debug_enabled: Optional[bool] = None,
        queue_db: Optional[int] = None,
        **kwargs,
    ):
        """
        Configures the settings
        """
        if url is not None: self.url = url
        if host is not None: self.host = host
        if port is not None: self.port = port
        if db is not None: self.db_id = db
        if username is not None: self.username = username
        if password is not None: self.password = password
        if ssl is not None: self.ssl = ssl
        if cache_ttl is not None: self.cache_ttl = cache_ttl
        if cache_prefix is not None: self.cache_prefix = cache_prefix
        if cache_enabled is not None: self.cache_enabled = cache_enabled
        if worker_enabled is not None: self.worker_enabled = worker_enabled
        if serializer is not None: self.serializer = serializer
        if db_mapping is not None: self.db_mapping = db_mapping
        if socket_timeout is not None: self.socket_timeout = socket_timeout
        if socket_connect_timeout is not None: self.socket_connect_timeout = socket_connect_timeout
        if socket_keepalive is not None: self.socket_keepalive = socket_keepalive
        if connection_timeout is not None: self.connection_timeout = connection_timeout
        if encoding is not None: self.encoding = encoding
        if encoding_errors is not None: self.encoding_errors = encoding_errors
        if config_kwargs is not None: self.config_kwargs = config_kwargs
        if debug_enabled is not None: self.debug_enabled = debug_enabled
        if log_level is not None: self.log_level = log_level

        if kwargs or queue_db is not None:
            self.worker.configure(db = queue_db, **kwargs)

        