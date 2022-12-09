import json
import socket
import contextlib
import logging
from typing import Optional, Dict, Any, Union, Type, Tuple

from aiokeydb.client.types import BaseSettings, validator, lazyproperty, KeyDBUri
from aiokeydb.client.serializers import SerializerType, BaseSerializer


_default_db_mapping = {
    'default': 0,
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
    ssl: Optional[bool] = False

    cache_ttl: Optional[int] = 60 * 60 * 24  # 1 day
    cache_prefix: Optional[str] = 'cache_'
    cache_enabled: Optional[bool] = True
    
    serializer: Optional[Union[str, SerializerType]] = SerializerType.default
    db_mapping: Optional[Union[str, Dict[str, int]]] = _default_db_mapping

    socket_timeout: Optional[float] = 60.0
    socket_connect_timeout: Optional[float] = 60.0
    socket_keepalive: Optional[bool] = True
    connection_timeout: Optional[int] = 300
    encoding: Optional[str] = 'utf-8'
    encoding_errors: Optional[str] = 'strict'
    config_kwargs: Optional[Union[str, Dict[str, Any]]] = {}
    
    log_level: Optional[str] = 'INFO'

    class Config:
        case_sensitive = False
        env_prefix = 'KEYDB_'
    
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

    @lazyproperty
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

    @lazyproperty
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
    
    @lazyproperty
    def host_uri(self):
        return f'{self.host}:{self.port}' if self.host else self.base_uri.split('://', 1)[-1]
    
    @lazyproperty
    def hostname_uri(self):
        """
        Returns ``{scheme}://{host}``
        """
        return f'{self.protocol}://{self.host}'

    @lazyproperty
    def auth_str(self):
        """
        returns the auth string in the format of 
        ``username:password`` or ``:password``

        """
        if self.username and self.password:
            return f'{self.username}:{self.password}'
        return f':{self.password}' if self.password else None
    
    @lazyproperty
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
    
    @lazyproperty
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
            db_id = int(split[1])

        else:
            if not db_id and name: db_id = self.db_mapping.get(name)
            db_id = db_id or self.db_id
        
        return KeyDBUri(dsn = uri)
        

    def get_serializer(self) -> Type[BaseSerializer]:
        if isinstance(self.serializer, str):
            self.serializer = SerializerType[self.serializer]
        return self.serializer.get_serializer()

    @lazyproperty
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
    
