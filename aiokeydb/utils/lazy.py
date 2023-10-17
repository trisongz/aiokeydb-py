from __future__ import annotations

from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from aiokeydb.configs.core import KeyDBSettings

_keydb_settings: Optional['KeyDBSettings'] = None

def get_keydb_settings(**kwargs) -> KeyDBSettings:
    """
    Get the current KeyDB settings
    """
    global _keydb_settings
    if _keydb_settings is None:
        from aiokeydb.configs.core import KeyDBSettings
        _keydb_settings = KeyDBSettings(**kwargs)
    return _keydb_settings

def get_default_job_timeout() -> int:
    """
    Get the default job timeout
    """
    return get_keydb_settings().get_default_job_timeout()

def get_default_job_ttl() -> int:
    """
    Get the default job ttl
    """
    return get_keydb_settings().get_default_job_ttl()


def get_default_job_retries() -> int:
    """
    Get the default job retries
    """
    return get_keydb_settings().get_default_job_retries()


def get_default_job_retry_delay() -> int:
    """
    Get the default job retry delay
    """
    return get_keydb_settings().get_default_job_retry_delay()
