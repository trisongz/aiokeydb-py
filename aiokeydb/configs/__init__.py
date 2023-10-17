from __future__ import absolute_import

from aiokeydb.configs.worker import KeyDBWorkerSettings
from aiokeydb.configs.core import KeyDBSettings


class ProxySettings:
    def __init__(self):
        self._settings = None

    def __getattr__(self, name):
        if self._settings is None:
            from aiokeydb.utils.lazy import get_keydb_settings
            self._settings = get_keydb_settings()
        return getattr(self._settings, name)

settings: KeyDBSettings = ProxySettings()