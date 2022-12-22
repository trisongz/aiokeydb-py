# aiokeydb changelogs

## 0.1.4 (2022-12-22)
  - hotfix for locks.

## 0.1.3 (2022-12-21)
  - add `_lazy_init` param to `KeyDBClient.cachify` to postpone session initialization if the session has not already been configured
  - add `_cache_invalidator` param to `KeyDBClient.cachify` to allow for custom cache invalidation logic. If the result is True, the key will be deleted first prior to fetching from the cache.
  - add `debug_enabled` param to `KeyDBSettings` to enable debug logging for the `KeyDBClient` session.


## 0.1.2 (2022-12-20)
- add `overwrite` option for `KeyDBClient.configure` to overwrite the default session.

## 0.0.12 (2022-12-08)

- Migration of `aiokeydb.client` -> `aiokeydb.core` and `aiokeydb.asyncio.client` -> `aiokeydb.asyncio.core`
- Unified API available through new `KeyDBClient` class that creates `sessions` which are `KeyDBSession` inherits from `KeyDB` and `AsyncKeyDBClient` class that inherits from `AsyncKeyDB` class
- Implemented `.cachify` method that allows for a caching decorator to be created for a function
