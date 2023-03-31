# aiokeydb changelogs

## 0.1.31 (2023-03-29)
  - Add new `add_fallback_function` method for Worker, which allows for a fallback function to be called when the worker fails to execute the task. (v2)


## 0.1.30 (2023-03-28)
  - Start of migration of library to maintain upstream compatibility
  with `redis-py`.
  - Usable in `aiokeydb.v2` namespace. Will complete full migration by v0.1.50
  - Attempts to maintain backwards compatibility with `aiokeydb` v0.1.19
  - All classes are now subclassed from `redis` and `redis.asyncio` rather than being explictly defined.
  - Worker tasks functions are now callable via `KeyDBClient` and `KeyDBWorkerSettings` 
  - Rework `ConnectionPool` and `AsyncConnectionPool` to handle resetting of the connection pool when the maximum number of connections are reached.


## 0.1.19 (2023-03-08)
  - Add utility to set ulimits when initializing the connection pool.

## 0.1.18 (2023-03-08)
  - Resolve ConnectionPools with reset capabilities
  - Refactor `KeyDBSession` to utilize the ConnectionPool initialized by `KeyDBClient`
  - Refactor `KeyDBClient` to initialize Sessions using shared connection pools for async and sync in order to avoid spawning a new connection pool per session.
  - Moved certain class vars to its own state for `KeyDBSesssion`
  - Reorder Worker Queue initialization to prevent overlapping event loops
  - Implement certain changes from `redis-py`
  - kept previous `KeyDBClient` that is accessible via `aiokeydb.client.core` vs `aiokeydb.client.meta`

## 0.1.7 (2023-02-01)
  - Resolve worker issues for startup ctx

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
