# aiokeydb changelogs


## 0.0.12 (2022-12-08)

- Migration of `aiokeydb.client` -> `aiokeydb.core` and `aiokeydb.asyncio.client` -> `aiokeydb.asyncio.core`
- Unified API available through new `KeyDBClient` class that creates `sessions` which are `KeyDBSession` inherits from `KeyDB` and `AsyncKeyDBClient` class that inherits from `AsyncKeyDB` class
- Implemented `.cachify` method that allows for a caching decorator to be created for a function
