# aiokeydb-py

  **Important**

  v0.0.8 Fully Migrates away from `aioredis` to `redis-py` @ `v4.3.4`.

  This inherits all API methods from [`redis-py`](https://github.com/redis/redis-py) to enforce compatability since deprecation of `aioredis` [going forward](https://github.com/aio-libs/aioredis-py/issues/1273)

  This fork of `redis-py` has some modified semantics, while attempting to replicate all the API methods of `redis-py` to avoid compatibility issues with underlying libraries that require a pinned `redis` version.

  Notably, all `async` Classes and methods are prefixed by `Async` to avoid name collisions with the `sync` version.


---

## Usage

```python

from aiokeydb import KeyDB, AsyncKeyDB, from_url

sync_client = KeyDB()
async_client = AsyncKeyDB()

# Alternative methods
sync_client = from_url('keydb://localhost:6379/0')
async_client = from_url('keydb://localhost:6379/0', asyncio = True)


```