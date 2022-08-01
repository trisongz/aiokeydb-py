from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aiokeydb.asyncio.client import AsyncPipeline, AsyncKeyDB
    #from redis.asyncio.client import Pipeline, Redis


def async_from_url(url, **kwargs):
    """
    Returns an active AsyncKeyDB client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """
    from aiokeydb.asyncio.client import AsyncKeyDB

    return AsyncKeyDB.from_url(url, **kwargs)


class async_pipeline:
    def __init__(self, keydb_obj: "AsyncKeyDB"):
        self.p: "AsyncPipeline" = keydb_obj.pipeline()

    async def __aenter__(self) -> "AsyncPipeline":
        return self.p

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.p.execute()
        del self.p
