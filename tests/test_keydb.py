import asyncio
import aiokeydb


keydb_uri = "keydb://localhost:6379/0"

def sync_test():
    keydb = aiokeydb.KeyDB.from_url(keydb_uri)
    keydb.set("foo", "bar")
    print(keydb.get("foo"))

async def async_test():
    keydb = aiokeydb.AsyncKeyDB.from_url(keydb_uri)
    await keydb.set("foo", "bar")
    print(await keydb.get("foo"))

async def run_tests():
    await async_test()
    sync_test()

asyncio.run(run_tests())