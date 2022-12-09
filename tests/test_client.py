import asyncio
from aiokeydb import KeyDBClient

keydb_uri = "keydb://.."

KeyDBClient.init_session(
    uri = keydb_uri,
)

@KeyDBClient.cachify()
async def async_fibonacci(number: int):
    if number == 0: return 0
    elif number == 1: return 1
    return await async_fibonacci(number - 1) + await async_fibonacci(number - 2)


@KeyDBClient.cachify()
def fibonacci(number: int):
    if number == 0: return 0
    elif number == 1: return 1
    return fibonacci(number - 1) + fibonacci(number - 2)

async def run_tests():
    print(fibonacci(100))
    print(await async_fibonacci(100))

asyncio.run(run_tests())