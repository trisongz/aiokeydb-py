
import time
import asyncio
import uuid
from aiokeydb import KeyDBClient

# The session can be explicitly initialized, or
# will be lazily initialized on first use
# through environment variables with all 
# params being prefixed with `KEYDB_`

keydb_uri = "keydb://localhost:6379/0"

# Initialize the Unified Client
KeyDBClient.init_session(
    uri = keydb_uri,
)

# Cache the results of these functions
# cachify works for both sync and async functions
# and has many params to customize the caching behavior
# and supports both `redis` and `keydb` backends
# as well as `api` frameworks such as `fastapi` and `starlette`

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

async def test_fib(n: int = 100, runs: int = 10):
    # Test that both results are the same.
    sync_t, async_t = 0.0, 0.0

    for i in range(runs):
        t = time.time()
        print(f'[Async - {i}/{runs}] Result: {await async_fibonacci(n)}')
        tt = time.time() - t
        print(f'[Async - {i}/{runs}] Time: {tt:.2f}s')
        async_t += tt

        t = time.time()
        print(f'[Sync - {i}/{runs}] Result: {fibonacci(n)}')
        tt = time.time() - t
        print(f'[Sync - {i}/{runs}] Time: {tt:.2f}s')
        sync_t += tt
    
    print(f'[Async] Cache Average Time: {async_t / runs:.2f}s | Total Time: {async_t:.2f}s')
    print(f'[Sync] Cache Average Time: {sync_t / runs:.2f}s | Total Time: {sync_t:.2f}s')
        
async def test_setget(runs: int = 10):
    # By default, the client utilizes `pickle` to serialize
    # and deserialize objects. This can be changed by setting
    # the `serializer`

    sync_t, async_t = 0.0, 0.0
    for i in range(runs):
        value = str(uuid.uuid4())
        key = f'async-test-{i}'
        t = time.time()
        await KeyDBClient.async_set(key, value)
        assert await KeyDBClient.async_get(key) == value
        tt = time.time() - t
        print(f'[Async - {i}/{runs}] Get/Set: {key} -> {value} = {tt:.2f}s')
        async_t += tt

        value = str(uuid.uuid4())
        key = f'sync-test-{i}'
        t = time.time()
        KeyDBClient.set(key, value)
        assert KeyDBClient.get(key) == value
        tt = time.time() - t
        print(f'[Sync - {i}/{runs}] Get/Set: {key} -> {value} = {tt:.2f}s')
        sync_t += tt
    
    print(f'[Async] GetSet Average Time: {async_t / runs:.2f}s | Total Time: {async_t:.2f}s')
    print(f'[Sync] GetSet Average Time: {sync_t / runs:.2f}s | Total Time: {sync_t:.2f}s')


async def run_tests(fib_n: int = 100, fib_runs: int = 10, setget_runs: int = 10):
    
    # You can explicitly wait for the client to be ready
    # Sync version
    # KeyDBClient.wait_for_ready()
    await KeyDBClient.async_wait_for_ready()

    # Run the tests
    await test_fib(n = fib_n, runs = fib_runs)
    await test_setget(runs = setget_runs)


    # Utilize the current session
    await KeyDBClient.async_set('async_test_0', 'test')
    assert await KeyDBClient.async_get('async_test_0') == 'test'

    KeyDBClient.set('sync_test_0', 'test')
    assert KeyDBClient.get('sync_test_0') == 'test'


    # you can access the `KeyDBSession` object directly
    # which mirrors the APIs in `KeyDBClient`

    await KeyDBClient.session.async_set('async_test_1', 'test')
    assert await KeyDBClient.session.async_get('async_test_1') == 'test'

    KeyDBClient.session.set('sync_test_1', 'test')
    assert KeyDBClient.session.get('sync_test_1') == 'test'

    # The underlying client can be accessed directly
    # if the desired api methods aren't mirrored

    # KeyDBClient.keydb
    # KeyDBClient.async_keydb
    # Since encoding / decoding is not handled by the client
    # you must encode / decode the data yourself
    await KeyDBClient.async_keydb.set('async_test_2', b'test')
    assert await KeyDBClient.async_keydb.get('async_test_2') == b'test'

    KeyDBClient.keydb.set('sync_test_2', b'test')
    assert KeyDBClient.keydb.get('sync_test_2') == b'test'

    # You can also explicitly close the client
    # However, this closes the connectionpool and will terminate
    # all connections. This is not recommended unless you are
    # explicitly closing the client.

    # Sync version
    # KeyDBClient.close()
    await KeyDBClient.aclose()

asyncio.run(run_tests())