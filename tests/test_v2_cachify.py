import sys
import time
import asyncio
import uuid
import multiprocessing
from aiokeydb.v2.client import KeyDBClient
from aiokeydb.v2.queues import Worker, TaskQueue
from aiokeydb.v2.client import logger

@KeyDBClient.cachify(cache_ttl=10)
async def a_cached_func(number: int):
    return number + 1

@KeyDBClient.worker.add_fallback_function()
async def w_a_cached_func(ctx, number: int):

    @KeyDBClient.cachify(cache_ttl=10)
    async def inner(n: int):
        return n + 1

    return await inner(number)

@KeyDBClient.cachify(cache_ttl=10)
def cached_func(number: int):
    return number + 1

@KeyDBClient.worker.add_fallback_function()
def w_cached_func(ctx, number: int):

    @KeyDBClient.cachify(cache_ttl=10)
    def inner(n: int):
        return n + 1

    return inner(number)


async def start_worker():
    tq = TaskQueue("test_queue")
    KeyDBClient.worker.set_queue_func(tq)
    worker = Worker(tq)
    await worker.start()

async def test_cached_func(n: int = 5, runs: int = 10):
    # Test that both results are the same.
    sync_t, async_t = 0.0, 0.0

    for i in range(runs):
        t = time.time()
        print(f'[Async - {i}/{runs}] Result: {await a_cached_func(n)}')
        tt = time.time() - t
        print(f'[Async - {i}/{runs}] Time: {tt:.2f}s')
        async_t += tt
    print(f'[Async] Cache Average Time: {async_t / runs:.2f}s | Total Time: {async_t:.2f}s')

    for i in range(runs):
        t = time.time()
        print(f'[Sync - {i}/{runs}] Result: {cached_func(n)}')
        tt = time.time() - t
        print(f'[Sync - {i}/{runs}] Time: {tt:.2f}s')
        sync_t += tt
    print(f'[Sync] Cache Average Time: {sync_t / runs:.2f}s | Total Time: {sync_t:.2f}s')

async def test_worker_func(n: int = 5, runs: int = 10):
    # test inner caching
    sync_t, async_t = 0.0, 0.0
    
    for i in range(runs):
        t = time.time()
        print(f'[Async Inner - {i}/{runs}] Result: {await w_a_cached_func(number = n)}')
        tt = time.time() - t
        print(f'[Async Inner - {i}/{runs}] Time: {tt:.2f}s')
        async_t += tt
    print(f'[Async Inner] Cache Average Time: {async_t / runs:.2f}s | Total Time: {async_t:.2f}s')

    for i in range(runs):
        t = time.time()
        print(f'[Sync Inner - {i}/{runs}] Result: {await w_cached_func(number = n)}')
        tt = time.time() - t
        print(f'[Sync Inner - {i}/{runs}] Time: {tt:.2f}s')
        sync_t += tt
    print(f'[Sync Inner] Cache Average Time: {sync_t / runs:.2f}s | Total Time: {sync_t:.2f}s')
    # task.join()
    sys.exit(0)
    
    # await worker.stop()
    # await asyncio.gather(task)
    # task.cancel()

async def run_tests():
    await test_cached_func()
    # proc = 


asyncio.run(test_cached_func())

# async def run_tests(fib_n: int = 15, fib_runs: int = 10, setget_runs: int = 10):
