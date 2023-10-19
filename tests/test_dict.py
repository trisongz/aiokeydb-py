
import time
import asyncio
import uuid
from aiokeydb import KeyDBClient
from aiokeydb.types.cachify import CachifyKwargs
from aiokeydb.types.jobs import FunctionTracker, Job
from lazyops.types.models import BaseModel, Field
from typing import ClassVar

# The session can be explicitly initialized, or
# will be lazily initialized on first use
# through environment variables with all 
# params being prefixed with `KEYDB_`

keydb_uri = "keydb://localhost:6379/0"

# Initialize the Session
session = KeyDBClient.init_session(uri = keydb_uri)

class DummyObject(BaseModel):
    key1: float # = Field(default_factory=time.time)

data_dict = {
    "key1": "value1",
    "key2": 234,
    "key3": CachifyKwargs(),
    "key4": FunctionTracker(function = 'test'),
    "key5": Job(function = 'test'),
    # DummyObject(key1 = time.time()),
}


def test_dict():

    for key, value in data_dict.items():
        session[key] = value
    
    for key, value in data_dict.items():
        assert session[key] == value
        assert key in session
    
    for key, value in data_dict.items():
        del session[key]


async def test_async_dict():

    session.configure_dict_methods(async_enabled = True)

    for key, value in data_dict.items():
        session[key] = value
    
    for key, value in data_dict.items():
        stored_value = await session[key]
        assert stored_value == value
        assert key in session
        print(stored_value)
    
    for key, value in data_dict.items():
        del session[key]

async def run_tests():

    test_dict()
    await test_async_dict()

if __name__ == "__main__":
    asyncio.run(run_tests())


