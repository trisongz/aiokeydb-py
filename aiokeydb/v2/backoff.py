import typing

from redis.backoff import (
    AbstractBackoff,
    ConstantBackoff,
    ExponentialBackoff,
    FullJitterBackoff,
    NoBackoff,
    EqualJitterBackoff,
    DecorrelatedJitterBackoff,
)

def default_backoff() -> typing.Type[AbstractBackoff]:
    return EqualJitterBackoff()
