try:
    # if we have crudrouter, the crudrouter will work
    import fastapi_crudrouter as _crudrouter  # noqa: F401
    from .crudrouter import PydanticAioredisCRUDRouter  # noqa: F401
except ImportError:  # pragma: no cover
    PydanticAioredisCRUDRouter = None

try:
    # if we have fastapi, fastapi model will work
    from fastapi import FastAPI as _  # noqa: F401
    from .model import FastAPIModel  # noqa: F401
except ImportError:  # pragma: no cover
    FastAPIModel = None