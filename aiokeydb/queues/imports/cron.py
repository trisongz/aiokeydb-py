
"""
Import Handler for croniter
"""

from lazyops.utils import resolve_missing, require_missing_wrapper


try:
    from croniter import croniter
    _croniter_available = True
except ImportError:
    croniter = object
    _croniter_available = False


def resolve_croniter(
    required: bool = False,
):
    """
    Ensures that `croniter` is available
    """
    global croniter, _croniter_available
    if not _croniter_available:
        resolve_missing('croniter', required = required)
        from croniter import croniter
        _croniter_available = True


def require_croniter(
    required: bool = False,
):
    """
    Wrapper for `resolve_croniter` that can be used as a decorator
    """
    def decorator(func):
        return require_missing_wrapper(
            resolver = resolve_croniter, 
            func = func, 
            required = required
        )
    return decorator