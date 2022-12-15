import os
import pathlib
import typing
import threading
import functools

from pydantic import validator, Field
from pydantic import BaseSettings as _BaseSettings
from pydantic import BaseModel as _BaseModel
from pydantic.networks import AnyUrl, Parts

__all__ = [
    'BaseModel', 
    'BaseSettings', 
    'validator', 
    'classproperty', 
    'lazyproperty', 
    'Constant', 
    'ENOVAL', 
    'KeyDBDsn',
    'KeyDBUri',
]

_NotFound = object()

class Constant(tuple):
    "Pretty display of immutable constant."

    def __new__(cls, name):
        return tuple.__new__(cls, (name,))

    def __repr__(self):
        return f'{self[0]}'

ENOVAL = Constant('ENOVAL')

class classproperty(property):
    """
    Similar to `property`, but allows class-level properties.  That is,
    a property whose getter is like a `classmethod`.

    The wrapped method may explicitly use the `classmethod` decorator (which
    must become before this decorator), or the `classmethod` may be omitted
    (it is implicit through use of this decorator).

    .. note::

        classproperty only works for *read-only* properties.  It does not
        currently allow writeable/deletable properties, due to subtleties of how
        Python descriptors work.  In order to implement such properties on a class
        a metaclass for that class must be implemented.

    Parameters
    ----------
    fget : callable
        The function that computes the value of this property (in particular,
        the function when this is used as a decorator) a la `property`.

    doc : str, optional
        The docstring for the property--by default inherited from the getter
        function.

    lazy : bool, optional
        If True, caches the value returned by the first call to the getter
        function, so that it is only called once (used for lazy evaluation
        of an attribute).  This is analogous to `lazyproperty`.  The ``lazy``
        argument can also be used when `classproperty` is used as a decorator
        (see the third example below).  When used in the decorator syntax this
        *must* be passed in as a keyword argument.

    Examples
    --------

    ::

        >>> class Foo:
        ...     _bar_internal = 1
        ...     @classproperty
        ...     def bar(cls):
        ...         return cls._bar_internal + 1
        ...
        >>> Foo.bar
        2
        >>> foo_instance = Foo()
        >>> foo_instance.bar
        2
        >>> foo_instance._bar_internal = 2
        >>> foo_instance.bar  # Ignores instance attributes
        2

    As previously noted, a `classproperty` is limited to implementing
    read-only attributes::

        >>> class Foo:
        ...     _bar_internal = 1
        ...     @classproperty
        ...     def bar(cls):
        ...         return cls._bar_internal
        ...     @bar.setter
        ...     def bar(cls, value):
        ...         cls._bar_internal = value
        ...
        Traceback (most recent call last):
        ...
        NotImplementedError: classproperty can only be read-only; use a
        metaclass to implement modifiable class-level properties

    When the ``lazy`` option is used, the getter is only called once::

        >>> class Foo:
        ...     @classproperty(lazy=True)
        ...     def bar(cls):
        ...         print("Performing complicated calculation")
        ...         return 1
        ...
        >>> Foo.bar
        Performing complicated calculation
        1
        >>> Foo.bar
        1

    If a subclass inherits a lazy `classproperty` the property is still
    re-evaluated for the subclass::

        >>> class FooSub(Foo):
        ...     pass
        ...
        >>> FooSub.bar
        Performing complicated calculation
        1
        >>> FooSub.bar
        1
    """

    def __new__(cls, fget=None, doc=None, lazy=False):
        if fget is None:
            # Being used as a decorator--return a wrapper that implements
            # decorator syntax
            def wrapper(func):
                return cls(func, lazy=lazy)

            return wrapper

        return super().__new__(cls)

    def __init__(self, fget, doc=None, lazy=False):
        self._lazy = lazy
        if lazy:
            self._lock = threading.RLock()   # Protects _cache
            self._cache = {}
        fget = self._wrap_fget(fget)

        super().__init__(fget=fget, doc=doc)

        # There is a buglet in Python where self.__doc__ doesn't
        # get set properly on instances of property subclasses if
        # the doc argument was used rather than taking the docstring
        # from fget
        # Related Python issue: https://bugs.python.org/issue24766
        if doc is not None:
            self.__doc__ = doc

    def __get__(self, obj, objtype):
        if self._lazy:
            val = self._cache.get(objtype, _NotFound)
            if val is _NotFound:
                with self._lock:
                    # Check if another thread initialised before we locked.
                    val = self._cache.get(objtype, _NotFound)
                    if val is _NotFound:
                        val = self.fget.__wrapped__(objtype)
                        self._cache[objtype] = val
        else:
            # The base property.__get__ will just return self here;
            # instead we pass objtype through to the original wrapped
            # function (which takes the class as its sole argument)
            val = self.fget.__wrapped__(objtype)
        return val

    def getter(self, fget):
        return super().getter(self._wrap_fget(fget))


    def setter(self, fset):
        raise NotImplementedError(
            "classproperty can only be read-only; use a metaclass to "
            "implement modifiable class-level properties")


    def deleter(self, fdel):
        raise NotImplementedError(
            "classproperty can only be read-only; use a metaclass to "
            "implement modifiable class-level properties")


    @staticmethod
    def _wrap_fget(orig_fget):
        if isinstance(orig_fget, classmethod):
            orig_fget = orig_fget.__func__

        # Using stock functools.wraps instead of the fancier version
        # found later in this module, which is overkill for this purpose

        @functools.wraps(orig_fget)
        def fget(obj):
            return orig_fget(obj.__class__)

        return fget


class _CachedClassProperty(object):
    """Cached class property decorator.
    Transforms a class method into a property whose value is computed once
    and then cached as a normal attribute for the life of the class.  Example
    usage:
    >>> class MyClass(object):
    ...   @cached_classproperty
    ...   def value(cls):
    ...     print("Computing value")
    ...     return '<property of %s>' % cls.__name__
    >>> class MySubclass(MyClass):
    ...   pass
    >>> MyClass.value
    Computing value
    '<property of MyClass>'
    >>> MyClass.value  # uses cached value
    '<property of MyClass>'
    >>> MySubclass.value
    Computing value
    '<property of MySubclass>'
    This decorator is similar to `functools.cached_property`, but it adds a
    property to the class, not to individual instances.
    """

    def __init__(self, func):
        self._func = func
        self._cache = {}

    def __get__(self, obj, objtype):
        if objtype not in self._cache:
            self._cache[objtype] = self._func(objtype)
        return self._cache[objtype]

    def __set__(self, obj, value):
        raise AttributeError(f'property {self._func.__name__} is read-only')

    def __delete__(self, obj):
        raise AttributeError(f'property {self._func.__name__} is read-only')


def cached_classproperty(func) -> property:
    return _CachedClassProperty(func)


class lazyproperty(property):
    """
    Works similarly to property(), but computes the value only once.

    This essentially memorizes the value of the property by storing the result
    of its computation in the ``__dict__`` of the object instance.  This is
    useful for computing the value of some property that should otherwise be
    invariant.  For example::

        >>> class LazyTest:
        ...     @lazyproperty
        ...     def complicated_property(self):
        ...         print('Computing the value for complicated_property...')
        ...         return 42
        ...
        >>> lt = LazyTest()
        >>> lt.complicated_property
        Computing the value for complicated_property...
        42
        >>> lt.complicated_property
        42

    As the example shows, the second time ``complicated_property`` is accessed,
    the ``print`` statement is not executed.  Only the return value from the
    first access off ``complicated_property`` is returned.

    By default, a setter and deleter are used which simply overwrite and
    delete, respectively, the value stored in ``__dict__``. Any user-specified
    setter or deleter is executed before executing these default actions.
    The one exception is that the default setter is not run if the user setter
    already sets the new value in ``__dict__`` and returns that value and the
    returned value is not ``None``.

    """

    def __init__(self, fget, fset=None, fdel=None, doc=None):
        super().__init__(fget, fset, fdel, doc)
        self._key = self.fget.__name__
        self._lock = threading.RLock()

    def __get__(self, obj, owner=None):
        try:
            obj_dict = obj.__dict__
            val = obj_dict.get(self._key, _NotFound)
            if val is _NotFound:
                with self._lock:
                    # Check if another thread beat us to it.
                    val = obj_dict.get(self._key, _NotFound)
                    if val is _NotFound:
                        val = self.fget(obj)
                        obj_dict[self._key] = val
            return val
        except AttributeError:
            if obj is None:
                return self
            raise

    def __set__(self, obj, val):
        obj_dict = obj.__dict__
        if self.fset:
            ret = self.fset(obj, val)
            if ret is not None and obj_dict.get(self._key) is ret:
                # By returning the value set the setter signals that it
                # took over setting the value in obj.__dict__; this
                # mechanism allows it to override the input value
                return
        obj_dict[self._key] = val

    def __delete__(self, obj):
        if self.fdel:
            self.fdel(obj)
        obj.__dict__.pop(self._key, None)    # Delete if present


class BaseSettings(_BaseSettings):

    def update_config(self, **kwargs):
        """
        Update the config for the other settings
        """
        for k, v in kwargs.items():
            if not hasattr(self, k): continue
            if isinstance(getattr(self, k), pathlib.Path):
                setattr(self, k, pathlib.Path(v))
            elif isinstance(getattr(self, k), BaseSettings):
                val = getattr(self, k)
                if hasattr(val, 'update_config'):
                    val.update_config(**v)
                else:
                    val = val.__class__(**v)
                setattr(self, k, val)
            else: 
                setattr(self, k, v)

    def set_env(self):
        """
        Update the Env variables for the current session
        """
        data = self.dict(exclude_none=True)
        for k, v in data.items():
            if isinstance(v, BaseSettings):
                v.set_env()
            else:
                os.environ[self.Config.env_prefix + k.upper()] = str(v)


class BaseModel(_BaseModel):
    class Config:
        extra = 'allow'
        arbitrary_types_allowed = True

    def get(self, name, default: typing.Any = None):
        return getattr(self, name, default)
    
    def update(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k): continue
            setattr(self, k, v)


class KeyDBDsn(AnyUrl):
    __slots__ = ()
    allowed_schemes = {'redis', 'rediss', 'keydb', 'keydbs'}
    host_required = False

    @staticmethod
    def get_default_parts(parts: Parts) -> Parts:
        return {
            'domain': 'localhost' if not (parts['ipv4'] or parts['ipv6']) else '',
            'port': '6379',
            'path': '/0',
        }

class KeyDBUri(BaseModel):

    dsn: KeyDBDsn

    @lazyproperty
    def host(self):
        return self.dsn.host

    @lazyproperty
    def port(self):
        return self.dsn.port

    @lazyproperty
    def path(self):
        return self.dsn.path
    
    @lazyproperty
    def username(self):
        return self.dsn.user

    @lazyproperty
    def password(self):
        return self.dsn.password

    @lazyproperty
    def db_id(self):
        return int(self.dsn.path[1:]) if self.dsn.path else None

    @lazyproperty
    def ssl(self):
        return self.dsn.scheme in {'rediss', 'keydbs'}

    @lazyproperty
    def uri(self):
        return str(self.dsn)
    
    @lazyproperty
    def connection(self):
        return str(self.dsn)
    
    @lazyproperty
    def uri_no_auth(self):
        if self.has_auth:
            return str(self.dsn).replace(f'{self.auth_str}', '***')
        return str(self.dsn)
    
    @lazyproperty
    def auth_str(self):
        if self.dsn.user:
            return f'{self.dsn.user}:{self.password}' if self.password else f'{self.dsn.user}'
        return f':{self.dsn.password}' if self.dsn.password else ''

    @lazyproperty
    def has_auth(self):
        return self.dsn.user or self.dsn.password

    def __str__(self):
        return self.uri_no_auth

    def __repr__(self):
        return f'<KeyDBUri {self.uri_no_auth}>'


