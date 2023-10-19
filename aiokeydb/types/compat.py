"""
Resolver for Pydantic v1/v2 imports with additional helpers
"""


"""
Resolver for Pydantic v1/v2 imports with additional helpers
"""


import typing
from lazyops.utils.imports import resolve_missing


# Handle v1/v2 of pydantic
try:
    from pydantic import validator as _validator
    from pydantic import model_validator as base_root_validator

    PYD_VERSION = 2

    def root_validator(*args, **kwargs):
        """
        v1 Compatible root validator
        """
        def decorator(func):
            _pre_kw = kwargs.pop('pre', None)
            kwargs['mode'] = 'before' if _pre_kw is True else kwargs.get('mode', 'wrap')
            return base_root_validator(*args, **kwargs)(func)

        return decorator

    def pre_root_validator(*args, **kwargs):
        def decorator(func):
            return base_root_validator(*args, mode='before', **kwargs)(func)
        return decorator
    
    def validator(*args, **kwargs):
        def decorator(func):
            return _validator(*args, **kwargs)(classmethod(func))
        return decorator

except ImportError:
    from pydantic import root_validator, validator

    PYD_VERSION = 1

    def pre_root_validator(*args, **kwargs):
        def decorator(func):
            return root_validator(*args, pre=True, **kwargs)(func)
        return decorator


try:
    from pydantic_settings import BaseSettings

except ImportError:
    if PYD_VERSION == 2:
        resolve_missing('pydantic-settings', required = True)
        from pydantic_settings import BaseSettings
    else:
        from pydantic import BaseSettings


from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo

def get_pyd_dict(model: typing.Union[BaseModel, BaseSettings], **kwargs) -> typing.Dict[str, typing.Any]:
    """
    Get a dict from a pydantic model
    """
    if kwargs: kwargs = {k:v for k,v in kwargs.items() if v is not None}
    return model.model_dump(**kwargs) if PYD_VERSION == 2 else model.dict(**kwargs)

def get_pyd_fields_dict(model: typing.Type[typing.Union[BaseModel, BaseSettings]]) -> typing.Dict[str, FieldInfo]:
    """
    Get a dict of fields from a pydantic model
    """
    return model.model_fields if PYD_VERSION == 2 else model.__fields__

def get_pyd_field_names(model: typing.Type[typing.Union[BaseModel, BaseSettings]]) -> typing.List[str]:
    """
    Get a list of field names from a pydantic model
    """
    return list(get_pyd_fields_dict(model).keys())

def get_pyd_fields(model: typing.Type[typing.Union[BaseModel, BaseSettings]]) -> typing.List[FieldInfo]:
    """
    Get a list of fields from a pydantic model
    """
    return list(get_pyd_fields_dict(model).values())

def pyd_parse_obj(model: typing.Type[typing.Union[BaseModel, BaseSettings]], obj: typing.Any, **kwargs) -> typing.Union[BaseModel, BaseSettings]:
    """
    Parse an object into a pydantic model
    """
    return model.model_validate(obj, **kwargs) if PYD_VERSION == 2 else model.parse_obj(obj)


def get_pyd_schema(model: typing.Type[typing.Union[BaseModel, BaseSettings]], **kwargs) -> typing.Dict[str, typing.Any]:
    """
    Get a pydantic schema
    """
    return model.schema(**kwargs) if PYD_VERSION == 2 else model.model_json_schema(**kwargs)
