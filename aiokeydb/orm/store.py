"""Module containing the store classes"""
from typing import Any
from typing import Dict
from typing import Optional, Type


from aiokeydb.client import KeyDB
from aiokeydb.utils import from_url
from aiokeydb.orm.abstract import _AbstractStore
from aiokeydb.orm.config import KeyDBConfig
from aiokeydb.orm.model import Model


class Registry(_AbstractStore):
    """
    A store that allows a declarative way of querying for data in keydb
    """

    models: Dict[str, Type[Model]] = {}

    def __init__(
        self,
        name: str,
        keydb_config: KeyDBConfig,
        keydb_store: Optional[KeyDB] = None,
        life_span_in_seconds: Optional[int] = None,
        **data: Any,
    ):
        super().__init__(
            name=name,
            keydb_config=keydb_config,
            keydb_store=keydb_store,
            life_span_in_seconds=life_span_in_seconds,
            **data,
        )
        self.keydb_store = from_url(
            self.keydb_config.keydb_url,
            encoding=self.keydb_config.encoding,
            decode_responses=True,
        )

    def register_model(self, model_class: type[Model]):
        """Registers the model to this store"""
        if not isinstance(model_class.get_primary_key_field(), str):
            raise NotImplementedError(
                f"{model_class.__name__} should have a _primary_key_field"
            )

        model_class._store = self
        self.models[model_class.__name__.lower()] = model_class

    def model(self, name: str) -> Model:
        """Gets a model by name: case insensitive"""
        return self.models[name.lower()]