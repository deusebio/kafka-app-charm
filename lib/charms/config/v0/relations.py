import json
from functools import wraps
from typing import TypeVar, Type, Callable, Optional, Union, MutableMapping

import pydantic
from ops.charm import CharmBase, RelationEvent
from ops.model import RelationDataContent
from pydantic import BaseModel, ValidationError

S = TypeVar("S")
T = TypeVar("T", bound=BaseModel)
AppModel = TypeVar("AppModel", bound=BaseModel)
UnitModel = TypeVar("UnitModel", bound=BaseModel)


def write(relation_data: RelationDataContent, model: BaseModel):
    """Write the data contained in a domain object to the relation databag.

    Args:
        relation_data: pointer to the relation databag
        model: instance of pydantic model to be written
    """
    for key, value in model.dict(exclude_none=True).items():
        relation_data[key.replace("_", "-")] = str(value) if isinstance(value, str) or isinstance(value, int) \
            else json.dumps(value)


def read(relation_data: MutableMapping[str, str], obj: Type[T]) -> T:
    """Read data from a relation databag and parse it into a domain object.

    Args:
        relation_data: pointer to the relation databag
        obj: pydantic class represeting the model to be used for parsing
    """
    return obj(**{
        field_name: (
            relation_data[parsed_key] if field.type_ in [int, str, float] else json.dumps(relation_data[parsed_key])
        )
        for field_name, field in obj.__fields__.items()
        if (parsed_key := field_name.replace("_", "-")) in relation_data
        if relation_data[parsed_key]
    })


def parse_relation_data(app_model: Optional[Type[AppModel]] = None, unit_model: Optional[Type[UnitModel]] = None):
    """Return a decorator to allow pydantic parsing of the app and unit databags.

    Args:
        app_model: Pydantic class representing the model to be used for parsing the content of the app databag. None
            if no parsing ought to be done.
        unit_model: Pydantic class representing the model to be used for parsing the content of the unit databag. None
            if no parsing ought to be done.
    """

    def decorator(f: Callable[[
        CharmBase, RelationEvent, Union[AppModel, ValidationError], Union[UnitModel, ValidationError]
    ], S]) -> Callable[[CharmBase, RelationEvent], S]:
        @wraps(f)
        def event_wrapper(self: CharmBase, event: RelationEvent):

            try:
                app_data = read(event.relation.data[event.app], app_model) if app_model is not None else None
            except pydantic.ValidationError as e:
                app_data = e

            try:
                unit_data = read(event.relation.data[event.unit], unit_model) if unit_model is not None else None
            except pydantic.ValidationError as e:
                unit_data = e

            return f(self, event, app_data, unit_data)

        return event_wrapper

    return decorator


def get_relation_data_as(
        provider_data: RelationDataContent,
        requirer_data: RelationDataContent,
        model_type: Type[AppModel],
        logger
) -> Union[AppModel, ValidationError]:
    try:
        logger.info(dict(provider_data))
        logger.info(dict(requirer_data))
        app_data = read(dict(provider_data) | dict(requirer_data), model_type)
    except pydantic.ValidationError as e:
        app_data = e
    return app_data


class RelationDataModel(BaseModel):
    """Base class to be used for creating data models to be used for relation databags."""

    def write(self, relation_data: RelationDataContent):
        """Write data to a relation databag.

        Args:
            relation_data: pointer to the relation databag
        """
        return write(relation_data, self)

    @classmethod
    def read(cls, relation_data: RelationDataContent) -> 'RelationDataModel':
        """Read data from a relation databag and parse it as an instance of the pydantic class.

        Args:
            relation_data: pointer to the relation databag
        """
        return read(relation_data, cls)
