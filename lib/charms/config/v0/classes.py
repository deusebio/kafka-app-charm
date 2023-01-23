from functools import cached_property
from functools import wraps
from typing import TypeVar, Generic, Type, Callable, Union

from ops.charm import CharmBase, ActionEvent
from pydantic import BaseModel, ValidationError

TypedConfig = TypeVar("TypedConfig", bound=BaseModel)

T = TypeVar("T", bound=BaseModel)
S = TypeVar("S", bound=BaseModel)


class TypeSafeCharmBase(CharmBase, Generic[TypedConfig]):
    """Class to be used for extending config-typed charms."""

    config_type: Type[TypedConfig]

    @cached_property
    def config(self) -> TypedConfig:
        """Return a config instance, which is validated and parsed using the provided pydantic class."""
        translated_keys = {k.replace("-", "_"): v for k, v in self.model.config.items()}
        return self.config_type(**translated_keys)


def validate_params(cls: Type[T]):
    """Return a decorator to allow pydantic parsing of action parameters.

       Args:
           app_model: Pydantic class representing the model to be used for parsing the content of the action parameter
    """
    def decorator(f: Callable[[CharmBase, ActionEvent, Union[T, ValidationError]], S]) -> Callable[
        [CharmBase, ActionEvent], S]:
        @wraps(f)
        def event_wrapper(self: CharmBase, event: ActionEvent):
            try:
                params = cls(**{key.translate("-", "_"): value for key, value in event.params.items()})
            except ValidationError as e:
                params = e
            return f(self, event, params)

        return event_wrapper

    return decorator
