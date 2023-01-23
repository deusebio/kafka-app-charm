from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, validator, ValidationError

from charms.logging.v0.classes import WithLogging


class AppType(str, Enum):
    PRODUCER = "producer"
    CONSUMER = "consumer"


class CharmConfig(BaseModel, WithLogging):
    topic_name: str
    roles: str
    replication_factor: int

    @validator("roles")
    def _role_parser(cls, roles: str):
        try:
            # self.logger.info(roles)
            _app_type = [AppType(value) for value in roles.split(",")]
        except Exception as e:
            raise ValidationError(f"could not properly parsed the roles configuration: {e}")
        return roles

    @property
    def app_type(self) -> List[AppType]:
        return [AppType(value) for value in self.roles.split(",")]

    class Config:
        use_enum_values = True  # <--


class StartConsumerActionParam(BaseModel):
    consumer_group: Optional[str]


class KafkaRequirerRelationDataBag(BaseModel):
    topic: str
    extra_user_roles: str

    @validator("extra_user_roles")
    def _role_parser(cls, roles: str):
        try:
            # self.logger.info(roles)
            _app_type = [AppType(value) for value in roles.split(",")]
        except Exception as e:
            raise ValidationError(f"could not properly parsed the roles configuration: {e}")
        return roles

    @property
    def app_type(self) -> List[AppType]:
        return [AppType(value) for value in self.extra_user_roles.split(",")]


class KafkaProviderRelationDataBag(BaseModel):
    username: str
    password: str
    endpoints: str
    consumer_group_prefix: str
    tls: Optional[str] = None
    tls_ca: Optional[str] = None

    @property
    def security_protocol(self):
        return "SASL_PLAINTEXT" if self.tls is not None else "SASL_SSL"

    @property
    def bootstrap_server(self):
        return self.endpoints


class KafkaRelationDataBag(KafkaProviderRelationDataBag, KafkaRequirerRelationDataBag):
    pass
