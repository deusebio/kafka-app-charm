from enum import Enum
from typing import Dict, List, Optional

from charms.data_platform_libs.v0.data_models import RelationDataModel
from charms.logging.v0.classes import WithLogging
from pydantic import BaseModel, ValidationError, validator


class AppType(str, Enum):
    PRODUCER = "producer"
    CONSUMER = "consumer"


class CharmConfig(BaseModel, WithLogging):
    topic_name: str
    roles: str
    replication_factor: int
    consumer_group_prefix: Optional[str] = None
    partitions: int

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


class StartConsumerParam(BaseModel):
    consumer_group_prefix: Optional[str]


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


class AuthDataBag(BaseModel):
    endpoints: str
    username: str
    password: str
    tls: Optional[str] = None
    tls_ca: Optional[str] = None


class KafkaProviderRelationDataBag(AuthDataBag):
    consumer_group_prefix: Optional[str]

    @property
    def security_protocol(self):
        return "SASL_PLAINTEXT" if self.tls is not None else "SASL_SSL"

    @property
    def bootstrap_server(self):
        return self.endpoints


class MongoProviderRelationDataBag(AuthDataBag):
    read_only_endpoints: Optional[str]
    replset: Optional[str]
    uris: Optional[str]
    version: Optional[str]


class PeerRelationAppData(RelationDataModel):
    topic_name: Optional[str] = None
    database_name: Optional[str] = None


class PeerRelationUnitData(RelationDataModel):
    pids: Dict[AppType, int]

    def add_pid(self, app_type: AppType, pid: int):
        pids = self.pids | {app_type: pid}
        return type(self)(**self.copy(update={"pids": pids}).dict())

    def remove_pid(self, app_type: AppType, pid: int):
        if self.pids[app_type] != pid:
            raise ValueError(f"pid {pid} not associated to process {app_type.value}")
        pids = {
            key: value
            for key, value in self.pids.items()
            if key != app_type
        }
        return type(self)(**self.copy(update={"pids": pids}).dict())
