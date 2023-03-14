# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Class to handle configuration and relation databag."""
from enum import Enum
from typing import List, Optional

from charms.data_platform_libs.v0.data_models import RelationDataModel
from pydantic import BaseModel, ValidationError, validator


class AppType(str, Enum):
    """Class for the app type."""

    PRODUCER = "producer"
    CONSUMER = "consumer"


class CharmConfig(BaseModel):
    """Charmed configuration class."""

    topic_name: str
    role: str
    replication_factor: int
    consumer_group_prefix: Optional[str] = None
    partitions: int
    num_messages: int

    @classmethod
    @validator("role")
    def _role_parser(cls, value: str):
        """Handle the parsing of the role."""
        try:
            _app_type = AppType(value)
        except Exception as e:
            raise ValidationError(f"could not properly parsed the roles configuration: {e}")
        return AppType(value)

    @property
    def app_type(self) -> AppType:
        """Return the Kafka app type (producer or consumer)."""
        return AppType(self.role)


class StartConsumerParam(BaseModel):
    """Class to handle consumer parameters."""

    consumer_group_prefix: Optional[str]


class KafkaRequirerRelationDataBag(BaseModel):
    """Class for Kafka relation data."""

    topic: str
    extra_user_roles: str

    @classmethod
    @validator("extra_user_roles")
    def _role_parser(cls, roles: str):
        """Roles parsers."""
        try:
            _app_type = [AppType(value) for value in roles.split(",")]
        except Exception as e:
            raise ValidationError(f"could not properly parsed the roles configuration: {e}")
        return roles

    @property
    def app_type(self) -> List[AppType]:
        """Return the app types."""
        return [AppType(value) for value in self.extra_user_roles.split(",")]


class AuthDataBag(BaseModel):
    """Class to handle authentication parameters."""

    endpoints: str
    username: str
    password: str
    tls: Optional[str] = None
    tls_ca: Optional[str] = None


class KafkaProviderRelationDataBag(AuthDataBag):
    """Class for the provider relation databag."""

    consumer_group_prefix: Optional[str]

    @property
    def security_protocol(self):
        """Return the security protocol."""
        return "SASL_PLAINTEXT" if self.tls is not None else "SASL_SSL"

    @property
    def bootstrap_server(self):
        """Return the bootstrap servers endpoints."""
        return self.endpoints


class MongoProviderRelationDataBag(AuthDataBag):
    """Class that handle the MongoDB relation databag."""

    read_only_endpoints: Optional[str]
    replset: Optional[str]
    uris: Optional[str]
    version: Optional[str]


class PeerRelationAppData(RelationDataModel):
    """Class to handle the peer relation databag."""

    topic_name: Optional[str] = None
    database_name: Optional[str] = None
    private_key: Optional[str] = None


class PeerRelationUnitData(RelationDataModel):
    """Class to handle the unit peer relation databag."""

    pid: Optional[int]
