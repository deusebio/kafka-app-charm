from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class AppType(str, Enum):
    PRODUCER = "producer"
    CONSUMER = "consumer"


class CharmConfig(BaseModel):
    topic_name: str
    app_type: List[AppType]
    replication_factor: int

    class Config:
        use_enum_values = True  # <--


class StartConsumerActionParam(BaseModel):
    consumer_group: Optional[str]


class KafkaRelationDataBag(BaseModel):
    topic: str
    user_extra_roles: List[AppType]
    username: str
    password: str
    bootstrap_server: str
    consumer_group_prefix: str
    tls: Optional[str] = None
    tls_ca: Optional[str] = None

    @property
    def security_protocol(self):
        return "SASL_PLAINTEXT" if self.tls is not None else "SASL_SSL"
