#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Charm the service.

Refer to the following post for a quick-start guide that will help you
develop a new k8s charm using the Operator Framework:

    https://discourse.charmhub.io/t/4208
"""
import shlex
import subprocess
from time import time
from typing import Optional

from ops.charm import RelationBrokenEvent
from ops.main import main
from ops.model import ActiveStatus, StatusBase, BlockedStatus
from pydantic import ValidationError

from charms.data_platform_libs.v0.data_interfaces import (
    KafkaRequires, BootstrapServerChangedEvent, TopicCreatedEvent,
    DatabaseRequires, DatabaseCreatedEvent
)
from charms.data_platform_libs.v0.data_models import TypedCharmBase, get_relation_data_as
from charms.logging.v0.classes import WithLogging
from literals import KAFKA_CLUSTER, PEER, DATABASE
from models import CharmConfig, KafkaProviderRelationDataBag, AppType, StartConsumerParam, \
    PeerRelationUnitData, PeerRelationAppData, MongoProviderRelationDataBag


class PeerRelation(WithLogging):

    def __init__(self, charm: 'KafkaAppCharm', name: str = PEER):
        self.charm = charm
        self.name = name

    def set_pid(self, process_type: AppType, pid: int) -> int:
        if (
                relation_data := self.charm.model.get_relation(self.name)
        ):
            self.unit_data.add_pid(process_type, pid).write(
                relation_data.data[self.charm.unit]
            )
        return pid

    def remove_pid(self, process_type: AppType, pid: int) -> int:
        if (
                relation_data := self.charm.model.get_relation(self.name)
        ):
            self.unit_data.remove_pid(process_type, pid).write(
                relation_data.data[self.charm.unit]
            )
        return pid

    def set_topic(self, topic_name: str) -> str:
        if (
                relation_data := self.charm.model.get_relation(self.name)
        ):
            self.app_data.copy(update={"topic_name": topic_name}).write(
                relation_data.data[self.charm.app]
            )
        return topic_name

    def set_database(self, database_name: str) -> str:
        if (
                relation_data := self.charm.model.get_relation(self.name)
        ):
            self.app_data.copy(update={"database_name": database_name}).write(
                relation_data.data[self.charm.app]
            )
        return database_name

    @property
    def unit_data(self) -> PeerRelationUnitData:
        parsed = get_relation_data_as(
            PeerRelationUnitData, relation.data[self.charm.unit]
        ) if (relation := self.charm.model.get_relation(self.name)) else None

        if isinstance(parsed, ValidationError):
            self.logger.error(f"There was a problem to read {self.name} databag: {parsed}")

        return parsed if isinstance(parsed, PeerRelationUnitData) \
            else PeerRelationUnitData(pids={})

    @property
    def app_data(self) -> PeerRelationAppData:
        parsed = get_relation_data_as(
            PeerRelationAppData, relation.data[self.charm.app]
        ) if (relation := self.charm.model.get_relation(self.name)) else None

        if isinstance(parsed, ValidationError):
            self.logger.error(f"There was a problem to read {self.name} databag: {parsed}")

        return parsed if isinstance(parsed, PeerRelationAppData) \
            else PeerRelationAppData(topic_name=self.charm.config.topic_name)


class KafkaAppCharm(TypedCharmBase[CharmConfig], WithLogging):
    """Charm the service."""

    config_type = CharmConfig

    def __init__(self, *args):
        super().__init__(*args)

        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)

        self.peer_relation = PeerRelation(self, name=PEER)

        self.kafka_cluster = KafkaRequires(
            self, relation_name=KAFKA_CLUSTER, topic=self.config.topic_name,
            extra_user_roles=",".join(self.config.app_type)
        )
        self.framework.observe(
            self.kafka_cluster.on.bootstrap_server_changed, self._on_kafka_bootstrap_server_changed
        )
        self.framework.observe(
            self.kafka_cluster.on.topic_created, self._on_kafka_topic_created
        )
        self.framework.observe(
            self.on[KAFKA_CLUSTER].relation_broken, self._on_relation_broken
        )

        self.database = DatabaseRequires(
            self, relation_name=DATABASE, database_name=self.config.topic_name
        )
        self.framework.observe(self.database.on.database_created, self._on_database_created)
        self.framework.observe(
            self.on[DATABASE].relation_broken, self._on_database_relation_broken
        )

    def _start_process(self, process_type: AppType,
                       extra_data: Optional[StartConsumerParam]) -> int:
        t0 = int(time())
        my_cmd = f"{self._build_cmd(process_type, extra_data)}"
        self.logger.info(my_cmd)
        process = subprocess.Popen(
            shlex.split(my_cmd),
            stdout=open(f"/tmp/{t0}_{process_type.value}.log", "w"),
            stderr=open(f"/tmp/{t0}_{process_type.value}.err", "w")
        )
        self.peer_relation.set_pid(process_type, process.pid)
        return process.pid

    def _stop_process(self, process_type: AppType, pid: int):
        self.logger.info(f"Killing process with pid: {pid}")
        process = subprocess.Popen(["sudo", "kill", "-9", str(pid)])
        self.peer_relation.remove_pid(process_type, pid)
        return pid

    def _build_cmd(self, process_type: AppType,
                   extra_data: Optional[StartConsumerParam] = None):
        if self.kafka_relation_data.tls != "disabled":
            raise NotImplementedError("Cannot start process with TLS. Not yet implemented.")

        cmd = "nohup python3 -m charms.kafka.v0.client " + \
              f"--username {self.kafka_relation_data.username} " + \
              f"--password {self.kafka_relation_data.password} " + \
              f"--servers {self.kafka_relation_data.bootstrap_server} " + \
              f"--topic {self.peer_relation.app_data.topic_name} "

        if self.peer_relation.app_data.database_name and self.database_relation_data.uris:
            cmd += f" --mongo-uri {self.database_relation_data.uris} "

        if process_type == AppType.CONSUMER:
            consumer_group = extra_data.consumer_group_prefix \
                if extra_data and extra_data.consumer_group_prefix \
                else self.kafka_relation_data.consumer_group_prefix
            return f"{cmd} --consumer --consumer-group-prefix {consumer_group}"
        elif process_type == AppType.PRODUCER:
            return f"{cmd} --producer " + \
                f"--replication-factor {self.config.replication_factor} " + \
                f"--num-partitions {self.config.partitions} " + \
                f"--num-messages 10000"
        else:
            raise ValueError(f"process_type value {process_type} not recognised")

    @property
    def kafka_relation_data(self) -> Optional[KafkaProviderRelationDataBag]:
        parsed = get_relation_data_as(
            KafkaProviderRelationDataBag, relation.data[relation.app],
        ) if (relation := self.model.get_relation(KAFKA_CLUSTER)) else None

        if isinstance(parsed, ValidationError):
            self.logger.error(f"There was a problem to read {KAFKA_CLUSTER} databag: {parsed}")

        return parsed if isinstance(parsed, KafkaProviderRelationDataBag) else None

    @property
    def database_relation_data(self) -> Optional[MongoProviderRelationDataBag]:
        parsed = get_relation_data_as(
            MongoProviderRelationDataBag, relation.data[relation.app],
        ) if (relation := self.model.get_relation(DATABASE)) else None

        if isinstance(parsed, ValidationError):
            self.logger.error(f"There was a problem to read {DATABASE} databag: {parsed}")

        return parsed if isinstance(parsed, MongoProviderRelationDataBag) else None

    def get_status(self) -> StatusBase:
        if self.peer_relation.app_data.topic_name:
            if self.peer_relation.app_data.topic_name == self.config.topic_name:
                return ActiveStatus(f"Topic {self.config.topic_name} enabled "
                                    f"with processes {', '.join(self.config.app_type)}")
            else:
                return BlockedStatus(f"Please remove relation and recreate a new "
                                     f"one to track topic {self.config.topic_name}")
        else:
            return BlockedStatus("Relations with a Kafka cluster should be created")

    def _on_database_created(self, event: DatabaseCreatedEvent):
        self.logger.info(
            f"Database successfully created: {self.config.topic_name} with username: {event.username}")

        if self.unit.is_leader():
            self.peer_relation.set_database(self.config.topic_name)

        if not self.peer_relation.app_data.topic_name:
            event.defer()

    def _on_database_relation_broken(self, event: RelationBrokenEvent):
        if self.unit.is_leader():
            self.peer_relation.set_database("")

    def _on_kafka_bootstrap_server_changed(self, event: BootstrapServerChangedEvent):
        # Event triggered when a bootstrap server was changed for this application
        self.logger.info(f"Bootstrap servers changed into: {event.bootstrap_server}")

    def _on_kafka_topic_created(self, event: TopicCreatedEvent):
        # Event triggered when a topic was created for this application
        self.logger.info(
            f"Topic successfully created: {self.config.topic_name} with username: {event.username}")

        if self.unit.is_leader():
            self.peer_relation.set_topic(self.config.topic_name)

        if not self.peer_relation.app_data.topic_name:
            event.defer()

        for app_type in self.config.app_type:
            extra_data = StartConsumerParam(
                consumer_group_prefix=self.config.consumer_group_prefix
            )
            pid = self._start_process(app_type, extra_data)
            self.logger.info(f"Auto-Started {app_type} process with pid: {pid}")

        self.unit.status = self.get_status()

    def _on_relation_broken(self, _: RelationBrokenEvent):
        [
            self._stop_process(app_type, pid)
            for app_type, pid in self.peer_relation.unit_data.pids.items()
        ]
        if self.unit.is_leader():
            self.peer_relation.set_topic("")
        self.unit.status = self.get_status()

    def _on_config_changed(self, _):
        self.logger.info(f"Configuration changed to {','.join(self.config.app_type)}")
        self.unit.status = self.get_status()

    def _on_install(self, _):
        self.unit.status = self.get_status()


if __name__ == "__main__":
    main(KafkaAppCharm)
