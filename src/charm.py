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
from typing import Optional, Union, Dict

from ops.charm import ActionEvent
from ops.main import main
from ops.model import ActiveStatus, StatusBase, BlockedStatus, RelationData
from pydantic import ValidationError

from charms.config.v0.classes import TypeSafeCharmBase, validate_params
from charms.config.v0.relations import get_relation_data_as
from charms.data_platform_libs.v0.data_interfaces import (
    KafkaRequires, BootstrapServerChangedEvent, TopicCreatedEvent
)
from charms.logging.v0.classes import WithLogging
from literals import KAFKA_CLUSTER, PEER
from models import CharmConfig, KafkaRelationDataBag, AppType, StartConsumerActionParam


class KafkaAppCharm(TypeSafeCharmBase[CharmConfig], WithLogging):
    """Charm the service."""

    config_type = CharmConfig

    def __init__(self, *args):
        super().__init__(*args)

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
            getattr(self.on, "start_producer_action"), self._start_handler(AppType.PRODUCER)
        )
        self.framework.observe(
            getattr(self.on, "start_consumer_action"), self._start_handler(AppType.CONSUMER)
        )
        self.framework.observe(
            getattr(self.on, "stop_producer_action"), self._stop_handler(AppType.PRODUCER)
        )
        self.framework.observe(
            getattr(self.on, "stop_consumer_action"), self._stop_handler(AppType.CONSUMER)
        )

    def _start_handler(self, process_type: AppType):
        @validate_params(StartConsumerActionParam)
        def _start(
                self: KafkaAppCharm,
                event: ActionEvent,
                params: Optional[Union[StartConsumerActionParam, ValidationError]] = None
        ):
            if process_type in self.pids.keys():
                event.fail(f"Cannot run more processes of type {process_type}")

            extra_data = params if isinstance(params, StartConsumerActionParam) else None
            process = subprocess.Popen(shlex.split(self._build_cmd(process_type, extra_data)))
            self.logger.info(f"Started process with pid: {process.pid}")
            self.set_pid(process_type, process.pid)
            event.set_results({"pid": process.pid})

        return _start

    def _stop_handler(self, process_type: AppType):
        def _stop(
            self: KafkaAppCharm,
            event: ActionEvent,
        ):
            if process_type in self.pids.keys():
                pid = self.pids[process_type]
                self.logger.info(f"Killing process with pid: {pid}")
                process = subprocess.Popen(["kill", "-9", str(pid)])
                self.set_pid(process_type, None)
                event.set_results({"pid": pid})
            else:
                event.fail(f"No process running for {process_type}")

        return _stop

    def _build_cmd(self, process_type: AppType, extra_data: Optional[StartConsumerActionParam] = None):
        if self.kafka_relation_data.tls:
            raise NotImplementedError("Cannot start process with TLS. Not yet implemented.")

        cmd = "nohup python charms.kafka.v0.client " + \
              f"--username {self.kafka_relation_data.username} " + \
              f"--password {self.kafka_relation_data.password} " + \
              f"--servers {self.kafka_relation_data.bootstrap_server} " + \
              f"--topic {self.kafka_relation_data.topic} "

        if process_type == AppType.CONSUMER:
            consumer_group = extra_data.consumer_group if extra_data else self.kafka_relation_data.consumer_group_prefix
            return f"{cmd} --consumer --consumer-group-prefix {consumer_group}"
        elif process_type == AppType.PRODUCER:
            return f"{cmd} --producer --replication-factor {self.config.replication_factor} --num-messages 10000"
        else:
            raise ValueError(f"process_type value {process_type} not recognised")

    @property
    def kafka_relation_data(self) -> Optional[KafkaRelationDataBag]:
        parsed = get_relation_data_as(relation.data[relation.app], KafkaRelationDataBag) \
            if (relation := self.model.get_relation(KAFKA_CLUSTER)) else None

        if isinstance(parsed, ValidationError):
            self.logger.error(f"There was a problem to read {KAFKA_CLUSTER} databag: {parsed}")

        return parsed if isinstance(parsed, KafkaRelationDataBag) else None

    @property
    def peer_relation(self) -> RelationData:
        relation = self.model.get_relation(PEER)
        if relation:
            return relation.data
        else:
            raise ValueError("This should never happen")

    @property
    def pids(self) -> Dict[str, int]:
        return {
            app_type: int(self.peer_relation[self.unit][app_type])
            for app_type in [AppType.CONSUMER, AppType.PRODUCER]
            if app_type in self.peer_relation[self.unit]
        }

    def set_pid(self, app_type: AppType, pid: Optional[int]):
        if pid:
            self.peer_relation[self.unit][app_type] = str(pid)
        else:
            _ = self.peer_relation[self.unit].pop(app_type)

    def get_status(self) -> StatusBase:
        if self.kafka_relation_data:
            if self.kafka_relation_data.topic == self.config.topic_name:
                return ActiveStatus(f"Topic {self.config.topic_name} enabled")
            else:
                return BlockedStatus(f"Please remove relation and recreate a new "
                                     f"one to track topic {self.config.topic_name}")
        else:
            return BlockedStatus("Relations with a Kafka cluster should be created")

    def _on_kafka_bootstrap_server_changed(self, event: BootstrapServerChangedEvent):
        # Event triggered when a bootstrap server was changed for this application
        self.logger.info(f"Bootstrap servers changed into: {event.bootstrap_server}")

    def _on_kafka_topic_created(self, event: TopicCreatedEvent):
        # Event triggered when a topic was created for this application
        self.logger.info(f"Topic successfully created: {self.config.topic_name} with username: {event.username}")
        self.unit.status = self.get_status()

    def _on_config_changed(self, _):
        self.logger.info(f"Configuration changed to {','.join(self.config.app_type)}")
        self.unit.status = self.get_status()


if __name__ == "__main__":
    main(KafkaAppCharm)
