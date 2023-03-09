#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Kafka App for testing the Kafka Charmed Operator."""


import os
import shlex
import subprocess
from time import time
from typing import Optional

from charms.data_platform_libs.v0.data_interfaces import (
    BootstrapServerChangedEvent,
    DatabaseCreatedEvent,
    DatabaseRequires,
    KafkaRequires,
    TopicCreatedEvent,
)
from charms.data_platform_libs.v0.data_models import TypedCharmBase, get_relation_data_as
from charms.logging.v0.classes import WithLogging
from charms.tls_certificates_interface.v1.tls_certificates import (
    CertificateAvailableEvent,
    TLSCertificatesRequiresV1,
    generate_csr,
    generate_private_key,
)
from literals import (
    CA_FILE_PATH,
    CERT_FILE_PATH,
    CERTIFICATES,
    DATABASE,
    KAFKA_CLUSTER,
    KEY_FILE_PATH,
    PEER,
)
from models import (
    AppType,
    CharmConfig,
    KafkaProviderRelationDataBag,
    MongoProviderRelationDataBag,
    PeerRelationAppData,
    PeerRelationUnitData,
)
from ops.charm import ActionEvent, RelationBrokenEvent
from ops.framework import EventBase
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, StatusBase
from pydantic import ValidationError


class PeerRelation(WithLogging):
    """Peer relation model."""

    def __init__(self, charm: "KafkaAppCharm", name: str = PEER):
        self.charm = charm
        self.name = name

    def set_pid(self, pid: int) -> int:
        """Set pid on unit databag."""
        if relation_data := self.charm.model.get_relation(self.name):
            self.unit_data.copy(update={"pid": pid}).write(relation_data.data[self.charm.unit])
        return pid

    def remove_pid(self, pid: int) -> int:
        """Remove pid from the unit databag."""
        if relation_data := self.charm.model.get_relation(self.name):
            self.unit_data.copy(update={"pid": ""}).write(relation_data.data[self.charm.unit])
        return pid

    def set_topic(self, topic_name: str) -> str:
        """Set topic on the relation databag."""
        if relation_data := self.charm.model.get_relation(self.name):
            self.app_data.copy(update={"topic_name": topic_name}).write(
                relation_data.data[self.charm.app]
            )
        return topic_name

    def set_database(self, database_name: str) -> str:
        """Set database name in the peer relation databag."""
        if relation_data := self.charm.model.get_relation(self.name):
            self.app_data.copy(update={"database_name": database_name}).write(
                relation_data.data[self.charm.app]
            )
        return database_name

    def set_private_key(self, private_key: str) -> str:
        """Set private key in the peer relationd databag."""
        if relation_data := self.charm.model.get_relation(self.name):
            self.app_data.copy(update={"private_key": private_key}).write(
                relation_data.data[self.charm.app]
            )
        return private_key

    @property
    def unit_data(self) -> PeerRelationUnitData:
        """Return the unit databag."""
        parsed = (
            get_relation_data_as(PeerRelationUnitData, relation.data[self.charm.unit])
            if (relation := self.charm.model.get_relation(self.name))
            else None
        )

        if isinstance(parsed, ValidationError):
            self.logger.error(f"There was a problem to read {self.name} databag: {parsed}")

        return parsed if isinstance(parsed, PeerRelationUnitData) else PeerRelationUnitData()

    @property
    def app_data(self) -> PeerRelationAppData:
        """Return the peer relation databag."""
        parsed = (
            get_relation_data_as(PeerRelationAppData, relation.data[self.charm.app])
            if (relation := self.charm.model.get_relation(self.name))
            else None
        )

        if isinstance(parsed, ValidationError):
            self.logger.error(f"There was a problem to read {self.name} databag: {parsed}")

        return (
            parsed
            if isinstance(parsed, PeerRelationAppData)
            else PeerRelationAppData(topic_name=self.charm.config.topic_name)
        )


class KafkaAppCharm(TypedCharmBase[CharmConfig], WithLogging):
    """Charm the service."""

    config_type = CharmConfig

    def __init__(self, *args):
        super().__init__(*args)

        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)

        self.peer_relation = PeerRelation(self, name=PEER)
        self.certificates = TLSCertificatesRequiresV1(self, CERTIFICATES)

        self.kafka_cluster = KafkaRequires(
            self,
            relation_name=KAFKA_CLUSTER,
            topic=self.config.topic_name,
            extra_user_roles=self.config.app_type,
        )
        self.framework.observe(
            self.kafka_cluster.on.bootstrap_server_changed, self._on_kafka_bootstrap_server_changed
        )
        self.framework.observe(self.kafka_cluster.on.topic_created, self._on_kafka_topic_created)
        self.framework.observe(self.on[KAFKA_CLUSTER].relation_broken, self._on_relation_broken)

        self.database = DatabaseRequires(
            self, relation_name=DATABASE, database_name=self.config.topic_name
        )
        self.framework.observe(self.database.on.database_created, self._on_database_created)
        self.framework.observe(
            self.on[DATABASE].relation_broken, self._on_database_relation_broken
        )

        self.framework.observe(self.on[CERTIFICATES].relation_joined, self._tls_relation_joined)
        self.framework.observe(
            self.certificates.on.certificate_available, self._on_certificate_available
        )

        # actions
        self.framework.observe(
            getattr(self.on, "start_process_action"), self._start_process_action
        )
        self.framework.observe(getattr(self.on, "stop_process_action"), self._stop_process_action)

    def _tls_relation_joined(self, event: EventBase) -> None:
        """Handle the tls relation joined event."""
        if not self.peer_relation.app_data:
            event.defer()
            return

        self.peer_relation.set_private_key(generate_private_key().decode("utf-8"))
        self._request_certificate()

    def _request_certificate(self):
        """Request the certificate."""
        if not self.peer_relation.app_data:
            return

        private_key = self.peer_relation.app_data.private_key
        if not private_key:
            return

        csr = generate_csr(
            private_key=private_key.encode("utf-8"),
            subject=os.uname()[1],
        ).strip()

        self.certificates.request_certificate_creation(certificate_signing_request=csr)

    def _on_certificate_available(self, event: CertificateAvailableEvent) -> None:
        """Handle the on certificate available event."""
        if not self.peer_relation.app_data:
            event.defer()
            return

        self.write_file(content=self.peer_relation.app_data.private_key, path=KEY_FILE_PATH)
        self.write_file(content=event.certificate, path=CERT_FILE_PATH)
        self.write_file(content=event.ca, path=CA_FILE_PATH)

    @staticmethod
    def write_file(content: str, path: str) -> None:
        """Write content in a file."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(content)

    def _start_process_action(self, event: ActionEvent) -> None:
        """Handle the start process action."""
        if self.peer_relation.unit_data.pid:
            event.fail(f"Process id {self.peer_relation.unit_data.pid} already running!")

        app_type = self.config.app_type
        username = event.params["username"]
        password = event.params["password"]
        servers = event.params["servers"]
        topic = event.params["topic_name"]
        consumer_group_prefix = event.params["consumer_group_prefix"]

        pid = self._start_process(
            process_type=app_type,
            username=username,
            password=password,
            servers=servers,
            topic=topic,
            consumer_group_prefix=consumer_group_prefix,
            tls="disabled",
        )
        event.set_results({"process": pid})

    def _stop_process_action(self, event: ActionEvent):
        """Handle the stop process action."""
        if not self.peer_relation.unit_data.pid:
            event.fail("No process to stop!")
            return

        pid = self.peer_relation.unit_data.pid
        r_pid = self._stop_process(pid)
        assert r_pid == pid
        event.set_results({"process": pid})

    def _start_process(
        self,
        process_type: AppType,
        username: str,
        password: str,
        servers: str,
        topic: str,
        consumer_group_prefix: Optional[str],
        tls: str,
        cafile_path: Optional[str] = None,
        certfile_path: Optional[str] = None,
        keyfile_path: Optional[str] = None,
    ) -> int:
        """Handle the start of the process for consumption or production of messagges."""
        t0 = int(time())
        my_cmd = f"{self._build_cmd(process_type, username, password, servers, topic, consumer_group_prefix, tls, cafile_path, certfile_path, keyfile_path)}"
        self.logger.info(my_cmd)
        process = subprocess.Popen(
            shlex.split(my_cmd),
            stdout=open(f"/tmp/{t0}_{process_type.value}.log", "w"),
            stderr=open(f"/tmp/{t0}_{process_type.value}.err", "w"),
        )
        self.peer_relation.set_pid(process.pid)
        return process.pid

    def _stop_process(self, pid: int):
        """Handle the stop of the producer/consumer process."""
        self.logger.info(f"Killing process with pid: {pid}")
        subprocess.Popen(["sudo", "kill", "-9", str(pid)])
        self.peer_relation.remove_pid(pid)
        return pid

    def _build_cmd(
        self,
        process_type: AppType,
        username: str,
        password: str,
        servers: str,
        topic: str,
        consumer_group_prefix: Optional[str],
        tls: str,
        cafile_path: Optional[str] = None,
        certfile_path: Optional[str] = None,
        keyfile_path: Optional[str] = None,
    ):
        """Handle the creation of the command to launch producer or consumer."""
        if self.kafka_relation_data.tls != "disabled":
            raise NotImplementedError("Cannot start process with TLS. Not yet implemented.")

        if tls == "enabled" and self.peer_relation.app_data.private_key:
            self.logger.info(f"TLS enabled -> bootstrap servers: {servers}")
            servers = servers.replace("9092", "9093")

        cmd = (
            "nohup python3 -m charms.kafka.v0.client "
            + f"--username {username} "
            + f"--password {password} "
            + f"--servers {servers} "
            + f"--topic {topic} "
        )

        if self.peer_relation.app_data.database_name and self.database_relation_data.uris:
            cmd += f" --mongo-uri {self.database_relation_data.uris} "

        if tls == "enabled" and self.peer_relation.app_data.private_key:
            assert cafile_path
            assert certfile_path
            assert keyfile_path
            cmd = f"{cmd} --cafile-path {cafile_path} --certfile-path {certfile_path} --keyfile-path {keyfile_path} --security-protocol SASL_SSL "

        if process_type == AppType.CONSUMER:
            return f"{cmd} --consumer --consumer-group-prefix {consumer_group_prefix}"
        elif process_type == AppType.PRODUCER:
            return (
                f"{cmd} --producer "
                + f"--replication-factor {self.config.replication_factor} "
                + f"--num-partitions {self.config.partitions} "
                + f"--num-messages {self.config.num_messages}"
            )
        else:
            raise ValueError(f"process_type value {process_type} not recognised")

    @property
    def kafka_relation_data(self) -> Optional[KafkaProviderRelationDataBag]:
        """Return kafka relation data."""
        parsed = (
            get_relation_data_as(
                KafkaProviderRelationDataBag,
                relation.data[relation.app],
            )
            if (relation := self.model.get_relation(KAFKA_CLUSTER))
            else None
        )

        if isinstance(parsed, ValidationError):
            self.logger.error(f"There was a problem to read {KAFKA_CLUSTER} databag: {parsed}")

        return parsed if isinstance(parsed, KafkaProviderRelationDataBag) else None

    @property
    def database_relation_data(self) -> Optional[MongoProviderRelationDataBag]:
        """Return MongoDB relation data."""
        parsed = (
            get_relation_data_as(
                MongoProviderRelationDataBag,
                relation.data[relation.app],
            )
            if (relation := self.model.get_relation(DATABASE))
            else None
        )

        if isinstance(parsed, ValidationError):
            self.logger.error(f"There was a problem to read {DATABASE} databag: {parsed}")

        return parsed if isinstance(parsed, MongoProviderRelationDataBag) else None

    def get_status(self) -> StatusBase:
        """Return the status of the application."""
        if self.peer_relation.app_data.topic_name:
            if self.peer_relation.app_data.topic_name == self.config.topic_name:
                return ActiveStatus(
                    f"Topic {self.config.topic_name} enabled "
                    f"with processes {', '.join(self.config.app_type)}"
                )
            else:
                return BlockedStatus(
                    f"Please remove relation and recreate a new "
                    f"one to track topic {self.config.topic_name}"
                )
        else:
            return ActiveStatus(
                "Please relate with Kafka or use the action to run the application without the relation."
            )

    def _on_database_created(self, event: DatabaseCreatedEvent):
        """Handle the database created event."""
        self.logger.info(
            f"Database successfully created: {self.config.topic_name} with username: {event.username}"
        )

        if self.unit.is_leader():
            self.peer_relation.set_database(self.config.topic_name)

        if not self.peer_relation.app_data.topic_name:
            event.defer()

    def _on_database_relation_broken(self, _: RelationBrokenEvent):
        """Handle database relation broken event."""
        if self.unit.is_leader():
            self.peer_relation.set_database("")

    def _on_kafka_bootstrap_server_changed(self, event: BootstrapServerChangedEvent):
        """Handle the bootstrap server changed."""
        # Event triggered when a bootstrap server was changed for this application
        self.logger.info(f"Bootstrap servers changed into: {event.bootstrap_server}")

    def _on_kafka_topic_created(self, event: TopicCreatedEvent):
        """Handle the topic created event."""
        # Event triggered when a topic was created for this application
        self.logger.info(
            f"Topic successfully created: {self.config.topic_name} with username: {event.username}"
        )

        if self.unit.is_leader():
            self.peer_relation.set_topic(self.config.topic_name)

        if not self.peer_relation.app_data.topic_name:
            event.defer()

        app_type = self.config.app_type

        username = self.kafka_relation_data.username
        password = self.kafka_relation_data.password
        servers = self.kafka_relation_data.bootstrap_server
        topic = self.peer_relation.app_data.topic_name
        consumer_group_prefix = self.config.consumer_group_prefix

        pid = self._start_process(
            process_type=app_type,
            username=username,
            password=password,
            servers=servers,
            topic=topic,
            consumer_group_prefix=consumer_group_prefix,
            tls=self.kafka_relation_data.tls,
            cafile_path=CA_FILE_PATH,
            certfile_path=CERT_FILE_PATH,
            keyfile_path=KEY_FILE_PATH,
        )
        self.logger.info(f"Auto-Started {app_type} process with pid: {pid}")

        self.unit.status = self.get_status()

    def _on_relation_broken(self, _: RelationBrokenEvent):
        """Handle the relation broken event."""
        pid = self.peer_relation.unit_data.pid
        assert pid
        self.logger.info(f"Stopping process: {pid}")
        self._stop_process(pid)
        if self.unit.is_leader():
            self.peer_relation.set_topic("")
        self.unit.status = self.get_status()

    def _on_config_changed(self, _):
        """Handle the on configuration changed hook."""
        self.logger.info(f"Configuration changed to {','.join(self.config.app_type)}")
        self.unit.status = self.get_status()

    def _on_install(self, _):
        """Handle install hook."""
        self.unit.status = self.get_status()


if __name__ == "__main__":
    main(KafkaAppCharm)
