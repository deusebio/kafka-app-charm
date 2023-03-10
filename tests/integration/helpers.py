#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import logging

from pathlib import Path
from subprocess import PIPE, check_output
from typing import Any, Dict
import yaml

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
ZK_NAME = "zookeeper"
REL_NAME_ADMIN = "kafka-client-admin"

logger = logging.getLogger(__name__)

def show_unit(unit_name: str, model_full_name: str) -> Any:
    """Return the yaml of the show-unit command."""
    result = check_output(
        f"JUJU_MODEL={model_full_name} juju show-unit {unit_name}",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )

    return yaml.safe_load(result)

def get_kafka_app_database_relation_data(unit_name: str, model_full_name: str) -> Dict[str, str]:
    """Return the databag of the database."""
    result = show_unit(unit_name=unit_name, model_full_name=model_full_name)
    relations_info = result[unit_name]["relation-info"]

    database_relation_data = {}
    for info in relations_info:
        if info["endpoint"] == "database":
            database_relation_data["database"] = info["application-data"]["database"]
            database_relation_data["endpoints"] = info["application-data"]["endpoints"]
            database_relation_data["password"] = info["application-data"]["password"]
            database_relation_data["uris"] = info["application-data"]["uris"]
            database_relation_data["username"] = info["application-data"]["username"]
            database_relation_data["replset"] = info["application-data"]["replset"]
    return database_relation_data