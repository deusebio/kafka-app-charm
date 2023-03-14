#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
from pathlib import Path
from subprocess import PIPE, check_output
from typing import Any, Dict, Optional

import yaml
from juju.unit import Unit

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


async def fetch_action_get_credentials(unit: Unit) -> Dict:
    """Helper to run an action to fetch connection info.

    Args:
        unit: The juju unit on which to run the get_credentials action for credentials
    Returns:
        A dictionary with the username, password and access info for the service.
    """
    action = await unit.run_action(action_name="get-credentials")
    result = await action.wait()
    return result.results


async def fetch_action_start_process(
    unit: Unit,
    action_name: str,
    servers: str,
    username: str,
    password: str,
    topic_name: str,
    consumer_group_prefix: Optional[str] = None,
) -> Dict:
    """Helper to run an action to test Kafka.

    Args:
        unit: The juju unit on which to run the action
        action_name: name of the action
        servers: the kafka endpoints
        username: the kafka username
        password: the kafka password
        topic_name: the topic name
        consumer_group_prefix: the consumer group prefix parameter
    Returns:
        The result of the action.
    """
    parameters = {
        "servers": servers,
        "username": username,
        "password": password,
        "topic_name": topic_name,
    }
    if consumer_group_prefix:
        parameters["consumer_group_prefix"] = consumer_group_prefix
    action = await unit.run_action(action_name=action_name, **parameters)
    result = await action.wait()
    return result.results


async def fetch_action_stop_process(
    unit: Unit,
    action_name: str,
) -> Dict:
    """Helper to run an action to test Kafka app.

    Args:
        unit: The juju unit on which to run the action
        action_name: name of the action
    Returns:
        The result of the action.
    """
    action = await unit.run_action(action_name=action_name)
    result = await action.wait()
    return result.results


async def check_logs(model_full_name: str, unit_name: str):
    """Check logs for producer and consumer."""
    files = check_output(
        f"JUJU_MODEL={model_full_name} juju ssh {unit_name} 'find /tmp/' ",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    ).splitlines()

    logger.debug(f"{files=}")

    passed = False
    for file_name in files:
        if ".log" in file_name and ("consumer" in file_name or "producer" in file_name):
            passed = True
            break

    assert passed, "logs not found"
