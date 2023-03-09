#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging
import time

import pytest
from pytest_operator.plugin import OpsTest

KAFKA = "kafka"
ZOOKEEPER = "zookeeper"

CONSUMER = "kafka-consumer"
PRODUCER = "kafka-producer"


logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
async def test_deploy_charms(ops_test: OpsTest, kafka_app_charm):
    """Deploy both charms (application and database) to use in the tests."""
    # Deploy both charms (2 units for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    await asyncio.gather(
        ops_test.model.deploy(
            ZOOKEEPER,
            channel="edge",
            application_name=ZOOKEEPER,
            num_units=1,
            series="jammy" if ops_test.cloud_name == "localhost" else "focal",
        ),
        ops_test.model.deploy(
            KAFKA,
            channel="edge",
            application_name=KAFKA,
            num_units=1,
            series="jammy",
        ),
    )

    await ops_test.model.wait_for_idle(apps=[ZOOKEEPER], timeout=1000)
    await ops_test.model.wait_for_idle(apps=[KAFKA], timeout=1000, status="waiting")
    time.sleep(10)
    assert ops_test.model.applications[KAFKA].status == "waiting"
    assert ops_test.model.applications[ZOOKEEPER].status == "active"

    await ops_test.model.add_relation(KAFKA, ZOOKEEPER)
    await ops_test.model.wait_for_idle(apps=[KAFKA, ZOOKEEPER])
    assert ops_test.model.applications[KAFKA].status == "active"
    assert ops_test.model.applications[ZOOKEEPER].status == "active"

    # deploy one producer and one consumer
    # todo add configuration in place!
    consumer_config = {}
    producer_config = {}

    await asyncio.gather(
        ops_test.model.deploy(
            kafka_app_charm,
            application_name=CONSUMER,
            num_units=1,
            series="jammy",
            config=consumer_config,
        ),
        ops_test.model.deploy(
            kafka_app_charm,
            application_name=PRODUCER,
            num_units=1,
            series="jammy",
            config=producer_config,
        ),
    )

    await ops_test.model.wait_for_idle(apps=[CONSUMER, PRODUCER], timeout=1000)
    # configure topic and extra-user-roles
    # config = {"topic-name": TOPIC_NAME, "extra-user-roles": EXTRA_USER_ROLES}
    # await ops_test.model.applications[DATA_INTEGRATOR].set_config(config)

    # add producer before consumer.

    # stop producer after a certain time
    # stop consumer

    # test with mongodb.
