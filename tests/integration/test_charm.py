#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
import asyncio
import logging
import time

import pytest
from pytest_operator.plugin import OpsTest
from pymongo import MongoClient

from helpers import get_kafka_app_database_relation_data

KAFKA = "kafka"
ZOOKEEPER = "zookeeper"
MONGODB = "mongodb"
CONSUMER = "kafka-consumer"
PRODUCER = "kafka-producer"
TLS_NAME = "tls-certificates-operator"


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
    consumer_config = {"role":"consumer", "num_messages": 20}
    producer_config = {"role":"producer", "num_messages": 20}

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

    await ops_test.model.wait_for_idle(apps=[CONSUMER, PRODUCER], timeout=1000, status="active")
    assert ops_test.model.applications[KAFKA].status == "active"
    assert ops_test.model.applications[ZOOKEEPER].status == "active"
    assert ops_test.model.applications[PRODUCER].status == "active"
    assert ops_test.model.applications[CONSUMER].status == "active"
    # configure topic and extra-user-roles
    # config = {"topic-name": TOPIC_NAME, "extra-user-roles": EXTRA_USER_ROLES}
    # await ops_test.model.applications[DATA_INTEGRATOR].set_config(config)

    # add producer before consumer.

    # stop producer after a certain time
    # stop consumer

    # test with mongodb.

@pytest.mark.abort_on_fail
async def test_producer_and_consumer_charms(ops_test: OpsTest, kafka_app_charm):
    """Add relation and start consumer and producers."""
    await ops_test.model.add_relation(KAFKA, PRODUCER)
    await ops_test.model.wait_for_idle(apps=[KAFKA, PRODUCER])

    await ops_test.model.add_relation(KAFKA, CONSUMER)
    await ops_test.model.wait_for_idle(apps=[KAFKA, CONSUMER])

    time.sleep(60)

    await ops_test.model.applications[KAFKA].remove_relation(
        f"{PRODUCER}:kafka-cluster", f"{KAFKA}:kafka-client"
    )
    await ops_test.model.wait_for_idle(apps=[KAFKA, PRODUCER])
    await ops_test.model.applications[KAFKA].remove_relation(
        f"{CONSUMER}:kafka-cluster", f"{KAFKA}:kafka-client"
    )
    await ops_test.model.wait_for_idle(apps=[KAFKA, CONSUMER])


@pytest.mark.abort_on_fail
async def test_deploy_mongodb_and_relate(ops_test: OpsTest, kafka_app_charm):
    """Deploy mongoDB, relate it with the kafka-app and dump messages."""

    await asyncio.gather(
        ops_test.model.deploy(
            MONGODB,
            channel="dpe/edge",
            application_name=MONGODB,
            num_units=1,
            series="focal",
        ),
    )
    await ops_test.model.wait_for_idle(apps=[MONGODB], timeout=1000, status="active")
    await ops_test.model.add_relation(MONGODB, PRODUCER)
    await ops_test.model.wait_for_idle(apps=[MONGODB, PRODUCER])
    await ops_test.model.add_relation(MONGODB, CONSUMER)
    await ops_test.model.wait_for_idle(apps=[MONGODB, CONSUMER])

    # write messages to MongoDB
    await ops_test.model.add_relation(KAFKA, PRODUCER)
    await ops_test.model.wait_for_idle(apps=[KAFKA, PRODUCER])

    await ops_test.model.add_relation(KAFKA, CONSUMER)
    await ops_test.model.wait_for_idle(apps=[KAFKA, CONSUMER])

    time.sleep(60)

    # read messages to MongoDB

    mongodb_data = get_kafka_app_database_relation_data(unit_name=f"{PRODUCER}/0", model_full_name=ops_test.model_full_name)
    uris = mongodb_data["uris"]
    topic_name = mongodb_data["database"]
    logger.info(f"MongoDB uris: {uris}")
    logger.info(f"Topic: {topic_name}")
    try:
        client = MongoClient(
            uris,
            directConnection=False,
            connect=False,
            serverSelectionTimeoutMS=1000,
            connectTimeoutMS=2000,
        )
        db = client[topic_name]
        consumer_collection = db["consumer"]
        producer_collection = db["producer"]

        
        logger.info(f"Number of messages from consumer: {consumer_collection.count_documents({})}")
        logger.info(f"Number of messages from producer: {producer_collection.count_documents({})}")
        logger.info("Sleep....... 10000")
        time.sleep(10000)
        assert consumer_collection.count_documents({}) > 0
        assert producer_collection.count_documents({}) > 0
        assert consumer_collection.count_documents({}) == producer_collection.count_documents({})

        client.close()
    except Exception as e:
        logger.error("Cannot connect to MongoDB collection.")
        raise e

    # remove relation between kafka cluster and producer and consumer.
    await ops_test.model.applications[KAFKA].remove_relation(
        f"{PRODUCER}:kafka-cluster", f"{KAFKA}:kafka-client"
    )
    await ops_test.model.wait_for_idle(apps=[KAFKA, PRODUCER])
    await ops_test.model.applications[KAFKA].remove_relation(
        f"{CONSUMER}:kafka-cluster", f"{KAFKA}:kafka-client"
    )
    await ops_test.model.wait_for_idle(apps=[KAFKA, CONSUMER])

    # drop relation with MongoDB

    await ops_test.model.applications[MONGODB].remove_relation(
        f"{PRODUCER}:database", f"{MONGODB}:database"
    )
    await ops_test.model.wait_for_idle(apps=[MONGODB, PRODUCER])
    await ops_test.model.applications[MONGODB].remove_relation(
        f"{CONSUMER}:database", f"{MONGODB}:database"
    )
    await ops_test.model.wait_for_idle(apps=[MONGODB, CONSUMER])


@pytest.mark.abort_on_fail
async def test_tls(ops_test: OpsTest, kafka_app_charm):
    tls_config = {"generate-self-signed-certificates": "true", "ca-common-name": "kafka"}

    await asyncio.gather(
        ops_test.model.deploy(TLS_NAME, channel="beta", config=tls_config, series="jammy"),
    )

    await ops_test.model.wait_for_idle(apps=[KAFKA, ZOOKEEPER, TLS_NAME], timeout=1800)
    assert ops_test.model.applications[TLS_NAME].status == "active"

    logger.info("Relate Zookeeper to TLS")
    await ops_test.model.add_relation(TLS_NAME, ZOOKEEPER)
    await ops_test.model.wait_for_idle(apps=[TLS_NAME, ZOOKEEPER], idle_period=40)

    assert ops_test.model.applications[TLS_NAME].status == "active"
    assert ops_test.model.applications[ZOOKEEPER].status == "active"

    logger.info("Relate Kafka to TLS")
    await ops_test.model.add_relation(TLS_NAME, KAFKA)
    await ops_test.model.wait_for_idle(apps=[TLS_NAME, KAFKA], idle_period=40)

    assert ops_test.model.applications[TLS_NAME].status == "active"
    assert ops_test.model.applications[KAFKA].status == "active"

    logger.info("Relate Producer to TLS")
    await ops_test.model.add_relation(TLS_NAME, PRODUCER)
    await ops_test.model.wait_for_idle(apps=[TLS_NAME, PRODUCER], idle_period=40)

    assert ops_test.model.applications[TLS_NAME].status == "active"
    assert ops_test.model.applications[PRODUCER].status == "active"
    
    logger.info("Relate Consumer to TLS")
    await ops_test.model.add_relation(TLS_NAME, CONSUMER)
    await ops_test.model.wait_for_idle(apps=[TLS_NAME, CONSUMER], idle_period=40)

    assert ops_test.model.applications[TLS_NAME].status == "active"
    assert ops_test.model.applications[CONSUMER].status == "active"


    # relate producer and consumer
    await ops_test.model.add_relation(KAFKA, PRODUCER)
    await ops_test.model.wait_for_idle(apps=[KAFKA, PRODUCER])

    await ops_test.model.add_relation(KAFKA, CONSUMER)
    await ops_test.model.wait_for_idle(apps=[KAFKA, CONSUMER])

    time.sleep(60)

    await ops_test.model.applications[KAFKA].remove_relation(
        f"{PRODUCER}:kafka-cluster", f"{KAFKA}:kafka-client"
    )
    await ops_test.model.wait_for_idle(apps=[KAFKA, PRODUCER])
    await ops_test.model.applications[KAFKA].remove_relation(
        f"{CONSUMER}:kafka-cluster", f"{KAFKA}:kafka-client"
    )
    await ops_test.model.wait_for_idle(apps=[KAFKA, CONSUMER])