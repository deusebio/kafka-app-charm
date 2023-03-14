import unittest

from ops.testing import Harness

from charm import KafkaAppCharm
from literals import PEER
from models import CharmConfig


class TestCharm(unittest.TestCase):
    harness = Harness(
        KafkaAppCharm,
        meta=open("metadata.yaml", "r").read(),
        config=open("config.yaml", "r").read(),
        actions=open("actions.yaml", "r").read(),
    )

    @classmethod
    def setUpClass(cls) -> None:
        cls.harness.set_model_name("testing")
        cls.harness.begin()

    def setUp(self) -> None:
        # Instantiate the Charmed Operator Framework test harness

        self.addCleanup(self.harness.cleanup)

        self.assertIsInstance(self.harness.charm, KafkaAppCharm)

    def test_config_parsing_ok(self):
        self.assertIsInstance(self.harness.charm.config, CharmConfig)

        self.assertIsInstance(self.harness.charm.config.app_type, str)
        self.assertEqual(self.harness.charm.config.app_type, "consumer")

    def test_peer_data_bag(self):
        _ = self.harness.add_relation(PEER, "kafka-app")

        self.harness.charm.peer_relation.set_pid(10)

        self.assertEqual(self.harness.charm.peer_relation.unit_data.pid, 10)
        self.harness.charm.peer_relation.remove_pid(10)
        self.assertEqual(self.harness.charm.peer_relation.unit_data.pid, None)

        self.harness.charm.peer_relation.set_topic("test-topic-1")
        self.assertEqual(self.harness.charm.peer_relation.app_data.topic_name, "test-topic-1")
