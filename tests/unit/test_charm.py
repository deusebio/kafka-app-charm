import unittest

from ops.testing import Harness

from charm import KafkaAppCharm
from literals import PEER
from models import AppType
from models import CharmConfig


class TestCharm(unittest.TestCase):
    harness = Harness(
        KafkaAppCharm, meta=open("metadata.yaml", "r").read(),
        config=open("config.yaml", "r").read(), actions=open("actions.yaml", "r").read()
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

        self.assertIsInstance(self.harness.charm.config.app_type, list)
        self.assertEqual(len(self.harness.charm.config.app_type), 2)

    def test_peer_data_bag(self):
        _ = self.harness.add_relation(PEER, "kafka-app")

        peer_relation_unit = self.harness.charm.model.get_relation(PEER).data[
            self.harness.charm.unit]

        self.harness.charm.peer_relation.unit_data.add_pid(AppType.CONSUMER, 10) \
            .write(peer_relation_unit)

        self.assertEqual(self.harness.charm.peer_relation.unit_data.pids[AppType.CONSUMER], 10)
