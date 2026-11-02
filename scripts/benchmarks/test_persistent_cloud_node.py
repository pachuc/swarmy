import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location(
    "persistent_cloud_node", pathlib.Path(__file__).with_name("persistent-cloud-node.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class RemoteNodeTest(unittest.TestCase):
    def test_node_settings(self):
        env = {"SWARMY_NODE_API_URL": "http://127.0.0.1:8744",
               "SWARMY_NODE_TOKEN_FILE": "/private/token",
               "SWARMY_FDB_CLUSTER_FILE": "/private/fdb",
               "SWARMY_BENCH_REMOTE_IP": "example",
               "SWARMY_VALUE": "has 'quotes'"}
        self.assertEqual(module.node_api_port(env), 8744)
        forwards = module.ssh_forwards(env)
        self.assertEqual(forwards, ["-R", "127.0.0.1:4222:127.0.0.1:4222",
                                    "-R", "127.0.0.1:8744:127.0.0.1:8744"])
        self.assertNotIn("4500", " ".join(forwards))
        lines = module.remote_environment(env, "/opt/swarmy/node-token")
        self.assertIn("export SWARMY_NODE_TOKEN_FILE=/opt/swarmy/node-token\n", lines)
        self.assertIn("export SWARMY_VALUE='has '\"'\"'quotes'\"'\"''\n", lines)
        self.assertNotIn("SWARMY_FDB_CLUSTER_FILE", lines)
        self.assertNotIn("SWARMY_BENCH_", lines)
        self.assertNotIn("/private/token", lines)
