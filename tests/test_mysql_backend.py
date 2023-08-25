from unittest.mock import patch, MagicMock
from .common import TemporaryMySQLBackend


class TestMySQLBackend:
    @patch("squeal.MySQLBackend._gc")
    def test_garbage_collection_interval(self, __gc: MagicMock):
        with TemporaryMySQLBackend(garbage_collection_interval=100) as bk:
            for _ in range(100):
                bk._gc_increment(1)
        __gc.assert_called()

    def test_garbage_collection(self):
        with TemporaryMySQLBackend() as bk:
            bk._gc()
