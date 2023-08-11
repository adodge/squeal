import time
from unittest import TestCase
from squeal import QueueEmpty
from .common import TemporaryMySQLBackend


class TestMySQLBackend(TestCase):
    def test_release_stalled(self):
        with TemporaryMySQLBackend() as bk:
            bk.put(
                b"test_release_stalled",
                topic=1,
                hsh=None,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            x = bk.get(topic=1)
            self.assertEqual(b"test_release_stalled", x.payload)
            self.assertEqual(0, bk.release_stalled_messages(topic=1))

            time.sleep(2)
            self.assertEqual(1, bk.release_stalled_messages(topic=1))

            y = bk.get(topic=1)
            self.assertEqual(b"test_release_stalled", y.payload)

    def test_ack(self):
        with TemporaryMySQLBackend() as bk:
            bk.put(
                b"test_ack",
                topic=1,
                hsh=None,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            x = bk.get(topic=1)
            self.assertEqual(b"test_ack", x.payload)
            x.ack()

            time.sleep(2)
            bk.release_stalled_messages(topic=1)

            with self.assertRaises(QueueEmpty):
                bk.get(topic=1)

    def test_nack(self):
        with TemporaryMySQLBackend() as bk:
            bk.put(
                b"test_ack",
                topic=1,
                hsh=None,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            x = bk.get(topic=1)
            self.assertEqual(b"test_ack", x.payload)

            with self.assertRaises(QueueEmpty):
                bk.get(topic=1)

            x.nack()

            z = bk.get(topic=1)
            self.assertIsNotNone(z)

    def test_context_manager(self):
        with TemporaryMySQLBackend() as bk:
            bk.put(
                b"test_ack",
                topic=1,
                hsh=None,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            with bk.get(topic=1) as task:
                self.assertIsNotNone(task)
                pass

            self.assertEqual(task.status, False)

            with bk.get(topic=1) as task:
                self.assertIsNotNone(task)
                task.ack()
                pass

            self.assertEqual(task.status, True)

            with self.assertRaises(QueueEmpty):
                with bk.get(topic=1):
                    pass

    def test_priority(self):
        with TemporaryMySQLBackend() as bk:
            bk.put(
                b"a",
                topic=1,
                hsh=None,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            bk.put(
                b"b",
                topic=1,
                hsh=None,
                priority=1,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            msg = bk.get(topic=1)
            self.assertEqual(b"b", msg.payload)

    def test_batch_get_empty(self):
        with TemporaryMySQLBackend() as bk:
            msgs = bk.batch_get(topic=1, size=2)
            self.assertEqual(0, len(msgs))

    def test_batch_get_less_than_full(self):
        with TemporaryMySQLBackend() as bk:
            bk.put(
                b"a",
                topic=1,
                hsh=None,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            msgs = bk.batch_get(topic=1, size=2)
            self.assertEqual(1, len(msgs))
            self.assertEqual(0, bk.size(topic=1))

    def test_batch_get_full(self):
        with TemporaryMySQLBackend() as bk:
            bk.put(
                b"a",
                topic=1,
                hsh=None,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            bk.put(
                b"b",
                topic=1,
                hsh=None,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            msgs = bk.batch_get(topic=1, size=2)
            self.assertEqual(2, len(msgs))
            self.assertEqual(0, bk.size(topic=1))

    def test_batch_get_overfull(self):
        with TemporaryMySQLBackend() as bk:
            bk.put(
                b"a",
                topic=1,
                hsh=None,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            bk.put(
                b"b",
                topic=1,
                hsh=None,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            bk.put(
                b"b",
                topic=1,
                hsh=None,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            msgs = bk.batch_get(topic=1, size=2)
            self.assertEqual(2, len(msgs))
            self.assertEqual(1, bk.size(topic=1))
