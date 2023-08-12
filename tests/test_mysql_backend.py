import time
from unittest import TestCase
from .common import TemporaryMySQLBackend


class TestMySQLBackend(TestCase):
    def test_release_stalled(self):
        with TemporaryMySQLBackend() as bk:
            bk.batch_put(
                [(b"test_release_stalled", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            x = bk.batch_get(topic=1, size=1)[0]
            self.assertEqual(b"test_release_stalled", x.payload)
            self.assertEqual(0, bk.release_stalled_messages(topic=1))

            time.sleep(2)
            self.assertEqual(1, bk.release_stalled_messages(topic=1))

            y = bk.batch_get(topic=1, size=1)[0]
            self.assertEqual(b"test_release_stalled", y.payload)

    def test_ack(self):
        with TemporaryMySQLBackend() as bk:
            bk.batch_put(
                [(b"test_ack", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            x = bk.batch_get(topic=1, size=1)[0]
            self.assertEqual(b"test_ack", x.payload)
            x.ack()

            time.sleep(2)
            bk.release_stalled_messages(topic=1)

            self.assertEqual(0, len(bk.batch_get(topic=1, size=1)))

    def test_nack(self):
        with TemporaryMySQLBackend() as bk:
            bk.batch_put(
                [(b"test_ack", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            x = bk.batch_get(topic=1, size=1)[0]
            self.assertEqual(b"test_ack", x.payload)

            self.assertEqual(0, len(bk.batch_get(topic=1, size=1)))

            x.nack()

            z = bk.batch_get(topic=1, size=1)[0]
            self.assertIsNotNone(z)

    def test_context_manager(self):
        with TemporaryMySQLBackend() as bk:
            bk.batch_put(
                [(b"test_ack", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            with bk.batch_get(topic=1, size=1)[0] as task:
                self.assertIsNotNone(task)
                pass

            self.assertEqual(task.status, False)

            with bk.batch_get(topic=1, size=1)[0] as task:
                self.assertIsNotNone(task)
                task.ack()
                pass

            self.assertEqual(task.status, True)

            self.assertEqual(0, len(bk.batch_get(topic=1, size=1)))

    def test_priority(self):
        with TemporaryMySQLBackend() as bk:
            bk.batch_put(
                [(b"a", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            bk.batch_put(
                [(b"b", 1, None)],
                priority=1,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            msg = bk.batch_get(topic=1, size=1)[0]
            self.assertEqual(b"b", msg.payload)

    def test_batch_get_empty(self):
        with TemporaryMySQLBackend() as bk:
            msgs = bk.batch_get(topic=1, size=2)
            self.assertEqual(0, len(msgs))

    def test_batch_get_less_than_full(self):
        with TemporaryMySQLBackend() as bk:
            bk.batch_put(
                [(b"b", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            msgs = bk.batch_get(topic=1, size=2)
            self.assertEqual(1, len(msgs))
            self.assertEqual(0, bk.get_topic_size(topic=1))

    def test_batch_get_full(self):
        with TemporaryMySQLBackend() as bk:
            bk.batch_put(
                [(b"b", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            bk.batch_put(
                [(b"b", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            msgs = bk.batch_get(topic=1, size=2)
            self.assertEqual(2, len(msgs))
            self.assertEqual(0, bk.get_topic_size(topic=1))

    def test_batch_get_overfull(self):
        with TemporaryMySQLBackend() as bk:
            bk.batch_put(
                [(b"b", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            bk.batch_put(
                [(b"b", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            bk.batch_put(
                [(b"b", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            msgs = bk.batch_get(topic=1, size=2)
            self.assertEqual(2, len(msgs))
            self.assertEqual(1, bk.get_topic_size(topic=1))

    def test_batch_put(self):
        with TemporaryMySQLBackend() as bk:
            bk.batch_put(
                data=[(b"a", 1, None), (b"b", 1, None), (b"c", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            self.assertEqual(3, bk.get_topic_size(topic=1))
            msgs = bk.batch_get(topic=1, size=3)
            self.assertEqual(3, len(msgs))
            self.assertEqual(0, bk.get_topic_size(topic=1))
