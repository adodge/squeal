import time
from unittest import TestCase
from squeal import Queue, QueueEmpty
from .common import TemporaryMySQLBackend


class TestMySQLQueue(TestCase):
    def test_queue_topics_dont_interfere(self):
        with TemporaryMySQLBackend() as bk:
            q = Queue(bk, visibility_timeout=100)
            q.put(b"a", topic=1)

            with self.assertRaises(QueueEmpty):
                q.get_nowait(topic=2)

            q.put(b"b", topic=2)

            x = q.get_nowait(topic=2)
            self.assertIsNotNone(x)

    def test_queue_topics(self):
        with TemporaryMySQLBackend() as bk:
            q = Queue(bk, visibility_timeout=100)
            q.put(b"a", topic=1)
            q.put(b"a", topic=2)
            q.put(b"a", topic=3)
            q.put(b"a", topic=3)
            q.put(b"a", topic=3)
            q.put(b"a", topic=3)
            q.put(b"a", topic=2)
            q.put(b"a", topic=2)
            q.put(b"a", topic=2)
            q.put(b"a", topic=2)

            topics = dict(q.topics())
            self.assertEqual(
                {
                    1: 1,
                    2: 5,
                    3: 4,
                },
                topics,
            )

            self.assertEqual(0, q.size(100))
            self.assertEqual(1, q.size(1))
            self.assertEqual(5, q.size(2))
            self.assertEqual(4, q.size(3))

            task = q.get_nowait(topic=2)

            self.assertEqual(4, q.size(2))

            task.nack()

            self.assertEqual(5, q.size(2))

    def test_queue_put_get_destroy(self):
        with TemporaryMySQLBackend() as bk:
            q = Queue(bk, visibility_timeout=100)
            q.put(b"test_queue_put_get_destroy", topic=1)
            ret = q.get_nowait(topic=1)
            self.assertIsNotNone(ret)
            self.assertEqual(b"test_queue_put_get_destroy", ret.payload)

    def test_get_nothing(self):
        with TemporaryMySQLBackend() as bk:
            q = Queue(bk, visibility_timeout=100)
            with self.assertRaises(QueueEmpty):
                q.get_nowait(topic=1)

    def test_no_double_get(self):
        with TemporaryMySQLBackend() as bk:
            q = Queue(bk, visibility_timeout=100)

            q.put(b"test_no_double_get", topic=1)
            ret = q.get_nowait(topic=1)
            self.assertIsNotNone(ret)
            self.assertEqual(b"test_no_double_get", ret.payload)

            with self.assertRaises(QueueEmpty):
                q.get_nowait(topic=1)

    def test_queue_automatic_release(self):
        with TemporaryMySQLBackend() as bk:
            q = Queue(bk, visibility_timeout=1)
            q.put(b"test_queue_automatic_release", topic=1)

            x = q.get_nowait(topic=1)
            self.assertEqual(b"test_queue_automatic_release", x.payload)

            time.sleep(2)

            y = q.get_nowait(topic=1)
            self.assertEqual(b"test_queue_automatic_release", y.payload)

    def test_queue_nack(self):
        with TemporaryMySQLBackend() as bk:
            q = Queue(bk, visibility_timeout=100, failure_base_delay=0)
            q.put(b"test_queue_nack", topic=1)

            x = q.get_nowait(topic=1)
            self.assertEqual(b"test_queue_nack", x.payload)

            time.sleep(2)

            with self.assertRaises(QueueEmpty):
                q.get_nowait(topic=1)

            x.nack()
            z = q.get_nowait(topic=1)
            self.assertEqual(b"test_queue_nack", z.payload)

    def test_hash_uniqueness(self):
        with TemporaryMySQLBackend() as bk:
            q = Queue(bk, visibility_timeout=100, failure_base_delay=0)
            q.put(b"", topic=1, priority=0, hsh=b"0000000000000000")
            q.put(b"", topic=1, priority=100, hsh=b"0000000000000001")
            with self.assertRaises(Exception):
                q.put(b"", topic=1, hsh=b"0000000000000001")

            x = q.get(topic=1)
            x.ack()

            q.put(b"", topic=1, hsh=b"0000000000000001")

    def test_batch_put(self):
        with TemporaryMySQLBackend() as bk:
            q = Queue(bk, visibility_timeout=1, failure_base_delay=0)
            q.batch_put([(b"a", 1, None), (b"b", 1, None), (b"c", 1, None)])
            self.assertEqual(3, q.size(topic=1))
            msgs = q.batch_get(topics=[(1, 3)])
            self.assertEqual(3, len(msgs))
            self.assertEqual(0, q.size(topic=1))
