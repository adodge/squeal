from unittest import TestCase
from squeal import Queue, QueueEmpty
from .common import *


class TestMySQLQueue(TestCase):
    def test_queue_topics_dont_interfere(self):
        with TemporaryMySQLBackend(visibility_timeout=100) as bk:
            q = Queue(bk)
            q.put(b"a", topic=1)

            with self.assertRaises(QueueEmpty):
                q.get_nowait(topic=2)

            q.put(b"b", topic=2)

            x = q.get_nowait(topic=2)
            self.assertIsNotNone(x)

    def test_queue_topics(self):
        with TemporaryMySQLBackend(visibility_timeout=100) as bk:
            q = Queue(bk)
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
