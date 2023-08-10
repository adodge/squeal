import time
from unittest import TestCase
from squeal import Queue, QueueEmpty
from squeal import LocalBackend


class TestQueue(TestCase):
    def test_put_get(self):
        q = Queue(LocalBackend())
        q.put(b"a", topic=1)
        msg = q.get(topic=1)
        self.assertEqual(b"a", msg.payload)

    def test_get_nowait(self):
        q = Queue(LocalBackend(), timeout=-1)
        with self.assertRaises(QueueEmpty):
            x = q.get_nowait(topic=1)

    def test_timeout(self):
        q = Queue(LocalBackend(), timeout=1, poll_interval=0.1)
        with self.assertRaises(QueueEmpty):
            x = q.get(topic=1)

    def test_delay(self):
        q = Queue(LocalBackend(), delay=1)
        q.put(b"a", topic=1)
        with self.assertRaises(QueueEmpty):
            q.get_nowait(topic=1)
        time.sleep(1.1)
        x = q.get_nowait(topic=1)
        self.assertEqual(b"a", x.payload)

    def test_visibility_timeout(self):
        q = Queue(LocalBackend(), visibility_timeout=1)
        q.put(b"a", topic=1)
        q.get_nowait(topic=1)
        with self.assertRaises(QueueEmpty):
            q.get_nowait(topic=1)
        time.sleep(1.1)
        x = q.get_nowait(topic=1)
        self.assertEqual(b"a", x.payload)

    def test_queue_topics_dont_interfere(self):
        q = Queue(LocalBackend())
        q.put(b"a", topic=1)

        with self.assertRaises(QueueEmpty):
            q.get_nowait(topic=2)

        q.put(b"b", topic=2)

        x = q.get_nowait(topic=2)
        self.assertIsNotNone(x)

    def test_queue_topics(self):
        q = Queue(LocalBackend())
        for _ in range(1):
            q.put(b"", topic=1)
        for _ in range(5):
            q.put(b"", topic=2)
        for _ in range(4):
            q.put(b"", topic=3)

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
        self.assertEqual(5, q.size(2))