import time
from unittest import TestCase
from squeal import Queue
from squeal.backend.local import LocalBackend


class TestQueue(TestCase):
    def test_put_get(self):
        q = Queue(LocalBackend())
        q.put(b"a", topic=1)
        msg = q.get(topic=1)
        self.assertEqual(b"a", msg.payload)

    def test_delay(self):
        q = Queue(LocalBackend(), new_message_delay=1)
        q.put(b"a", topic=1)
        self.assertIsNone(q.get(topic=1))
        time.sleep(1.1)
        x = q.get(topic=1)
        self.assertEqual(b"a", x.payload)

    def test_visibility_timeout(self):
        q = Queue(LocalBackend(), visibility_timeout=1)
        q.put(b"a", topic=1)
        q.get(topic=1)
        self.assertIsNone(q.get(topic=1))
        time.sleep(1.1)
        x = q.get(topic=1)
        self.assertEqual(b"a", x.payload)

    def test_queue_topics_dont_interfere(self):
        q = Queue(LocalBackend())
        q.put(b"a", topic=1)

        self.assertIsNone(q.get(topic=2))

        q.put(b"b", topic=2)

        x = q.get(topic=2)
        self.assertIsNotNone(x)

    def test_queue_topics(self):
        q = Queue(LocalBackend())
        for _ in range(1):
            q.put(b"", topic=1)
        for _ in range(5):
            q.put(b"", topic=2)
        for _ in range(4):
            q.put(b"", topic=3)

        topics = dict(q.list_topics())
        self.assertEqual(
            {
                1: 1,
                2: 5,
                3: 4,
            },
            topics,
        )
        self.assertEqual(0, q.get_topic_size(100))
        self.assertEqual(5, q.get_topic_size(2))

    def test_priority(self):
        q = Queue(LocalBackend())
        q.put(b"a", topic=1, priority=0)
        q.put(b"b", topic=1, priority=1)

        msg = q.get(topic=1)
        self.assertEqual(b"b", msg.payload)

    def test_batch_get(self):
        q = Queue(LocalBackend())
        q.put(b"a", topic=1, priority=0)
        q.put(b"b", topic=1, priority=1)

        msgs = q.batch_get(topics=[(1, 2)])
        self.assertEqual(2, len(msgs))

    def test_hash_uniqueness(self):
        q = Queue(LocalBackend())
        q.put(b"", topic=1, priority=0, hsh=b"0000000000000000")
        q.put(b"", topic=1, priority=100, hsh=b"0000000000000001")
        with self.assertRaises(Exception):
            q.put(b"", topic=1, hsh=b"0000000000000001")

        x = q.get(topic=1)
        x.ack()

        q.put(b"", topic=1, hsh=b"0000000000000001")

    def test_batch_put(self):
        q = Queue(LocalBackend())
        q.batch_put([(b"a", 1, None), (b"b", 1, None), (b"c", 1, None)])
        self.assertEqual(3, q.get_topic_size(topic=1))
        msgs = q.batch_get(topics=[(1, 3)])
        self.assertEqual(3, len(msgs))
        self.assertEqual(0, q.get_topic_size(topic=1))
