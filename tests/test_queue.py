import time
from typing import Type

import pytest

from squeal import Queue
from .common import TemporaryMySQLBackend, TemporaryLocalBackend, TemporaryBackendMixin


@pytest.mark.parametrize(
    "backend_class", [TemporaryMySQLBackend, TemporaryLocalBackend]
)
class TestMySQLQueue:
    def test_queue_topics_dont_interfere(
        self, backend_class: Type[TemporaryBackendMixin]
    ):
        with backend_class() as bk:
            q = Queue(bk)

            q.put(b"a", topic=1)
            assert q.get(topic=2) is None

            q.put(b"b", topic=2)
            assert q.get(topic=2) is not None

    def test_queue_topics(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk)
            for _ in range(1):
                q.put(b"", topic=1)
            for _ in range(5):
                q.put(b"", topic=2)
            for _ in range(4):
                q.put(b"", topic=3)

            topics = dict(q.list_topics())
            assert {1: 1, 2: 5, 3: 4} == topics

            assert 0 == q.get_topic_size(100)
            assert 1 == q.get_topic_size(1)
            assert 5 == q.get_topic_size(2)
            assert 4 == q.get_topic_size(3)

            x = q.get(topic=3)
            assert x is not None

            assert 3 == q.get_topic_size(3)

    def test_queue_put_get_destroy(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk)
            q.put(b"test_queue_put_get_destroy", topic=1)
            ret = q.get(topic=1)

            assert b"test_queue_put_get_destroy" == ret.payload

    def test_get_nothing(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk)
            assert q.get(topic=1) is None

    def test_no_double_get(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk)

            q.put(b"test_no_double_get", topic=1)
            ret = q.get(topic=1)

            assert b"test_no_double_get" == ret.payload

            assert q.get(topic=1) is None

    def test_queue_automatic_release(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk, visibility_timeout=1)
            q.put(b"test_queue_automatic_release", topic=1)

            x = q.get(topic=1)
            assert b"test_queue_automatic_release" == x.payload

            time.sleep(2)

            y = q.get(topic=1)
            assert b"test_queue_automatic_release" == y.payload

    def test_queue_nack(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk, failure_base_delay=0)
            q.put(b"test_queue_nack", topic=1)

            x = q.get(topic=1)
            assert b"test_queue_nack" == x.payload

            assert q.get(topic=1) is None

            x.nack()
            z = q.get(topic=1)
            assert b"test_queue_nack" == z.payload

    def test_queue_skip_nack_with_delay(
        self, backend_class: Type[TemporaryBackendMixin]
    ):
        with backend_class() as bk:
            q = Queue(bk, failure_base_delay=100)
            q.put(b"a", topic=1)
            q.put(b"b", topic=1)
            q.put(b"c", topic=1)

            x1 = q.get(topic=1)
            assert b"a" == x1.payload
            x1.nack()

            x2 = q.get(topic=1)
            assert b"b" == x2.payload
            x2.nack()

            x3 = q.get(topic=1)
            assert b"c" == x3.payload
            x3.nack()

            x4 = q.get(topic=1)
            assert x4 is None

    def test_hash_uniqueness(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk)
            assert 1 == q.put(b"", topic=1, priority=0, hsh=b"0000000000000000")
            assert 1 == q.put(b"", topic=1, priority=100, hsh=b"0000000000000001")
            assert 0 == q.put(b"", topic=1, hsh=b"0000000000000001")

            x = q.get(topic=1)
            x.ack()

            assert 1 == q.put(b"", topic=1, hsh=b"0000000000000001")

    def test_batch_put_uniqueness_violation(
        self, backend_class: Type[TemporaryBackendMixin]
    ):
        with backend_class() as bk:
            q = Queue(bk)
            assert 1 == q.batch_put([(b"b", 1, b"0000000000000000")], priority=0)
            assert 0 == q.batch_put([(b"b", 1, b"0000000000000000")], priority=0)

    def test_put_with_rate_limit(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk, rate_limit_per_hour=1)
            assert 1 == q.put(item=b"a", topic=1, hsh=b"0000000000000000")
            assert 0 == q.put(item=b"a", topic=1, hsh=b"0000000000000000")
            m = q.get(topic=1)
            m.ack()
            assert 0 == q.put(item=b"a", topic=1, hsh=b"0000000000000000")

    def test_batch_put(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk)
            q.batch_put([(b"a", 1, None), (b"b", 1, None), (b"c", 1, None)])
            assert 3 == q.get_topic_size(topic=1)
            msgs = q.batch_get(topics=[(1, 3)])
            assert 3 == len(msgs)
            assert 0 == q.get_topic_size(topic=1)

    def test_batch_get(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk)
            q.put(b"a", topic=1, priority=0)
            q.put(b"b", topic=1, priority=1)

            msgs = q.batch_get(topics=[(1, 2)])
            assert 2 == len(msgs)

    def test_put_get(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk)
            q.put(b"a", topic=1)
            msg = q.get(topic=1)
            assert b"a" == msg.payload

    def test_delay(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk, new_message_delay=1)
            q.put(b"a", topic=1)
            assert q.get(topic=1) is None
            time.sleep(1.1)
            x = q.get(topic=1)
            assert b"a" == x.payload

    def test_visibility_timeout(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk, visibility_timeout=1)
            q.put(b"a", topic=1)
            q.get(topic=1)
            assert q.get(topic=1) is None
            time.sleep(2)
            x = q.get(topic=1)
            assert b"a" == x.payload

    def test_priority(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk)
            q.put(b"a", topic=1, priority=0)
            q.put(b"b", topic=1, priority=1)

            msg = q.get(topic=1)
            assert b"b" == msg.payload

    def test_topic_lock_with_no_topics(
        self, backend_class: Type[TemporaryBackendMixin]
    ):
        with backend_class() as bk:
            q = Queue(bk)

            topic = q.acquire_topic()
            assert topic is None

    def test_topic_lock(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            q = Queue(bk)
            q.put(b"", topic=1)

            topic = q.acquire_topic()
            assert topic.idx == 1

            topic2 = q.acquire_topic()
            assert topic2 is None

            assert len(q.list_held_topics()) == 1
            topic.release()
            assert len(q.list_held_topics()) == 0

            topic3 = q.acquire_topic()
            assert topic3.idx == 1
