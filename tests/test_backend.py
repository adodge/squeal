import time
from typing import Type

import pytest

from .common import TemporaryMySQLBackend, TemporaryLocalBackend, TemporaryBackendMixin


@pytest.mark.parametrize(
    "backend_class", [TemporaryMySQLBackend, TemporaryLocalBackend]
)
class TestBackend:
    def test_create_destroy(self, backend_class: Type[TemporaryBackendMixin]):
        backend = backend_class()
        backend.create()
        backend.destroy()

    def test_release_stalled(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_put(
                [(b"test_release_stalled", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            x = bk.batch_get(topic=1, size=1)[0]
            assert b"test_release_stalled" == x.payload

            time.sleep(1)
            assert 1 == bk.release_stalled_messages()

            y = bk.batch_get(topic=1, size=1)[0]
            assert b"test_release_stalled" == y.payload

    def test_ack(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_put(
                [(b"test_ack", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            x = bk.batch_get(topic=1, size=1)[0]
            assert b"test_ack" == x.payload
            x.ack()

            time.sleep(1)
            bk.release_stalled_messages()

            assert 0 == len(bk.batch_get(topic=1, size=1))

    def test_nack(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_put(
                [(b"test_ack", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            x = bk.batch_get(topic=1, size=1)[0]
            assert b"test_ack" == x.payload

            assert 0 == len(bk.batch_get(topic=1, size=1))

            x.nack()

            z = bk.batch_get(topic=1, size=1)[0]
            assert z is not None

    def test_message_context_manager(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_put(
                [(b"test_ack", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            with bk.batch_get(topic=1, size=1)[0] as task:
                assert task is not None
                pass

            assert not task.status
            time.sleep(1)

            with bk.batch_get(topic=1, size=1)[0] as task:
                assert task is not None
                task.ack()
                pass

            assert task.status
            time.sleep(1)

            assert 0 == len(bk.batch_get(topic=1, size=1))

    def test_priority(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
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
            assert b"b" == msg.payload

    def test_batch_get_empty(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            msgs = bk.batch_get(topic=1, size=2)
            assert 0 == len(msgs)

    def test_batch_get_less_than_full(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_put(
                [(b"b", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            msgs = bk.batch_get(topic=1, size=2)
            assert 1 == len(msgs)
            assert 0 == bk.get_topic_size(topic=1)

    def test_batch_get_full(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_put(
                [(b"b", 1, None)] * 2,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            msgs = bk.batch_get(topic=1, size=2)
            assert 2 == len(msgs)
            assert 0 == bk.get_topic_size(topic=1)

    def test_batch_get_overfull(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_put(
                [(b"b", 1, None)] * 3,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            msgs = bk.batch_get(topic=1, size=2)
            assert 2 == len(msgs)
            assert 1 == bk.get_topic_size(topic=1)

    def test_batch_put(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_put(
                data=[(b"a", 1, None), (b"b", 1, None), (b"c", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            assert 3 == bk.get_topic_size(topic=1)
            msgs = bk.batch_get(topic=1, size=3)
            assert 3 == len(msgs)
            assert 0 == bk.get_topic_size(topic=1)

    def test_uniqueness(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_put(
                data=[
                    (b"a", 1, b"0000000000000000"),
                    (b"b", 1, b"0000000000000001"),
                    (b"c", 1, b"0000000000000002"),
                ],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            assert bk.get_topic_size(1) == 3

    def test_uniqueness_doesnt_preclude_redoing_messages(
        self, backend_class: Type[TemporaryBackendMixin]
    ):
        with backend_class() as bk:
            bk.batch_put(
                data=[(b"a", 1, b"0000000000000000")],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            assert bk.get_topic_size(1) == 1
            msg = bk.batch_get(1, 1)[0]
            msg.ack()

            assert bk.get_topic_size(1) == 0

            bk.batch_put(
                data=[(b"a", 1, b"0000000000000000")],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            assert bk.get_topic_size(1) == 1

    def test_uniqueness_violation(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            n_rows = bk.batch_put(
                data=[
                    (b"a", 1, b"0000000000000000"),
                    (b"b", 1, b"0000000000000001"),
                    (b"c", 1, b"0000000000000001"),
                ],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            assert n_rows == 2
            assert bk.get_topic_size(1) == 2

    def test_topic_acquire(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_put(
                data=[(b"a", 1, None), (b"b", 2, None), (b"c", 3, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            a = bk.acquire_topic(600)
            b = bk.acquire_topic(600)
            c = bk.acquire_topic(600)
            d = bk.acquire_topic(600)

            assert {a.idx, b.idx, c.idx} == {1, 2, 3}
            assert d is None

    def test_topic_timeout(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_put(
                data=[(b"a", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )

            a = bk.acquire_topic(1)
            assert a.idx == 1

            assert 0 == bk.release_stalled_topic_locks()

            b = bk.acquire_topic(1)
            assert b is None

            time.sleep(2)

            assert 1 == bk.release_stalled_topic_locks()
            c = bk.acquire_topic(1)
            assert c.idx == 1

    def test_touching_no_messages(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_touch([])

    def test_nacking_no_messages(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_nack([])

    def test_releasing_no_topics(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_release_topic([])

    def test_touching_no_topics(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_touch_topic([])

    def test_touching_non_list_collection(
        self, backend_class: Type[TemporaryBackendMixin]
    ):
        with backend_class() as bk:
            bk.batch_put(
                data=[(b"a", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            msgs = bk.batch_get(1, 1)
            msgs = {m.idx: m for m in msgs}
            bk.batch_touch(msgs.keys())

    def test_nacking_non_list_collection(
        self, backend_class: Type[TemporaryBackendMixin]
    ):
        with backend_class() as bk:
            bk.batch_put(
                data=[(b"a", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            msgs = bk.batch_get(1, 1)
            msgs = {m.idx: m for m in msgs}
            bk.batch_nack(msgs.keys())

    def test_releasing_non_list_collection(
        self, backend_class: Type[TemporaryBackendMixin]
    ):
        with backend_class() as bk:
            bk.batch_put(
                data=[(b"a", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            msgs = bk.batch_get(1, 1)
            msgs = {m.idx: m for m in msgs}
            bk.batch_release_topic(msgs.keys())

    def test_touching_topics_non_list_collection(
        self, backend_class: Type[TemporaryBackendMixin]
    ):
        with backend_class() as bk:
            bk.batch_put(
                data=[(b"a", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            msgs = bk.batch_get(1, 1)
            msgs = {m.idx: m for m in msgs}
            bk.batch_touch_topic(msgs.keys())

    def test_rate_limiting(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            key = b"0" * bk.hash_size
            for _ in range(10):
                assert bk.rate_limit(key, max_events=10, interval_seconds=60)
            for _ in range(10):
                assert not bk.rate_limit(key, max_events=10, interval_seconds=60)

    def test_rate_limiting_expires(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            key = b"0" * bk.hash_size
            for _ in range(2):
                assert bk.rate_limit(key, max_events=2, interval_seconds=1)
            for _ in range(2):
                assert not bk.rate_limit(key, max_events=2, interval_seconds=1)

            time.sleep(2)

            for _ in range(2):
                assert bk.rate_limit(key, max_events=2, interval_seconds=1)
            for _ in range(2):
                assert not bk.rate_limit(key, max_events=2, interval_seconds=1)

    def test_rate_limiting_bad_key(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            key = b"0" * bk.hash_size + b"0"
            with pytest.raises(ValueError):
                assert bk.rate_limit(key, max_events=10, interval_seconds=60)
