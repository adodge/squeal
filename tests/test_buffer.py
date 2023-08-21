from collections import Counter
from typing import Type

import pytest

from squeal import Queue, Buffer
from .common import TemporaryMySQLBackend, TemporaryLocalBackend, TemporaryBackendMixin


@pytest.mark.parametrize(
    "backend_class", [TemporaryMySQLBackend, TemporaryLocalBackend]
)
class TestBuffer:
    def test_buffer_activity(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_put(
                [(b"a", 1, None)] * 100 + [(b"b", 2, None)] * 100,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            buf = Buffer(Queue(bk))

            msg = buf.get()
            msg2 = buf.get()

            assert {msg.payload, msg2.payload} == {b"a", b"b"}

            msg3 = buf.get()
            assert msg3 is None

            msg.ack()
            msg4 = buf.get()
            assert msg4.payload == msg.payload  # same topic as the one we acked

            msg5 = buf.get()
            assert msg5 is None

    def test_buffer_works_through_large_queue(
        self, backend_class: Type[TemporaryBackendMixin]
    ):
        with backend_class() as bk:
            bk.batch_put(
                [(b"a", 1, None)] * 100 + [(b"b", 2, None)] * 100,
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            buf = Buffer(Queue(bk))

            seen = Counter()
            while True:
                msg = buf.get()
                if msg is None:
                    break
                seen[msg.payload] += 1
                msg.ack()

            assert dict(seen.items()) == {b"a": 100, b"b": 100}

    def test_buffer_finish_queue(self, backend_class: Type[TemporaryBackendMixin]):
        with backend_class() as bk:
            bk.batch_put(
                [(b"a", 1, None)],
                priority=0,
                delay=0,
                failure_base_delay=0,
                visibility_timeout=0,
            )
            buf = Buffer(Queue(bk))

            msg = buf.get()
            assert msg.payload == b"a"
            msg.ack()

            msg2 = buf.get()
            assert msg2 is None
