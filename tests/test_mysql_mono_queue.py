import time
from unittest import TestCase

from squeal import SQLMonoQueue, QueueEmpty
from .common import *


class TestMySQLMonoQueue(TestCase):
    def test_queue_put_get_destroy(self):
        with TemporaryMySQLBackend(acquire_timeout=100) as bk:
            q = SQLMonoQueue(bk)
            q.put(b'test_queue_put_get_destroy')
            ret = q.get()
            self.assertIsNotNone(ret)
            self.assertEqual(b'test_queue_put_get_destroy', ret.payload)

    def test_get_nothing(self):
        with TemporaryMySQLBackend(acquire_timeout=100) as bk:
            q = SQLMonoQueue(bk)
            with self.assertRaises(QueueEmpty):
                q.get()

    def test_no_double_get(self):
        with TemporaryMySQLBackend(acquire_timeout=100) as bk:
            q = SQLMonoQueue(bk)

            q.put(b'test_no_double_get')
            ret = q.get()
            self.assertIsNotNone(ret)
            self.assertEqual(b'test_no_double_get', ret.payload)

            with self.assertRaises(QueueEmpty):
                q.get()

    def test_queue_automatic_release(self):
        with TemporaryMySQLBackend(acquire_timeout=1) as bk:
            q = SQLMonoQueue(bk)
            q.put(b"test_queue_automatic_release")

            x = q.get()
            self.assertEqual(b'test_queue_automatic_release', x.payload)

            time.sleep(2)

            y = q.get()
            self.assertEqual(b'test_queue_automatic_release', y.payload)

    def test_queue_nack(self):
        with TemporaryMySQLBackend(acquire_timeout=100) as bk:
            q = SQLMonoQueue(bk)
            q.put(b"test_queue_nack")

            x = q.get()
            self.assertEqual(b'test_queue_nack', x.payload)

            time.sleep(2)

            with self.assertRaises(QueueEmpty):
                q.get()

            q.nack(x.idx)
            z = q.get()
            self.assertEqual(b'test_queue_nack', z.payload)
