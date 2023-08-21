import logging
from typing import Optional, Dict, List, Coroutine, Any, Generator
from squeal.queue import Queue
from squeal.backend.base import Message


class BufferMessage(Message):
    FIELDS = Message.FIELDS + ["buffer"]

    def __init__(self, *args, buffer: "Buffer", **kwargs):
        super().__init__(*args, **kwargs)
        self.buffer = buffer

    @classmethod
    def from_message(cls, msg: Message, *args, **kwargs) -> "BufferMessage":
        kwargs.update({k: getattr(msg, k) for k in msg.FIELDS})
        return cls(*args, **kwargs)

    def ack(self):
        super().ack()
        self.buffer.ack(self.idx)

    def nack(self):
        super().nack()
        self.buffer.nack(self.idx)


class Buffer:
    def __init__(self, queue: Queue):
        self.queue = queue

        self.topic_buffer: Dict[int, List[Message]] = {}
        self.extra_buffer_multiplier = 2
        self.default_topic_quota: int = 1
        self.topic_quota: Dict[int, int] = {}
        self.topic_processing: Dict[int, int] = {}
        self.message_topic: Dict[int, int] = {}

        self.closed = False

    def close(self):
        self.closed = True
        self.queue.release_topics()
        self.queue.nack_all()

    def touch(self) -> None:
        if self.closed:
            raise RuntimeError
        self.queue.touch_all()
        self.queue.touch_topics()

    def _fill_buffer(self, idx: int) -> None:
        held = len(self.topic_buffer[idx])
        quota = self.topic_quota[idx]
        processing = self.topic_processing[idx]
        if held - processing >= quota:
            return
        target = self.extra_buffer_multiplier * quota
        msgs = self.queue.batch_get([(idx, target - held + processing)])
        self.topic_buffer[idx].extend(msgs)

    def _acquire_topic(self) -> bool:
        topic = self.queue.acquire_topic()
        if topic is None:
            return False
        self.topic_quota[topic.idx] = self.default_topic_quota
        self.topic_processing[topic.idx] = 0
        self.topic_buffer[topic.idx] = []
        self._fill_buffer(topic.idx)
        return True

    def _drop_topic(self, topic: int):
        if self.topic_processing[topic] > 0:
            logging.error("Can't release topic, as there are still pending tasks")
            return

        for msg in self.topic_buffer[topic]:
            msg.nack()
        self.queue.release_topic(topic)

        del self.topic_buffer[topic]
        del self.topic_processing[topic]
        del self.topic_quota[topic]

    def _fill_buffers(self) -> None:
        # Check all the checked-out topics and make sure we have messages in the buffer
        # according to the topic's quota
        need_new_topic = True
        for topic_lock in self.queue.list_held_topics():
            topic = topic_lock.idx
            self._fill_buffer(topic)

            allowed = self.topic_quota[topic] - self.topic_processing[topic]
            if self.topic_buffer[topic] and allowed > 0:
                need_new_topic = False

            if self.topic_processing[topic] == 0 and not self.topic_buffer[topic]:
                self._drop_topic(topic)

        if need_new_topic:
            self._acquire_topic()

    def get(self) -> Optional[BufferMessage]:
        if self.closed:
            raise RuntimeError
        self._fill_buffers()
        for topic_lock in self.queue.list_held_topics():
            topic = topic_lock.idx
            allowed = self.topic_quota[topic] - self.topic_processing[topic]
            if allowed <= 0 or not self.topic_buffer[topic]:
                continue

            msg = self.topic_buffer[topic].pop(-1)
            self.message_topic[msg.idx] = topic
            self.topic_processing[topic] += 1
            return BufferMessage.from_message(msg, buffer=self)
        return None

    def ack(self, message_idx: int) -> None:
        if self.closed:
            raise RuntimeError
        topic = self.message_topic[message_idx]

        self.topic_processing[topic] -= 1

    def nack(self, message_idx: int) -> None:
        if self.closed:
            raise RuntimeError
        topic = self.message_topic[message_idx]
        self.topic_processing[topic] -= 1

    def __iter__(self) -> Generator[BufferMessage, Any, None]:
        while True:
            msg = self.get()
            if msg is None:
                break
            yield msg
