import abc
from typing import List, Tuple, Iterable, Optional


class Backend(abc.ABC):
    def __init__(self, *args, **kwargs):
        pass

    @property
    def max_payload_size(self) -> Optional[int]:
        raise NotImplementedError

    @property
    def hash_size(self) -> int:
        raise NotImplementedError

    def create(self) -> None:
        raise NotImplementedError

    def destroy(self) -> None:
        raise NotImplementedError

    def batch_put(
        self,
        data: Iterable[Tuple[bytes, int, Optional[bytes]]],
        priority: int,
        delay: int,
        failure_base_delay: int,
        visibility_timeout: int,
    ) -> None:
        raise NotImplementedError

    def release_stalled_messages(self, topic: int) -> int:
        raise NotImplementedError

    def ack(self, task_id: int) -> None:
        raise NotImplementedError

    def batch_get(self, topic: int, size: int) -> List["Message"]:
        raise NotImplementedError

    def batch_nack(self, task_ids: Iterable[int]) -> None:
        raise NotImplementedError

    def batch_touch(self, task_ids: Iterable[int]) -> None:
        raise NotImplementedError

    def list_topics(self) -> List[Tuple[int, int]]:
        raise NotImplementedError

    def get_topic_size(self, topic: int) -> int:
        raise NotImplementedError

    def acquire_topic(self, topic_lock_visibility_timeout: int) -> "TopicLock":
        raise NotImplementedError

    def release_topic(self, topic: int) -> None:
        raise NotImplementedError

    def batch_release_topic(self, topics: Iterable[int]) -> None:
        raise NotImplementedError

    def touch_topic(self, topic: int) -> None:
        raise NotImplementedError

    def batch_touch_topic(self, topics: Iterable[int]) -> None:
        raise NotImplementedError

    def release_stalled_topic_locks(self) -> None:
        raise NotImplementedError


class TopicLock:
    pass


class Message:
    def __init__(self, payload: bytes, idx: int, backend: Backend):
        self.payload = payload
        self.idx = idx
        self.backend = backend
        self.status = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.status is None:
            self.nack()

    @property
    def released(self):
        return self.status is not None

    def ack(self):
        if self.released:
            raise RuntimeError("Message has already been relinquished")
        self.status = True
        self.backend.ack(self.idx)

    def nack(self):
        if self.released:
            raise RuntimeError("Message has already been relinquished")
        self.status = False
        self.backend.batch_nack([self.idx])

    def touch(self):
        if self.released:
            raise RuntimeError("Message has already been relinquished")
        self.backend.batch_touch([self.idx])

    def check(self) -> bool:
        """
        Check whether the message is still owned by this consumer.
        Use a local estimate based on when the message was acquired.
        """
        if self.released:
            return False

        raise NotImplementedError
