import abc
from typing import List, Tuple, Sequence, Iterable, Optional


class QueueEmpty(Exception):
    pass


class Backend(abc.ABC):
    def __init__(self, *args, **kwargs):
        pass

    def create(self) -> None:
        raise NotImplementedError

    def destroy(self) -> None:
        raise NotImplementedError

    @property
    def max_payload_size(self) -> Optional[int]:
        raise NotImplementedError

    @property
    def hash_size(self) -> int:
        raise NotImplementedError

    def put(
        self,
        payload: bytes,
        topic: int,
        hsh: Optional[bytes],
        priority: int,
        delay: int,
        failure_base_delay: int,
        visibility_timeout: int,
    ) -> None:
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

    def get(self, topic: int) -> "Message":
        raise NotImplementedError

    def batch_get(self, topic: int, size: int) -> List["Message"]:
        raise NotImplementedError

    def ack(self, task_id: int) -> None:
        raise NotImplementedError

    def nack(self, task_id: int) -> None:
        raise NotImplementedError

    def batch_nack(self, task_ids: Iterable[int]) -> None:
        raise NotImplementedError

    def touch(self, task_id: int) -> None:
        raise NotImplementedError

    def batch_touch(self, task_ids: Iterable[int]) -> None:
        raise NotImplementedError

    def topics(self) -> List[Tuple[int, int]]:
        raise NotImplementedError

    def size(self, topic: int) -> int:
        raise NotImplementedError


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
        self.backend.nack(self.idx)

    def touch(self):
        if self.released:
            raise RuntimeError("Message has already been relinquished")
        self.backend.touch(self.idx)

    def check(self) -> bool:
        """
        Check whether the message is still owned by this consumer.
        Use a local estimate based on when the message was acquired.
        """
        if self.released:
            return False

        raise NotImplementedError
