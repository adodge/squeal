import abc
from typing import List, Tuple


class QueueEmpty(Exception):
    pass


class Backend(abc.ABC):
    def __init__(self, *args, **kwargs):
        pass

    def create(self) -> None:
        raise NotImplementedError

    def destroy(self) -> None:
        raise NotImplementedError

    def put(
        self,
        payload: bytes,
        topic: int,
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

    def ack(self):
        if self.status is not None:
            raise RuntimeError(
                "Trying to ack a Message that's already been acked or nacked"
            )
        self.status = True
        self.backend.ack(self.idx)

    def nack(self):
        if self.status is not None:
            raise RuntimeError(
                "Trying to nack a Message that's already been acked or nacked"
            )
        self.status = False
        self.backend.nack(self.idx)
