from abc import ABC, abstractmethod


class DataBaseAbstractClass(ABC):
    @abstractmethod
    def set(self, k: str, v: str) -> None:
        pass

    @abstractmethod
    def get(self, k: str) -> str:
        pass

    @abstractmethod
    def unset(self, k: str) -> None:
        pass

    @abstractmethod
    def counts(self, v: str) -> int:
        pass

    @abstractmethod
    def find(self, v: str) -> str:
        pass

    def begin(self):
        pass

    def rollback(self):
        pass

    def commit(self, *args, **kwargs):
        pass
