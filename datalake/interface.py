from abc import ABC, abstractmethod


class AbstractStorage(ABC):  # pragma: no cover
    @abstractmethod
    def exists(self, key):
        pass

    @abstractmethod
    def keys_iterator(self, prefix):
        pass

    @abstractmethod
    def upload(self, src, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        pass

    @abstractmethod
    def download(self, src, dst):
        pass

    @abstractmethod
    def copy(self, src, dst, bucket=None):
        pass

    @abstractmethod
    def delete(self, key):
        pass

    @abstractmethod
    def move(self, src, dst, bucket=None):
        pass

    @abstractmethod
    def put(self, content, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        pass

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def stream(self, key):
        pass
