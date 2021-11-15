from abc import ABC, abstractmethod


class IStorage(ABC):  # pragma: no cover
    """
    Storage interface
    """

    @property
    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def exists(self, key):
        pass

    @abstractmethod
    def checksum(self, key):
        pass

    @abstractmethod
    def is_folder(self, key):
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
    def stream(self, key, encoding="utf-8"):
        pass


class IStorageEvent(ABC):  # pragma: no cover
    """
    Storage event callback interface
    """

    @abstractmethod
    def process(self, storage, object):
        """
        Callback method for handling an object
        """
        pass
