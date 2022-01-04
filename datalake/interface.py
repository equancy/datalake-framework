from abc import ABC, abstractmethod
from logging import getLogger


class IStorage(ABC):  # pragma: no cover
    """
    Storage interface
    """

    @property
    @abstractmethod
    def name(self):
        """
        The storage name
        """
        pass

    @abstractmethod
    def exists(self, key):
        """
        True if the specified key exists, False otherwise
        """
        pass

    @abstractmethod
    def checksum(self, key):
        """
        Return the SHA256 hash for the specified key
        """
        pass

    @abstractmethod
    def is_folder(self, key):
        """
        True if the specified key is a folder-like object, False otherwise
        """
        pass

    @abstractmethod
    def keys_iterator(self, prefix):
        """
        Returns a list of keys that match the specified prefix
        """
        pass

    @abstractmethod
    def upload(self, src, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        """
        Uploads a local file to the storage
        """
        pass

    @abstractmethod
    def download(self, src, dst):
        """
        Download a storage file locally
        """
        pass

    @abstractmethod
    def copy(self, src, dst, bucket=None):
        """
        Copies a storage key to another key in the same storage or another
        """
        pass

    @abstractmethod
    def delete(self, key):
        """
        Remove a key from the storage
        """
        pass

    @abstractmethod
    def move(self, src, dst, bucket=None):
        """
        Moves a storage key to another key in the same storage or in another
        """
        pass

    @abstractmethod
    def put(self, content, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        """
        Puts the specified key's content
        """
        pass

    @abstractmethod
    def get(self, key):
        """
        Returns the content of the specified key
        """
        pass

    @abstractmethod
    def stream(self, key, encoding="utf-8"):
        """
        Returns an iterator on each lines from the specified key
        """
        pass

    @abstractmethod
    def size(self, key):
        """
        Return the size in bytes for the specified key
        """
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


class ISecret(ABC):  # pragma: no cover
    """
    Secret interface
    """

    @property
    @abstractmethod
    def plain(self):
        """
        Return the plain secret
        """
        pass

    @property
    @abstractmethod
    def json(self):
        """
        Return the secret as a dict
        """
        pass


class IMonitor(ABC):  # pragma: no cover
    """
    Monitoring interface
    """

    def safe_push(self, measurement):
        """
        Sends a measurement safely without disrupting the main program
        """
        logger = getLogger(__name__)
        try:
            self.push_measurement(measurement)
        except Exception as e:
            logger.warning(f"An error occured whilst pushing a measurement: {str(e)}")

    @abstractmethod
    def push(self, measurement):
        """
        Sends a measurement to the TSDB backend
        """
        pass
