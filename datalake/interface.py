from abc import ABC, abstractmethod
from logging import getLogger


class IStorage(ABC):  # pragma: no cover
    """The storage interface defines method for manipulating files in a cloud object/blob storage"""

    @property
    @abstractmethod
    def name(self):
        """Returns the name of the storage"""
        pass

    @abstractmethod
    def exists(self, key):
        """Returns ``True`` if the specified key exists, ``False`` otherwise

        Args:
            key (str): the path of a storage object
        """
        pass

    @abstractmethod
    def checksum(self, key):
        """Returns the **SHA256** hash for the file at the specified path

        Args:
            key (str): the path of a storage object
        """
        pass

    @abstractmethod
    def is_folder(self, key):
        """
        Return ``True`` if the specified key is a folder-like object, ``False`` otherwise

        Args:
            key (str): the path of a storage object
        """
        pass

    @abstractmethod
    def keys_iterator(self, prefix):
        """Returns an iterator of keys that match the specified prefix

        Args:
            prefix (str): a partial path of a storage object
        """
        pass

    @abstractmethod
    def upload(self, src, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        """Uploads a local file to the storage
        
        Args:
            src (str): the path of a local file
            dst (str): the path of the target storage object
            content_type (str): the MIME type for the file
            encoding (str): the encoding for the file
            metadata (dict): a map of key/value pairs to add as metadata for the uploaded file
        """
        pass

    @abstractmethod
    def download(self, src, dst):
        """Downloads a storage file locally
        
        Args:
            src (str): the path of a storage object
            dst (str): the path of the target local file
        """
        pass

    @abstractmethod
    def copy(self, src, dst, bucket=None):
        """Copies a storage key to another key in the same storage or another
        
        Args:
            src (str): the path of a storage object to copy
            dst (str): the path of the target storage object
            bucket (str): if specified then the object is copied in this bucket. 
                If ``None`` the object is copied in the same bucket than the source object
        """
        pass

    @abstractmethod
    def delete(self, key):
        """Removes an object from the storage
        
        Args:
            key (str): the path of a storage object"""
        pass

    @abstractmethod
    def move(self, src, dst, bucket=None):
        """Moves a storage key to another key in the same storage or another
        
        Args:
            src (str): the path of a storage object to move
            dst (str): the path of the target storage object
            bucket (str): if specified then the object is moved in this bucket. 
                If ``None`` the object is moved in the same bucket than the source object
        """
        pass

    @abstractmethod
    def put(self, content, dst, content_type="text/csv", encoding="utf-8", metadata={}):
        """Writes the specified content in a storage object

        Args:
            content (str): the content to upload
            dst (str): the path of the target storage object
            content_type (str): the MIME type for the file
            encoding (str): the encoding for the file
            metadata (dict): a map of key/value pairs to add as metadata for the uploaded file
        """
        pass

    @abstractmethod
    def get(self, key):
        """Returns the content of the specified object

        Args:
            key (str): the path of a storage object
        """
        pass

    @abstractmethod
    def stream(self, key, encoding="utf-8"):
        """Returns an iterator on each lines for the specified object

        Args:
            key (str): the path of a storage object
            encoding (str): the encoding to use for decoding the byte stream
        """
        pass

    @abstractmethod
    def size(self, key):
        """Return the size in bytes for the specified object
        
        Args:
            key (str): the path of a storage object
        """
        pass


class IStorageEvent(ABC):  # pragma: no cover
    """The storage interface defines methods for handling event notifications from buckets"""

    @abstractmethod
    def process(self, storage, object):
        """Callback method for handling an object creation

        Args:
            storage (IStorage): the concrete implementation of the Storage interface
            object (str): the path of the created object 
        """
        pass


class ISecret(ABC):  # pragma: no cover
    """The secret interface defines methods for fetching a cloud managed secret"""

    @property
    @abstractmethod
    def plain(self):
        """Returns the secret in plain UTF-8 text"""
        pass

    @property
    @abstractmethod
    def json(self):
        """Returns the secret as a ``dict``"""
        pass


class IMonitor(ABC):  # pragma: no cover
    """The monitoring abstract class defines methods for sending metrics to a Time Series Database"""

    def safe_push(self, measurement):
        """Same as ``push()`` but with all exceptions trapped hence not disrupting the main program"""
        logger = getLogger(__name__)
        try:
            self.push(measurement)
        except Exception as e:
            logger.warning(f"An error occured whilst pushing a measurement: {str(e)}")

    @abstractmethod
    def push(self, measurement):
        """Sends a measurement to the TSDB backend

        Args:
            measurement (Measurement): the measurement to send
        """
        pass
