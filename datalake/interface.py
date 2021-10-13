from abc import ABC, abstractmethod

class AbstractStorage(ABC): # pragma: no cover
    
    @abstractmethod
    def exists(self, key):
        pass