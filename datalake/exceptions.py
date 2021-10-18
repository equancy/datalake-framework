class DatalakeError(Exception):
    """
    Base exception for framework exceptions
    """

    pass


class CatalogError(DatalakeError):
    """
    Base exception for catalog related errors
    """

    pass


class EntryNotFound(CatalogError):
    """
    An error when a catalog entry cannot be found
    """
    pass
