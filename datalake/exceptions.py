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


class StoreNotFound(CatalogError):
    """
    An error when a store id cannot be found
    """

    pass


class ContainerNotFound(DatalakeError):
    """
    An error when Azure container does not exists
    """

    pass
