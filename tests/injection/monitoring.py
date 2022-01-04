from datalake.interface import IMonitor


class Invalid:
    pass


class Valid(IMonitor):
    def __init__(self, dummy):
        pass

    def push(self, *args, **kwargs):
        pass


class Break(IMonitor):
    def push(self, *args, **kwargs):
        raise NotImplementedError()
