from typing import Protocol, runtime_checkable

@runtime_checkable
class Placeholder(Protocol):
    def read(self):
        pass
