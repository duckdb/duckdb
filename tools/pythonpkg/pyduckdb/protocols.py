from typing import Protocol, runtime_checkable


@runtime_checkable
class TableLike(Protocol):
    def read(self):
        pass
