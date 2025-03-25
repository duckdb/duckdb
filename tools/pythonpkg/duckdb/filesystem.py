from fsspec import filesystem, AbstractFileSystem
from fsspec.implementations.memory import MemoryFileSystem, MemoryFile
from .bytes_io_wrapper import BytesIOWrapper
from io import TextIOBase

def is_file_like(obj):
    # We only care that we can read from the file
    return hasattr(obj, "read") and hasattr(obj, "seek")


class ModifiedMemoryFileSystem(MemoryFileSystem):
    protocol = ('DUCKDB_INTERNAL_OBJECTSTORE',)
    # defer to the original implementation that doesn't hardcode the protocol
    _strip_protocol = classmethod(AbstractFileSystem._strip_protocol.__func__)

    def add_file(self, object, path):
        if not is_file_like(object):
            raise ValueError("Can not read from a non file-like object")
        path = self._strip_protocol(path)
        if isinstance(object, TextIOBase):
            # Wrap this so that we can return a bytes object from 'read'
            object = BytesIOWrapper(object)
        self.store[path] = MemoryFile(self, path, object.read())
