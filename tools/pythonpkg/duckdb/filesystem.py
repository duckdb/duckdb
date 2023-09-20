from fsspec import filesystem, AbstractFileSystem
from fsspec.implementations.memory import MemoryFileSystem
from shutil import copyfileobj
from .bytes_io_wrapper import BytesIOWrapper
from io import TextIOBase


def is_file_like(obj):
    # We only care that we can read from the file
    return hasattr(obj, "read") and hasattr(obj, "seek")


class ModifiedMemoryFileSystem(MemoryFileSystem):
    protocol = ('DUCKDB_INTERNAL_OBJECTSTORE',)
    # defer to the original implementation that doesn't hardcode the protocol
    _strip_protocol = classmethod(AbstractFileSystem._strip_protocol.__func__)

    # Add this manually because it's apparently missing on windows???
    def unstrip_protocol(self, name):
        """Format FS-specific path to generic, including protocol"""
        protos = (self.protocol,) if isinstance(self.protocol, str) else self.protocol
        for protocol in protos:
            if name.startswith(f"{protocol}://"):
                return name
        return f"{protos[0]}://{name}"

    def info(self, path, **kwargs):
        path = self._strip_protocol(path)
        if path in self.store:
            filelike = self.store[path]
            return {
                "name": path,
                "size": getattr(filelike, "size", 0),
                "type": "file",
                "created": getattr(filelike, "created", None),
            }
        else:
            raise FileNotFoundError(path)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        path = self._strip_protocol(path)
        if path in self.store:
            f = self.store[path]
            return f
        else:
            raise FileNotFoundError(path)

    def add_file(self, object, path):
        if not is_file_like(object):
            raise ValueError("Can not read from a non file-like object")
        path = self._strip_protocol(path)
        if isinstance(object, TextIOBase):
            # Wrap this so that we can return a bytes object from 'read'
            self.store[path] = BytesIOWrapper(object)
        else:
            self.store[path] = object
