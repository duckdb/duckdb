from fsspec import filesystem, AbstractFileSystem
from fsspec.implementations.memory import MemoryFileSystem
from shutil import copyfileobj
from io import StringIO, TextIOBase

# Shamelessly stolen from pandas
class BytesIOWrapper:
	# Wrapper that wraps a StringIO buffer and reads bytes from it
	# Created for compat with pyarrow read_csv
	def __init__(self, buffer: StringIO | TextIOBase, encoding: str = "utf-8") -> None:
		self.buffer = buffer
		self.encoding = encoding
		# Because a character can be represented by more than 1 byte,
		# it is possible that reading will produce more bytes than n
		# We store the extra bytes in this overflow variable, and append the
		# overflow to the front of the bytestring the next time reading is performed
		self.overflow = b""

	def __getattr__(self, attr: str):
		return getattr(self.buffer, attr)

	def read(self, n: int | None = -1) -> bytes:
		assert self.buffer is not None
		bytestring = self.buffer.read(n).encode(self.encoding)
		#When n=-1/n greater than remaining bytes: Read entire file/rest of file
		combined_bytestring = self.overflow + bytestring
		if n is None or n < 0 or n >= len(combined_bytestring):
			self.overflow = b""
			return combined_bytestring
		else:
			to_return = combined_bytestring[:n]
			self.overflow = combined_bytestring[n:]
			return to_return

def is_file_like(obj):
	if not (hasattr(obj, "read") or hasattr(obj, "write")):
		return False
	return bool(hasattr(obj, "__iter__"))

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
			self.store[path] = BytesIOWrapper(object)
		else:
			self.store[path] = object
