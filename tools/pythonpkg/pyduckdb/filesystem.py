from fsspec import filesystem, AbstractFileSystem
from fsspec.implementations.memory import MemoryFileSystem
from shutil import copyfileobj
import io

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

	def add_file(self, object, filename):
		if (isinstance(object, io.BytesIO)):
			with self.open(filename, 'wb') as f:
				f.write(object.getvalue())
		elif (isinstance(object, io.StringIO)):
			with self.open(filename, 'w') as f:
				f.write(object.getvalue())
