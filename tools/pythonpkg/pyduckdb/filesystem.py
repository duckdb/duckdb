from fsspec import filesystem, AbstractFileSystem
from fsspec.implementations.memory import MemoryFileSystem
from shutil import copyfileobj
import io

class ModifiedMemoryFileSystem(MemoryFileSystem):
	protocol = ('DUCKDB_INTERNAL_OBJECTSTORE',)
	# defer to the original implementation that doesn't hardcode the protocol
	_strip_protocol = classmethod(AbstractFileSystem._strip_protocol.__func__)

	def add_file(self, object, filename):
		if (isinstance(object, io.BytesIO)):
			print("BYTES IO")
			with self.open(filename, 'wb') as f:
				f.write(object.getvalue())
		elif (isinstance(object, io.StringIO)):
			print("STRING IO")
			with self.open(filename, 'w') as f:
				f.write(object.getvalue())

