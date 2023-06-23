from typing_extensions import Self
from typing import Optional

class SparkSession:
	def __init__(self):
		self.name = "session"
		self._master = 'master'

	def newSession(self) -> "SparkSession":
		return SparkSession()

	class Builder:
		def __init__(self):
			self.name = "builder"
			self._config = {}

		def master(self, name: str) -> Self:
			self._master = name
			return self

		def appName(self, name: str) -> Self:
			self._appName = name
			return self

		def getOrCreate(self) -> "SparkSession":
			return SparkSession()
		
		def config(self, key: Optional[str] = None, value: Optional[str] = None) -> Self:
			if (key and value):
				self._config[key] = value
			return self
		
		def enableHiveSupport(self) -> Self:
			return self

	builder = Builder()

__all__ = [
	"SparkSession"
]
