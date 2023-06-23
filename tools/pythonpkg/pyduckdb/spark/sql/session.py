from typing_extensions import Self


class SparkSession:
	def __init__(self):
		self.name = "session"
		self._master = 'master'

	def newSession(self) -> "SparkSession":
		return SparkSession()

	class Builder:
		def __init__(self):
			self.name = "builder"

		def master(self, name: str) -> Self:
			self._master = name
			return self

		def appName(self, name: str) -> Self:
			self._appName = name
			return self

		def getOrCreate(self) -> "SparkSession":
			return SparkSession()

	builder = Builder()

__all__ = [
	"SparkSession"
]
