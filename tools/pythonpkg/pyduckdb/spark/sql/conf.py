from typing import Optional

class RuntimeConfig:
	def __init__(self):
		self._config = {}
		pass
	
	def set(self, key: str, value: str) -> None:
		self._config[key] = value

	def get(self, key: str, default: Optional[str] = 'default') -> str:
		if key in self._config:
			return self._config[key]
		if default:
			return default
		# raise exception?
		return 'default'

__all__ = [
	"RuntimeConfig"
]
