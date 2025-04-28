from typing import Optional, Union
from pathlib import Path
from duckdb.wal_reader import ParseInfo, ParseInfoType

class ParseInfo:
    def __init__(self, info_type: ParseInfoType) -> None: ...

class WALReader:
    def __init__(self, db: Union[str, Path]) -> None: ...
    def next(self) -> Optional[ParseInfo]: ...
