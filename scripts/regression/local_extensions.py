import re
from pathlib import Path
from typing import List, Sequence


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def extension_loading_args(executable: str, extensions: Sequence[str]) -> List[str]:
    if not extensions:
        return []

    commands = []
    repository = Path(executable).resolve().parent / "repository"
    if repository.is_dir():
        commands.append(f"SET extension_directory={_sql_string(str(repository))}")

    for extension in extensions:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", extension):
            raise ValueError(f"Invalid extension name: {extension}")
        commands.append(f"LOAD {extension}")

    return ["-unsigned", "-cmd", "; ".join(commands) + ";"]
