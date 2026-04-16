#!/usr/bin/env python3
"""Create an AFL++ dictionary by harvesting auto_extras from a short fuzzing run."""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

DEFAULT_TARGET = Path("build/fuzzer/test/unittest")
DEFAULT_AFL_FUZZ_BIN = "afl-fuzz"
DEFAULT_FUZZ_SECS = 60
DEFAULT_MIN_TOKEN_LEN = 2


@dataclass(frozen=True)
class DictConfig:
    input_dir: Path
    output_file: Path
    target: Path = DEFAULT_TARGET
    afl_fuzz_cmd: str = DEFAULT_AFL_FUZZ_BIN
    fuzz_secs: int = DEFAULT_FUZZ_SECS
    min_token_len: int = DEFAULT_MIN_TOKEN_LEN


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate an AFL++ dictionary from auto_extras")
    parser.add_argument(
        "-i",
        "--input-dir",
        type=Path,
        required=True,
        help="Input corpus directory",
    )
    parser.add_argument(
        "-o",
        "--output-file",
        type=Path,
        required=True,
        help="Output dictionary file path",
    )
    parser.add_argument(
        "--target",
        type=Path,
        default=DEFAULT_TARGET,
        help=f"Fuzzer target binary (default: {DEFAULT_TARGET})",
    )
    parser.add_argument(
        "--afl-fuzz",
        default=DEFAULT_AFL_FUZZ_BIN,
        help=f"afl-fuzz executable and optional leading flags as one string (default: {DEFAULT_AFL_FUZZ_BIN})",
    )
    parser.add_argument(
        "--fuzz-secs",
        type=int,
        default=DEFAULT_FUZZ_SECS,
        help=f"Duration of the fuzzing run in seconds (default: {DEFAULT_FUZZ_SECS})",
    )
    parser.add_argument(
        "--min-token-len",
        type=int,
        default=DEFAULT_MIN_TOKEN_LEN,
        help=f"Minimum token length to keep (default: {DEFAULT_MIN_TOKEN_LEN})",
    )
    return parser.parse_args()


def normalize_afl_fuzz_cmd(afl_fuzz: str) -> list[str]:
    cmd = shlex.split(afl_fuzz)
    if not cmd:
        raise ValueError("afl-fuzz command must include a binary")
    return cmd


def validate_environment(config: DictConfig, afl_fuzz_cmd: list[str]) -> None:
    afl_fuzz_bin = afl_fuzz_cmd[0]
    if shutil.which(afl_fuzz_bin) is None:
        raise RuntimeError(f"{afl_fuzz_bin} not found in PATH")
    if not config.target.exists():
        raise RuntimeError(f"Fuzzer target missing at {config.target}. Build it first with `make fuzzer`.")
    if not config.input_dir.exists() or not config.input_dir.is_dir():
        raise RuntimeError(f"Input directory does not exist: {config.input_dir}")
    has_seed = any(path.is_file() for path in config.input_dir.iterdir())
    if not has_seed:
        raise RuntimeError(f"Input directory is empty: {config.input_dir}")
    if config.fuzz_secs < 1:
        raise RuntimeError("--fuzz-secs must be >= 1")
    if config.min_token_len < 1:
        raise RuntimeError("--min-token-len must be >= 1")


def format_token(token: bytes) -> str:
    escaped: list[str] = []
    for byte in token:
        if byte == 0x22:
            escaped.append('\\"')
        elif byte == 0x5C:
            escaped.append("\\\\")
        elif 0x20 <= byte <= 0x7E:
            escaped.append(chr(byte))
        else:
            escaped.append(f"\\x{byte:02x}")
    return '"' + "".join(escaped) + '"'


def collect_tokens(out_root: Path, min_token_len: int) -> list[bytes]:
    token_set: set[bytes] = set()
    for token_file in sorted(out_root.glob("*/.state/auto_extras/auto_*")):
        if not token_file.is_file():
            continue
        token = token_file.read_bytes()
        if len(token) < min_token_len:
            continue
        token_set.add(token)
    return sorted(token_set, key=lambda value: (len(value), value))


def write_dictionary(tokens: list[bytes], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8", newline="\n") as handle:
        for idx, token in enumerate(tokens):
            handle.write(f"tok_{idx:06d}={format_token(token)}\n")


def run(config: DictConfig) -> int:
    try:
        afl_fuzz_cmd = normalize_afl_fuzz_cmd(config.afl_fuzz_cmd)
    except ValueError as ex:
        print(str(ex), file=sys.stderr)
        return 2

    try:
        validate_environment(config, afl_fuzz_cmd)
    except RuntimeError as ex:
        print(str(ex), file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="afl_dict_") as workdir:
        out_root = Path(workdir) / "out"
        cmd = [
            *afl_fuzz_cmd,
            "-i",
            str(config.input_dir),
            "-o",
            str(out_root),
            "-V",
            str(config.fuzz_secs),
            "--",
            str(config.target),
        ]
        proc = subprocess.run(cmd, text=True, capture_output=True, check=False, env=os.environ.copy())
        if proc.returncode != 0:
            print("afl-fuzz failed while extracting dictionary tokens", file=sys.stderr)
            if proc.stdout:
                print(proc.stdout.rstrip(), file=sys.stderr)
            if proc.stderr:
                print(proc.stderr.rstrip(), file=sys.stderr)
            return proc.returncode

        tokens = collect_tokens(out_root, config.min_token_len)
        if not tokens:
            print(
                "No auto_extras tokens were generated. Try increasing --fuzz-secs or using a larger corpus.",
                file=sys.stderr,
            )
            return 1

        write_dictionary(tokens, config.output_file)
        print(f"Wrote {len(tokens)} dictionary tokens to {config.output_file}")
        return 0


def main() -> int:
    args = parse_args()
    config = DictConfig(
        input_dir=args.input_dir,
        output_file=args.output_file,
        target=args.target,
        afl_fuzz_cmd=args.afl_fuzz,
        fuzz_secs=args.fuzz_secs,
        min_token_len=args.min_token_len,
    )
    return run(config)


if __name__ == "__main__":
    raise SystemExit(main())
