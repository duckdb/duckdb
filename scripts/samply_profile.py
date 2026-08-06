#!/usr/bin/env python3
"""Record DuckDB with stock Samply and inject DuckDB resource counters."""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Iterable


class ProfileInjectionError(RuntimeError):
    pass


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ProfileInjectionError(f"profile field {field} must be a finite number")
    return float(value)


def load_profile(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as raw_file:
            is_gzip = raw_file.read(2) == b"\x1f\x8b"
        opener = gzip.open if is_gzip else open
        with opener(path, "rt", encoding="utf-8") as profile_file:
            profile = json.load(profile_file)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ProfileInjectionError(f"could not read Samply profile {path}: {error}") from error
    if not isinstance(profile, dict):
        raise ProfileInjectionError("Samply profile root must be an object")
    return profile


def write_profile_atomic(profile: dict[str, Any], output: Path) -> None:
    output = output.absolute()
    if not output.parent.is_dir():
        raise ProfileInjectionError(f"output directory does not exist: {output.parent}")
    temporary_path: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{output.name}.", dir=output.parent)
        temporary_path = Path(temporary_name)
        with os.fdopen(descriptor, "wb") as raw_file:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw_file, mtime=0) as gzip_file:
                payload = json.dumps(profile, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
                gzip_file.write(payload)
            raw_file.flush()
            os.fsync(raw_file.fileno())
        os.replace(temporary_path, output)
        temporary_path = None
    except OSError as error:
        raise ProfileInjectionError(f"could not atomically write {output}: {error}") from error
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except OSError:
                pass


def read_sidecar(path: Path) -> tuple[int, int, dict[str, list[tuple[int, int]]]]:
    calibration: tuple[int, int] | None = None
    samples: dict[str, list[tuple[int, int]]] = {"rss": [], "network-rx": [], "network-tx": []}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise ProfileInjectionError(f"could not read counter sidecar {path}: {error}") from error
    for line_number, line in enumerate(lines, 1):
        fields = line.split()
        if not fields:
            continue
        try:
            if fields[0] == "clock":
                if len(fields) != 3 or calibration is not None:
                    raise ValueError("expected one 'clock MONOTONIC_NS UNIX_NS' record")
                calibration = (int(fields[1]), int(fields[2]))
                if calibration[0] < 0 or calibration[1] < 0:
                    raise ValueError("clock timestamps cannot be negative")
            else:
                if len(fields) != 3 or fields[1] not in samples:
                    raise ValueError("invalid counter sample")
                monotonic_ns = int(fields[0])
                value = int(fields[2])
                if monotonic_ns < 0 or value < 0:
                    raise ValueError("counter samples cannot be negative")
                samples[fields[1]].append((monotonic_ns, value))
        except ValueError as error:
            raise ProfileInjectionError(f"malformed {path}:{line_number}: {error}") from error
    if calibration is None:
        raise ProfileInjectionError(f"counter sidecar {path} has no clock calibration")
    for name, track_samples in samples.items():
        if any(current[0] < previous[0] for previous, current in zip(track_samples, track_samples[1:])):
            raise ProfileInjectionError(f"counter sidecar {path} has out-of-order {name} samples")
    return calibration[0], calibration[1], samples


def _time_deltas(times: list[float]) -> list[float]:
    if not times:
        return []
    return [times[0], *(current - previous for previous, current in zip(times, times[1:]))]


def _memory_display() -> dict[str, Any]:
    return {
        "graphType": "line-accumulated",
        "unit": "bytes",
        "color": "red",
        "markerSchemaLocation": "timeline-memory",
        "sortWeight": 0,
        "label": "Memory usage (RSS)",
        "tooltipRows": [
            {
                "type": "value",
                "source": "accumulated",
                "format": {"unit": "bytes"},
                "label": "Resident memory",
            }
        ],
    }


def _network_display(label: str, color: str, sort_weight: int) -> dict[str, Any]:
    return {
        "graphType": "line-rate",
        "unit": "bytes-per-second",
        "color": color,
        "markerSchemaLocation": None,
        "sortWeight": sort_weight,
        "label": label,
        "tooltipRows": [
            {
                "type": "value",
                "source": "rate",
                "format": {"unit": "bytes-per-second"},
                "label": "Rate",
            },
            {
                "type": "value",
                "source": "selection-total",
                "format": {"unit": "bytes"},
                "label": "Transferred",
                "requiresPreviewSelection": True,
            },
        ],
    }


def _make_counter(
    *,
    name: str,
    category: str,
    description: str,
    pid: Any,
    main_thread_index: int,
    times: list[float],
    counts: list[int],
    display: dict[str, Any],
) -> dict[str, Any]:
    return {
        "name": name,
        "category": category,
        "description": description,
        "pid": pid,
        "mainThreadIndex": main_thread_index,
        "samples": {"timeDeltas": _time_deltas(times), "count": counts, "length": len(times)},
        "display": display,
    }


def _matching_thread(profile: dict[str, Any], pid: int) -> tuple[int, dict[str, Any]] | None:
    for index, thread in enumerate(profile["threads"]):
        if not isinstance(thread, dict) or "pid" not in thread:
            raise ProfileInjectionError(f"profile thread {index} is missing its pid")
        try:
            matches = int(thread["pid"]) == pid
        except (TypeError, ValueError):
            matches = False
        if matches:
            return index, thread
    return None


def inject_counters(profile: dict[str, Any], sidecars: Iterable[Path]) -> int:
    meta = profile.get("meta")
    threads = profile.get("threads")
    counters = profile.get("counters")
    if not isinstance(meta, dict):
        raise ProfileInjectionError("profile is missing its meta object")
    start_time = _finite_number(meta.get("startTime"), "meta.startTime")
    if not isinstance(threads, list) or not threads:
        raise ProfileInjectionError("profile threads must be a non-empty array")
    if not isinstance(counters, list):
        raise ProfileInjectionError("profile counters must be an array")

    added = 0
    for sidecar in sidecars:
        match = re.fullmatch(r"counter-(\d+)\.txt", sidecar.name)
        if not match:
            continue
        pid = int(match.group(1))
        thread_match = _matching_thread(profile, pid)
        if thread_match is None:
            continue
        main_thread_index, thread = thread_match
        process_start = _finite_number(
            thread.get("processStartupTime"), f"threads[{main_thread_index}].processStartupTime"
        )
        process_end_value = thread.get("processShutdownTime")
        process_end = (
            math.inf
            if process_end_value is None
            else _finite_number(process_end_value, f"threads[{main_thread_index}].processShutdownTime")
        )
        calibration_monotonic, calibration_unix, samples = read_sidecar(sidecar)

        def converted(track: str) -> tuple[list[float], list[int]]:
            converted_samples: list[tuple[float, int]] = []
            for monotonic_ns, value in samples[track]:
                unix_ns = calibration_unix + monotonic_ns - calibration_monotonic
                profile_ms = unix_ns / 1_000_000 - start_time
                if profile_ms < 0 or profile_ms < process_start or profile_ms > process_end:
                    continue
                converted_samples.append((profile_ms, value))
            return ([sample[0] for sample in converted_samples], [sample[1] for sample in converted_samples])

        rss_times, rss_values = converted("rss")
        if rss_times:
            rss_deltas = [
                rss_values[0],
                *(current - previous for previous, current in zip(rss_values, rss_values[1:])),
            ]
            counters.append(
                _make_counter(
                    name="Memory usage (RSS)",
                    category="Memory",
                    description="Resident memory used by the DuckDB process",
                    pid=thread["pid"],
                    main_thread_index=main_thread_index,
                    times=rss_times,
                    counts=rss_deltas,
                    display=_memory_display(),
                )
            )
            added += 1

        for track, name, color, sort_weight in (
            ("network-rx", "System network RX", "blue", 10),
            ("network-tx", "System network TX", "green", 11),
        ):
            times, values = converted(track)
            if times:
                counters.append(
                    _make_counter(
                        name=name,
                        category="Network",
                        description="Bytes transferred across system non-loopback interfaces",
                        pid=thread["pid"],
                        main_thread_index=main_thread_index,
                        times=times,
                        counts=values,
                        display=_network_display(name, color, sort_weight),
                    )
                )
                added += 1
    return added


def inject_profile(raw_profile: Path, sidecar_directory: Path, output: Path) -> int:
    profile = load_profile(raw_profile)
    added = inject_counters(profile, sorted(sidecar_directory.glob("counter-*.txt")))
    write_profile_atomic(profile, output)
    return added


def resolve_samply(argument: str | None) -> str:
    candidate = argument or os.environ.get("SAMPLY") or "samply"
    resolved = shutil.which(candidate)
    if resolved is None:
        raise ProfileInjectionError(f"could not find Samply executable: {candidate}")
    return resolved


def resolve_duckdb(launcher_path: str | Path) -> Path:
    duckdb = Path(launcher_path).resolve().with_name("duckdb")
    if not duckdb.is_file():
        raise ProfileInjectionError(f"could not find DuckDB executable next to profile launcher: {duckdb}")
    return duckdb


def duckdb_arguments(arguments: list[str], launcher_path: str | Path) -> list[str]:
    duckdb = resolve_duckdb(launcher_path)
    if "--" in arguments:
        separator = arguments.index("--")
        wrapper_arguments = arguments[:separator]
        command_arguments = arguments[separator + 1 :]
    else:
        wrapper_arguments = []
        command_arguments = arguments
    return [*wrapper_arguments, "--", str(duckdb), *command_arguments]


def duckdb_main(arguments: list[str] | None = None, *, launcher_path: str | Path) -> int:
    if arguments is None:
        arguments = sys.argv[1:]
    try:
        return main(duckdb_arguments(arguments, launcher_path))
    except ProfileInjectionError as error:
        print(f"samply_profile.py: {error}", file=sys.stderr)
        return 1


def parse_arguments(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samply", help="path to the Samply executable (then SAMPLY, then PATH)")
    parser.add_argument("--output", default="profile.json.gz", help="final injected gzip profile")
    parser.add_argument("--rate", type=float, help="Samply sampling rate in Hz")
    parser.add_argument("--duration", type=float, help="recording duration in seconds")
    parser.add_argument("--profile-name", help="custom profile name")
    parser.add_argument("--no-open", action="store_true", help="do not open the final profile")
    parser.add_argument("--keep-temp", action="store_true", help="preserve raw profile and sidecars")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="command to profile, preceded by --")
    result = parser.parse_args(arguments)
    if not result.command:
        parser.error("a command is required after --")
    if result.command[0] == "--":
        result.command = result.command[1:]
    if not result.command:
        parser.error("a command is required after --")
    if result.rate is not None and (not math.isfinite(result.rate) or result.rate <= 0):
        parser.error("--rate must be positive")
    if result.duration is not None and (not math.isfinite(result.duration) or result.duration <= 0):
        parser.error("--duration must be positive")
    return result


def _format_option(value: float) -> str:
    return format(value, "g")


def main(arguments: list[str] | None = None) -> int:
    options = parse_arguments(arguments)
    temporary_directory = Path(tempfile.mkdtemp(prefix="duckdb-samply-profile-"))
    raw_profile = temporary_directory / "raw-profile.json.gz"
    output = Path(options.output).absolute()
    preserve = options.keep_temp
    try:
        samply = resolve_samply(options.samply)
        record_command = [samply, "record", "--save-only", "--output", str(raw_profile)]
        if options.rate is not None:
            record_command.extend(["--rate", _format_option(options.rate)])
        if options.duration is not None:
            record_command.extend(["--duration", _format_option(options.duration)])
        if options.profile_name is not None:
            record_command.extend(["--profile-name", options.profile_name])
        record_command.extend(["--", *options.command])
        environment = os.environ.copy()
        environment["DUCKDB_SAMPLY_DIR"] = str(temporary_directory)
        record_result = subprocess.run(record_command, env=environment, check=False)
        if not raw_profile.is_file():
            preserve = True
            raise ProfileInjectionError(f"Samply did not create the raw profile {raw_profile}")

        added = inject_profile(raw_profile, temporary_directory, output)
        print(f"Injected {added} DuckDB resource counter(s) into {output}", file=sys.stderr)

        load_status = 0
        if not options.no_open:
            load_status = subprocess.run([samply, "load", str(output)], check=False).returncode
        if record_result.returncode != 0:
            preserve = True
            return record_result.returncode
        if load_status != 0:
            preserve = True
            print(f"samply load failed with exit status {load_status}", file=sys.stderr)
            return load_status
        return 0
    except (OSError, ProfileInjectionError) as error:
        preserve = True
        print(f"samply_profile.py: {error}", file=sys.stderr)
        return 1
    finally:
        if preserve:
            print(f"Raw profile and DuckDB sidecars preserved in {temporary_directory}", file=sys.stderr)
            if raw_profile.exists():
                print(f"Raw profile: {raw_profile}", file=sys.stderr)
        else:
            shutil.rmtree(temporary_directory, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
