#!/usr/bin/env python3
"""Record DuckDB with stock Samply and inject DuckDB counters and HTTP requests."""

from __future__ import annotations

import argparse
import base64
import binascii
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
    samples: dict[str, list[tuple[int, int]]] = {
        "tracked-memory": [],
        "http-download": [],
        "http-upload": [],
    }
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


def _decode_http_field(value: str) -> str:
    try:
        return base64.b64decode(value, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeError) as error:
        raise ValueError("invalid base64-encoded UTF-8 field") from error


def read_http_sidecar(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise ProfileInjectionError(f"could not read HTTP sidecar {path}: {error}") from error
    requests = []
    for line_number, line in enumerate(lines, 1):
        if not line:
            continue
        fields = line.split("\t")
        try:
            if len(fields) != 10 or fields[0] != "1":
                raise ValueError("expected a version 1 HTTP record with 10 fields")
            start_unix_ns = int(fields[1])
            duration_ns = int(fields[2])
            status_code = int(fields[3])
            bytes_received = int(fields[4])
            time_to_first_byte_ns = int(fields[5])
            method = fields[6]
            if start_unix_ns < 0 or duration_ns < 0 or bytes_received < 0:
                raise ValueError("timestamps, durations, and byte counts cannot be negative")
            if status_code < -1 or status_code > 999:
                raise ValueError("invalid HTTP status code")
            if time_to_first_byte_ns < -1 or time_to_first_byte_ns > duration_ns:
                raise ValueError("invalid time to first byte")
            if not method or any(character.isspace() for character in method):
                raise ValueError("invalid HTTP method")
            requests.append(
                {
                    "start_unix_ns": start_unix_ns,
                    "duration_ns": duration_ns,
                    "status_code": status_code,
                    "bytes_received": bytes_received,
                    "time_to_first_byte_ns": time_to_first_byte_ns,
                    "method": method,
                    "url": _decode_http_field(fields[7]),
                    "request_range": _decode_http_field(fields[8]),
                    "response_content_range": _decode_http_field(fields[9]),
                }
            )
        except ValueError as error:
            raise ProfileInjectionError(f"malformed {path}:{line_number}: {error}") from error
    return requests


def read_query_sidecar(path: Path) -> list[dict[str, Any]]:
    records = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise ProfileInjectionError(f"could not read query sidecar {path}: {error}") from error
    for line_number, line in enumerate(lines, 1):
        if not line:
            continue
        try:
            record = json.loads(line)
            if not isinstance(record, dict) or record.get("version") != 1:
                raise ValueError("expected a version 1 query profile record")
            start_unix_ns = record.get("start_unix_ns")
            duration_ns = record.get("duration_ns")
            profile = record.get("profile")
            if (
                isinstance(start_unix_ns, bool)
                or not isinstance(start_unix_ns, int)
                or start_unix_ns < 0
                or isinstance(duration_ns, bool)
                or not isinstance(duration_ns, int)
                or duration_ns < 0
                or not isinstance(profile, dict)
            ):
                raise ValueError("invalid query timestamps or profile")
            records.append(record)
        except (json.JSONDecodeError, ValueError) as error:
            raise ProfileInjectionError(f"malformed {path}:{line_number}: {error}") from error
    return records


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
        "label": "Tracked Memory",
        "tooltipRows": [
            {
                "type": "value",
                "source": "accumulated",
                "format": {"unit": "bytes"},
                "label": "Tracked memory",
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
    first_match = None
    for index, thread in enumerate(profile["threads"]):
        if not isinstance(thread, dict) or "pid" not in thread:
            raise ProfileInjectionError(f"profile thread {index} is missing its pid")
        try:
            matches = int(thread["pid"]) == pid
        except (TypeError, ValueError):
            matches = False
        if matches:
            if first_match is None:
                first_match = (index, thread)
            if thread.get("isMainThread") is True:
                return index, thread
    return first_match


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

        memory_times, memory_values = converted("tracked-memory")
        if memory_times:
            memory_deltas = [
                memory_values[0],
                *(current - previous for previous, current in zip(memory_values, memory_values[1:])),
            ]
            counters.append(
                _make_counter(
                    name="Tracked Memory",
                    category="Memory",
                    description="Live bytes requested through DuckDB memory allocators",
                    pid=thread["pid"],
                    main_thread_index=main_thread_index,
                    times=memory_times,
                    counts=memory_deltas,
                    display=_memory_display(),
                )
            )
            added += 1

        for track, name, color, sort_weight in (
            ("http-download", "HTTP Download", "blue", 10),
            ("http-upload", "HTTP Upload", "green", 11),
        ):
            times, values = converted(track)
            if times:
                counters.append(
                    _make_counter(
                        name=name,
                        category="Network",
                        description="HTTP payload bytes transferred by DuckDB",
                        pid=thread["pid"],
                        main_thread_index=main_thread_index,
                        times=times,
                        counts=values,
                        display=_network_display(name, color, sort_weight),
                    )
                )
                added += 1
    return added


def _network_category(profile: dict[str, Any]) -> int:
    meta = profile.get("meta")
    if not isinstance(meta, dict):
        raise ProfileInjectionError("profile is missing its meta object")
    categories = meta.get("categories")
    if not isinstance(categories, list):
        raise ProfileInjectionError("profile meta.categories must be an array")
    for index, category in enumerate(categories):
        if isinstance(category, dict) and category.get("name") == "Network":
            return index
    categories.append({"name": "Network", "color": "lightblue", "subcategories": ["Other"]})
    return len(categories) - 1


def _marker_table(thread: dict[str, Any], thread_index: int) -> dict[str, Any]:
    markers = thread.get("markers")
    if not isinstance(markers, dict):
        raise ProfileInjectionError(f"profile thread {thread_index} is missing its marker table")
    length = markers.get("length")
    if isinstance(length, bool) or not isinstance(length, int) or length < 0:
        raise ProfileInjectionError(f"profile thread {thread_index} has an invalid marker table length")
    for column in ("category", "data", "endTime", "name", "phase", "startTime"):
        values = markers.get(column)
        if not isinstance(values, list) or len(values) != length:
            raise ProfileInjectionError(f"profile thread {thread_index} has an invalid marker column {column}")
    string_array = thread.get("stringArray")
    if not isinstance(string_array, list) or not all(isinstance(value, str) for value in string_array):
        raise ProfileInjectionError(f"profile thread {thread_index} has an invalid string array")
    return markers


def _next_network_marker_id(profile: dict[str, Any]) -> int:
    used_ids = set()
    for thread in profile["threads"]:
        if not isinstance(thread, dict):
            continue
        markers = thread.get("markers")
        if not isinstance(markers, dict) or not isinstance(markers.get("data"), list):
            continue
        for payload in markers["data"]:
            if not isinstance(payload, dict) or payload.get("type") != "Network":
                continue
            marker_id = payload.get("id")
            if isinstance(marker_id, int) and not isinstance(marker_id, bool) and marker_id >= 0:
                used_ids.add(marker_id)
    return max(used_ids, default=0) + 1


def _append_raw_marker(
    markers: dict[str, Any], *, category: int, data: dict[str, Any], end_time: float, name: int, start_time: float
) -> None:
    markers["category"].append(category)
    markers["data"].append(data)
    markers["endTime"].append(end_time)
    markers["name"].append(name)
    markers["phase"].append(1)
    markers["startTime"].append(start_time)
    markers["length"] += 1


def _duckdb_category(profile: dict[str, Any]) -> int:
    categories = profile["meta"].get("categories")
    if not isinstance(categories, list):
        categories = []
        profile["meta"]["categories"] = categories
    for index, category in enumerate(categories):
        if isinstance(category, dict) and category.get("name") == "DuckDB":
            return index
    categories.append({"name": "DuckDB", "color": "purple", "subcategories": ["Other"]})
    return len(categories) - 1


def _ensure_duckdb_marker_schema(profile: dict[str, Any]) -> None:
    schemas = profile["meta"].get("markerSchema")
    if schemas is None:
        schemas = []
        profile["meta"]["markerSchema"] = schemas
    elif not isinstance(schemas, list):
        raise ProfileInjectionError("profile meta.markerSchema must be an array")
    if any(isinstance(schema, dict) and schema.get("name") == "DuckDBProfile" for schema in schemas):
        return
    schemas.append(
        {
            "name": "DuckDBProfile",
            "display": ["marker-chart", "marker-table"],
            "chartLabel": "{marker.data.name}",
            "tooltipLabel": "{marker.data.name}",
            "tableLabel": "{marker.data.name}",
            "data": [
                {"key": "name", "label": "Name", "format": "unique-string", "searchable": True},
                {"key": "kind", "label": "Kind", "format": "unique-string", "searchable": True},
                {"key": "metric", "label": "Metric", "format": "unique-string", "searchable": True},
                {"key": "activeDuration", "label": "Active time", "format": "duration"},
                {"key": "path", "label": "Operator path", "format": "unique-string", "searchable": True},
                {"key": "details", "label": "Details", "format": "unique-string", "searchable": True},
            ],
        }
    )


def _query_marker_name(sql: Any) -> str:
    if not isinstance(sql, str):
        return "Query: <unknown>"
    normalized = " ".join(sql.split()).replace("\x00", "?") or "<empty>"
    prefix = "Query: "
    maximum_bytes = 900 - len(prefix.encode("utf-8"))
    encoded = normalized.encode("utf-8")
    if len(encoded) <= maximum_bytes:
        return prefix + normalized
    ellipsis = "…"
    normalized = normalized[:500]
    while len((normalized + ellipsis).encode("utf-8")) > maximum_bytes:
        normalized = normalized[:-1]
    return prefix + normalized + ellipsis


def _metric_title(metric: str) -> str:
    return metric.rsplit(".", 1)[-1].replace("_", " ").title()


def inject_query_profiles(profile: dict[str, Any], sidecars: Iterable[Path]) -> int:
    meta = profile.get("meta")
    threads = profile.get("threads")
    if not isinstance(meta, dict):
        raise ProfileInjectionError("profile is missing its meta object")
    start_time = _finite_number(meta.get("startTime"), "meta.startTime")
    if not isinstance(threads, list) or not threads:
        raise ProfileInjectionError("profile threads must be a non-empty array")

    records_by_pid: dict[int, list[dict[str, Any]]] = {}
    for sidecar in sidecars:
        match = re.fullmatch(r"query-(\d+)-(\d+)\.jsonl", sidecar.name)
        if match:
            records_by_pid.setdefault(int(match.group(1)), []).extend(read_query_sidecar(sidecar))
    if not records_by_pid:
        return 0

    category = _duckdb_category(profile)
    _ensure_duckdb_marker_schema(profile)
    added = 0
    for pid, records in records_by_pid.items():
        thread_match = _matching_thread(profile, pid)
        if thread_match is None:
            continue
        thread_index, thread = thread_match
        process_start = _finite_number(thread.get("processStartupTime"), f"threads[{thread_index}].processStartupTime")
        process_end_value = thread.get("processShutdownTime")
        process_end = (
            math.inf
            if process_end_value is None
            else _finite_number(process_end_value, f"threads[{thread_index}].processShutdownTime")
        )
        markers = _marker_table(thread, thread_index)
        string_array = thread["stringArray"]

        for record in sorted(records, key=lambda item: item["start_unix_ns"]):
            query_start = record["start_unix_ns"] / 1_000_000 - start_time
            query_end = query_start + record["duration_ns"] / 1_000_000
            if query_start < 0 or query_start < process_start or query_start > process_end:
                continue
            query_end = max(query_start, min(query_end, process_end))
            query_profile = record["profile"]
            candidates: list[tuple[float, float, str, dict[str, Any]]] = []

            def add_marker(start: float, end: float, title: str, data: dict[str, Any]) -> None:
                start = max(query_start, start)
                end = min(query_end, max(start, end))
                candidates.append((start, end, title, {"type": "DuckDBProfile", "name": title, **data}))

            query = query_profile.get("query")
            sql = query.get("sql") if isinstance(query, dict) else None
            add_marker(query_start, query_end, _query_marker_name(sql), {"kind": "query"})

            bounds_by_name = {}
            timing_bounds = query_profile.get("timing_bounds")
            if isinstance(timing_bounds, list):
                for bounds in timing_bounds:
                    if not isinstance(bounds, dict) or not isinstance(bounds.get("name"), str):
                        continue
                    start_ns = bounds.get("start_ns")
                    end_ns = bounds.get("end_ns")
                    if isinstance(start_ns, int) and isinstance(end_ns, int) and 0 <= start_ns <= end_ns:
                        bounds_by_name[bounds["name"]] = (start_ns, end_ns)

            planning_metrics = {
                "parser.total_time": "Parser",
                "planner.total_time": "Planner",
                "optimizer.total_time": "Optimizer",
                "physical_planner.total_time": "Physical Planner",
            }
            planning_spans = []
            for metric, title in planning_metrics.items():
                bounds = bounds_by_name.get(metric)
                if bounds is None:
                    continue
                phase_start = query_start + bounds[0] / 1_000_000
                phase_end = query_start + bounds[1] / 1_000_000
                planning_spans.append((phase_start, phase_end))
                add_marker(phase_start, phase_end, title, {"kind": "phase", "metric": metric})

            if planning_spans:
                planning_start = min(span[0] for span in planning_spans)
                planning_end = max(span[1] for span in planning_spans)
                add_marker(planning_start, planning_end, "Planning", {"kind": "phase"})
            else:
                planning_end = query_start
            add_marker(planning_end, query_end, "Execution", {"kind": "phase"})

            for metric, bounds in bounds_by_name.items():
                if metric in planning_metrics or metric == "query.total_time":
                    continue
                if not metric.startswith(("planner.", "optimizer.", "physical_planner.")):
                    continue
                phase_start = query_start + bounds[0] / 1_000_000
                phase_end = query_start + bounds[1] / 1_000_000
                add_marker(
                    phase_start,
                    phase_end,
                    _metric_title(metric),
                    {"kind": "phase detail", "metric": metric},
                )

            def add_operators(nodes: Any, path: list[str]) -> None:
                if not isinstance(nodes, list):
                    return
                for node in nodes:
                    if not isinstance(node, dict):
                        continue
                    operator_type = node.get("type")
                    operator_type = operator_type if isinstance(operator_type, str) else "UNKNOWN"
                    operator_path = [*path, operator_type]
                    bounds = node.get("timing_bounds")
                    if isinstance(bounds, dict):
                        start_ns = bounds.get("start_ns")
                        end_ns = bounds.get("end_ns")
                        if isinstance(start_ns, int) and isinstance(end_ns, int) and 0 <= start_ns <= end_ns:
                            details = node.get("extra_info")
                            payload: dict[str, Any] = {
                                "kind": "operator",
                                "path": " → ".join(operator_path),
                                "details": (
                                    json.dumps(details, separators=(",", ":"), ensure_ascii=False)
                                    if isinstance(details, dict)
                                    else ""
                                ),
                            }
                            timing = node.get("timing")
                            if isinstance(timing, (int, float)) and not isinstance(timing, bool):
                                payload["activeDuration"] = timing * 1000
                            add_marker(
                                query_start + start_ns / 1_000_000,
                                query_start + end_ns / 1_000_000,
                                f"Operator: {operator_type}",
                                payload,
                            )
                    add_operators(node.get("children"), operator_path)

            add_operators(query_profile.get("operator"), [])
            for marker_start, marker_end, title, data in sorted(candidates, key=lambda item: (item[0], -item[1])):
                name_index = len(string_array)
                string_array.append(title)
                _append_raw_marker(
                    markers,
                    category=category,
                    data=data,
                    end_time=marker_end,
                    name=name_index,
                    start_time=marker_start,
                )
                added += 1
    return added


def inject_http_requests(profile: dict[str, Any], sidecars: Iterable[Path]) -> int:
    meta = profile.get("meta")
    threads = profile.get("threads")
    if not isinstance(meta, dict):
        raise ProfileInjectionError("profile is missing its meta object")
    start_time = _finite_number(meta.get("startTime"), "meta.startTime")
    if not isinstance(threads, list) or not threads:
        raise ProfileInjectionError("profile threads must be a non-empty array")

    requests_by_pid: dict[int, list[dict[str, Any]]] = {}
    for sidecar in sidecars:
        match = re.fullmatch(r"http-(\d+)-(\d+)\.txt", sidecar.name)
        if not match:
            continue
        requests_by_pid.setdefault(int(match.group(1)), []).extend(read_http_sidecar(sidecar))
    if not requests_by_pid:
        return 0

    category = _network_category(profile)
    marker_id = _next_network_marker_id(profile)
    added = 0
    for pid, requests in requests_by_pid.items():
        thread_match = _matching_thread(profile, pid)
        if thread_match is None:
            continue
        thread_index, thread = thread_match
        process_start = _finite_number(thread.get("processStartupTime"), f"threads[{thread_index}].processStartupTime")
        process_end_value = thread.get("processShutdownTime")
        process_end = (
            math.inf
            if process_end_value is None
            else _finite_number(process_end_value, f"threads[{thread_index}].processShutdownTime")
        )
        markers = _marker_table(thread, thread_index)
        string_array = thread["stringArray"]

        requests.sort(key=lambda request: (request["start_unix_ns"], request["duration_ns"]))
        for request in requests:
            request_start = request["start_unix_ns"] / 1_000_000 - start_time
            request_end = request_start + request["duration_ns"] / 1_000_000
            if request_start < 0 or request_start < process_start or request_start > process_end:
                continue
            request_end = min(request_end, process_end)
            request_end = max(request_start, request_end)

            title = f"Load {marker_id} {request['method']}"
            if request["request_range"]:
                title += f" [Range={request['request_range']}]"
            title += f": {request['url']}"
            name_index = len(string_array)
            string_array.append(title)

            start_payload = {
                "type": "Network",
                "URI": request["url"],
                "id": marker_id,
                "pri": 0,
                "status": "STATUS_START",
                "startTime": request_start,
                "endTime": request_start,
                "method": request["method"],
            }
            if request["request_range"]:
                start_payload["requestRange"] = request["request_range"]

            response_start = request_end
            if request["time_to_first_byte_ns"] >= 0:
                response_start = min(request_end, request_start + request["time_to_first_byte_ns"] / 1_000_000)
            stop_payload = {
                **start_payload,
                "status": "STATUS_STOP" if request["status_code"] > 0 else "STATUS_CANCEL",
                "startTime": request_start,
                "endTime": request_end,
                "requestStart": request_start,
                "responseStart": response_start,
                "responseEnd": request_end,
            }
            if request["status_code"] > 0:
                stop_payload["responseStatus"] = request["status_code"]
            if request["bytes_received"] > 0:
                stop_payload["count"] = request["bytes_received"]
            if request["response_content_range"]:
                stop_payload["responseContentRange"] = request["response_content_range"]

            _append_raw_marker(
                markers,
                category=category,
                data=start_payload,
                end_time=request_start,
                name=name_index,
                start_time=request_start,
            )
            _append_raw_marker(
                markers,
                category=category,
                data=stop_payload,
                end_time=request_end,
                name=name_index,
                start_time=request_start,
            )
            marker_id += 1
            added += 1
    return added


def inject_profile(raw_profile: Path, sidecar_directory: Path, output: Path) -> tuple[int, int, int]:
    profile = load_profile(raw_profile)
    added_counters = inject_counters(profile, sorted(sidecar_directory.glob("counter-*.txt")))
    added_http_requests = inject_http_requests(profile, sorted(sidecar_directory.glob("http-*.txt")))
    added_query_markers = inject_query_profiles(profile, sorted(sidecar_directory.glob("query-*.jsonl")))
    write_profile_atomic(profile, output)
    return added_counters, added_http_requests, added_query_markers


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
    return [
        *wrapper_arguments,
        "--",
        str(duckdb),
        "-cmd",
        "SET samply_tracks='all';",
        *command_arguments,
    ]


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

        added_counters, added_http_requests, added_query_markers = inject_profile(
            raw_profile, temporary_directory, output
        )
        print(
            f"Injected {added_counters} DuckDB resource counter(s) and "
            f"{added_http_requests} HTTP request(s), and {added_query_markers} query marker(s) into {output}",
            file=sys.stderr,
        )

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
