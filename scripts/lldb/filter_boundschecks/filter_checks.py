"""LLDB helpers for stepping over tiny checked wrapper/container frames.

Usage inside LLDB:
    command script import /path/to/duckdb/scripts/lldb/duckdb_step_avoid.py

Importing the script automatically augments `target.process.thread.step-avoid-regexp`
for the current LLDB session so `step`/`thread step-in` skip small helper frames such as:

    duckdb::optional_ptr<...>::operator*
    duckdb::unique_ptr<...>::operator->
    duckdb::shared_ptr<...>::operator*
    duckdb::vector<...>::operator[]

Commands:
    duckdb-step-avoid-enable
    duckdb-step-avoid-disable
    duckdb-step-avoid-show
"""

from __future__ import annotations


try:
    import lldb  # type: ignore
except ImportError:  # pragma: no cover - imported by LLDB at runtime
    lldb = None


ENABLE_COMMAND = "duckdb-step-avoid-enable"
DISABLE_COMMAND = "duckdb-step-avoid-disable"
SHOW_COMMAND = "duckdb-step-avoid-show"

_saved_step_avoid_regex = None


DUCKDB_STEP_AVOID_PATTERNS = (
    r"^duckdb::optional_ptr<.*>::(optional_ptr|CheckValid|get(_mutable)?|operator\*|operator->|operator bool)$",
    r"^duckdb::unique_ptr<.*>::(unique_ptr|AssertNotNull|operator\*|operator->|operator\[\])$",
    r"^duckdb::shared_ptr<.*>::(shared_ptr|AssertNotNull|get|use_count|operator\*|operator->|operator bool)$",
    r"^duckdb::vector<.*>::(vector|AssertIndexInBounds|get|operator\[\]|front|back)$",
)

DUCKDB_STEP_AVOID_REGEX = "|".join("({})".format(pattern) for pattern in DUCKDB_STEP_AVOID_PATTERNS)


def __lldb_init_module(debugger, _internal_dict):
    debugger.HandleCommand(
        "command script add --overwrite -h "
        "\"Enable DuckDB-specific step-avoid rules for checked wrappers\" "
        "-f duckdb_step_avoid.enable {}".format(ENABLE_COMMAND)
    )
    debugger.HandleCommand(
        "command script add --overwrite -h "
        "\"Disable DuckDB-specific step-avoid rules and restore the previous regexp\" "
        "-f duckdb_step_avoid.disable {}".format(DISABLE_COMMAND)
    )
    debugger.HandleCommand(
        "command script add --overwrite -h "
        "\"Show the current step-avoid regexp and the DuckDB additions\" "
        "-f duckdb_step_avoid.show {}".format(SHOW_COMMAND)
    )

    message = _enable(debugger)
    print(message)


def enable(debugger, _command, result, _internal_dict):
    result.AppendMessage(_enable(debugger))


def disable(debugger, _command, result, _internal_dict):
    global _saved_step_avoid_regex

    if _saved_step_avoid_regex is None:
        result.AppendMessage("duckdb step-avoid rules are not enabled")
        return

    _set_step_avoid_regex(debugger, _saved_step_avoid_regex)
    restored_regex = _saved_step_avoid_regex
    _saved_step_avoid_regex = None
    result.AppendMessage("restored target.process.thread.step-avoid-regexp to {}".format(repr(restored_regex)))


def show(debugger, _command, result, _internal_dict):
    current_regex = _get_step_avoid_regex(debugger)
    result.AppendMessage("current target.process.thread.step-avoid-regexp = {}".format(repr(current_regex)))
    result.AppendMessage("DuckDB additions = {}".format(repr(DUCKDB_STEP_AVOID_REGEX)))


def _enable(debugger):
    global _saved_step_avoid_regex

    current_regex = _get_step_avoid_regex(debugger)
    if _has_duckdb_patterns(current_regex):
        return "duckdb step-avoid rules already enabled: {}".format(repr(current_regex))

    if _saved_step_avoid_regex is None:
        _saved_step_avoid_regex = current_regex

    combined_regex = _combine_regex(current_regex, DUCKDB_STEP_AVOID_REGEX)
    _set_step_avoid_regex(debugger, combined_regex)
    return "duckdb step-avoid rules enabled: {}".format(repr(combined_regex))


def _combine_regex(existing_regex, extra_regex):
    if not existing_regex:
        return extra_regex
    return "({})|({})".format(existing_regex, extra_regex)


def _has_duckdb_patterns(current_regex):
    if not current_regex:
        return False
    return "duckdb::optional_ptr<" in current_regex and "duckdb::vector<" in current_regex


def _get_step_avoid_regex(debugger):
    values = debugger.GetInternalVariableValue("target.process.thread.step-avoid-regexp", debugger.GetInstanceName())
    if values.GetSize() == 0:
        return ""
    return values.GetStringAtIndex(0)


def _set_step_avoid_regex(debugger, regex):
    error = debugger.SetInternalVariable("target.process.thread.step-avoid-regexp", regex, debugger.GetInstanceName())
    if error is not None and not error.Success():
        raise RuntimeError(error.GetCString() or "failed to set step-avoid regexp")
