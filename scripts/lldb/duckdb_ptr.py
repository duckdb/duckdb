"""LLDB helpers for inspecting DuckDB smart pointers without calling inline methods.

Usage inside LLDB:
    command script import /path/to/duckdb/tools/lldb/duckdb_ptr.py
    duckdb-deref this->children.arena.arena_allocator.private_data->debug_info

This is useful when LLDB cannot call `duckdb::unique_ptr<T>::operator*()` or `.get()`
because the relevant template instantiation was optimized out or never emitted.
"""

from __future__ import annotations

import shlex


try:
    import lldb  # type: ignore
except ImportError:  # pragma: no cover - imported by LLDB at runtime
    lldb = None


COMMAND_NAME = "duckdb-deref"
COMMAND_ALIAS = "dderef"


def __lldb_init_module(debugger, _internal_dict):
    debugger.HandleCommand(
        "command script add --overwrite -h "
        "\"Dereference DuckDB smart pointers without calling operator* or get()\" "
        f"-f duckdb_ptr.duckdb_deref {COMMAND_NAME}"
    )
    debugger.HandleCommand(f"command alias {COMMAND_ALIAS} {COMMAND_NAME}")


def duckdb_deref(debugger, command, result, _internal_dict):
    tokens = shlex.split(command)
    if not tokens or "--help" in tokens or "-h" in tokens:
        result.AppendMessage(
            "usage: {} <expression>\n"
            "example: {} this->children.arena.arena_allocator.private_data->debug_info".format(
                COMMAND_NAME, COMMAND_NAME
            )
        )
        return

    expression = command.strip()
    target = debugger.GetSelectedTarget()
    process = target.GetProcess() if target.IsValid() else None
    thread = process.GetSelectedThread() if process and process.IsValid() else None
    frame = thread.GetSelectedFrame() if thread and thread.IsValid() else None
    if frame is None or not frame.IsValid():
        result.SetError("no selected frame")
        return

    options = lldb.SBExpressionOptions()
    options.SetTryAllThreads(False)
    value = frame.EvaluateExpression(expression, options)
    error = value.GetError()
    if error.Fail():
        result.SetError(error.GetCString())
        return

    pointer_value = _unwrap_to_pointer(value)
    if pointer_value is None or not pointer_value.IsValid():
        result.SetError(f"could not extract a pointer from expression: {expression}")
        return
    if pointer_value.GetValueAsUnsigned(0) == 0:
        result.AppendMessage("nullptr")
        return

    deref_value = pointer_value.Dereference()
    deref_error = deref_value.GetError()
    if deref_error.Fail():
        result.SetError(deref_error.GetCString())
        return

    description = _describe_value(deref_value)
    if description:
        result.AppendMessage(description)
    else:
        result.AppendMessage(str(deref_value))


def _unwrap_to_pointer(value):
    current = value
    while current and current.IsValid():
        type_obj = current.GetType()
        if type_obj.IsPointerType():
            return current

        type_name = type_obj.GetUnqualifiedType().GetName() or ""
        if "unique_ptr<" not in type_name:
            return None

        pointer_child = _find_child_named(current, "pointer")
        if pointer_child is None or not pointer_child.IsValid():
            return None
        current = pointer_child
    return None


def _find_child_named(value, name):
    direct = value.GetChildMemberWithName(name)
    if direct and direct.IsValid():
        return direct

    for i in range(value.GetNumChildren()):
        child = value.GetChildAtIndex(i)
        if not child.IsValid():
            continue
        if child.GetName() == name:
            return child
        nested = _find_child_named(child, name)
        if nested and nested.IsValid():
            return nested
    return None


def _describe_value(value):
    stream = lldb.SBStream()
    if value.GetDescription(stream):
        return stream.GetData()

    stream = lldb.SBStream()
    value.GetData().GetDescription(stream, value.GetTarget())
    return stream.GetData()
