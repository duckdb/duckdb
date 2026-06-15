"""LLDB helpers for DuckDB smart pointers.

Usage inside LLDB:
    command script import /path/to/duckdb/scripts/lldb/duckdb_ptr.py

After import, LLDB will:
1. Pretty-print `duckdb::unique_ptr<T>` as if it were the pointee.
2. Keep a manual escape hatch:
       duckdb-deref some_unique_ptr_expression
       dderef some_unique_ptr_expression

The formatter path avoids calling `duckdb::unique_ptr<T>::operator*()` or `.get()`,
which makes it robust even when those template symbols are not emitted.
"""

from __future__ import annotations

import shlex


try:
    import lldb  # type: ignore
except ImportError:  # pragma: no cover - imported by LLDB at runtime
    lldb = None


CATEGORY_NAME = "duckdb"
COMMAND_NAME = "duckdb-deref"
COMMAND_ALIAS = "dderef"
UNIQUE_PTR_REGEX = r"^duckdb::unique_ptr<.+>$"


def __lldb_init_module(debugger, _internal_dict):
    debugger.HandleCommand(
        "command script add --overwrite -h "
        "\"Dereference DuckDB smart pointers without calling operator* or get()\" "
        f"-f duckdb_ptr.duckdb_deref {COMMAND_NAME}"
    )
    debugger.HandleCommand(f"command alias {COMMAND_ALIAS} {COMMAND_NAME}")

    debugger.HandleCommand(f"type category define {CATEGORY_NAME}")
    debugger.HandleCommand(
        f"type synthetic add --category {CATEGORY_NAME} "
        f"-x '{UNIQUE_PTR_REGEX}' --python-class duckdb_ptr.DuckDBUniquePtrSyntheticProvider"
    )
    debugger.HandleCommand(
        f"type summary add --category {CATEGORY_NAME} "
        f"-x '{UNIQUE_PTR_REGEX}' -F duckdb_ptr.duckdb_unique_ptr_summary"
    )
    debugger.HandleCommand(f"type category enable {CATEGORY_NAME}")


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

    pointee = _get_pointee_value(value)
    if pointee is None:
        result.SetError(f"could not extract a pointer from expression: {expression}")
        return

    description = _describe_value(pointee)
    if description:
        result.AppendMessage(description)
    else:
        result.AppendMessage(str(pointee))


def duckdb_unique_ptr_summary(valobj, _internal_dict):
    pointer_value = _get_pointer_value(valobj)
    if pointer_value is None or not pointer_value.IsValid():
        return "<unavailable>"

    address = pointer_value.GetValueAsUnsigned(0)
    if address == 0:
        return "nullptr"

    pointee_type = pointer_value.GetType().GetPointeeType()
    pointee_name = pointee_type.GetDisplayTypeName() or pointee_type.GetName() or "value"
    pointee_value = _dereference_pointer(pointer_value)
    pointee_summary = _inline_description(pointee_value)
    if pointee_summary:
        return f"{pointee_name} @ 0x{address:016x} {pointee_summary}"
    return f"{pointee_name} @ 0x{address:016x}"


class DuckDBUniquePtrSyntheticProvider:
    def __init__(self, valobj, _internal_dict):
        self.valobj = valobj
        self.pointer_value = None
        self.pointee_value = None
        self.update()

    def update(self):
        self.pointer_value = _get_pointer_value(self.valobj)
        self.pointee_value = _dereference_pointer(self.pointer_value)
        return False

    def has_children(self):
        return self.pointee_value is not None and self.pointee_value.IsValid()

    def num_children(self):
        if not self.has_children():
            return 0
        return self.pointee_value.GetNumChildren()

    def get_child_at_index(self, index):
        if not self.has_children():
            return None
        if index < 0 or index >= self.pointee_value.GetNumChildren():
            return None
        return self.pointee_value.GetChildAtIndex(index)

    def get_child_index(self, name):
        if not self.has_children():
            return -1
        for index in range(self.pointee_value.GetNumChildren()):
            child = self.pointee_value.GetChildAtIndex(index)
            if child.IsValid() and child.GetName() == name:
                return index
        return -1


def _get_pointee_value(value):
    pointer_value = _get_pointer_value(value)
    return _dereference_pointer(pointer_value)


def _get_pointer_value(value):
    current = value.GetNonSyntheticValue() if value and value.IsValid() else value
    while current and current.IsValid():
        type_obj = current.GetType()
        if type_obj.IsPointerType():
            return current

        type_name = type_obj.GetUnqualifiedType().GetName() or ""
        if "unique_ptr<" not in type_name:
            return None

        pointer_child = _find_pointer_descendant(current)
        if pointer_child is None or not pointer_child.IsValid():
            return None
        current = pointer_child
    return None


def _dereference_pointer(pointer_value):
    if pointer_value is None or not pointer_value.IsValid():
        return None
    if pointer_value.GetValueAsUnsigned(0) == 0:
        return None

    pointee = pointer_value.Dereference()
    error = pointee.GetError()
    if error.Fail():
        return None
    return pointee


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


def _find_pointer_descendant(value):
    if value is None or not value.IsValid():
        return None

    direct = _find_child_named(value, "pointer")
    if direct and direct.IsValid():
        return direct

    for i in range(value.GetNumChildren()):
        child = value.GetChildAtIndex(i)
        if not child.IsValid():
            continue
        child_type = child.GetType()
        if child_type.IsPointerType():
            return child
        nested = _find_pointer_descendant(child)
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


def _inline_description(value):
    if value is None or not value.IsValid():
        return None

    description = _describe_value(value)
    if not description:
        return None

    collapsed = " ".join(description.split())
    parts = collapsed.split(" = ", 1)
    if len(parts) == 2:
        return parts[1]
    return collapsed
