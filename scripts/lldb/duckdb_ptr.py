"""LLDB helpers for DuckDB smart pointers.

Usage inside LLDB:
    command script import /path/to/duckdb/scripts/lldb/duckdb_ptr.py

After import, LLDB will:
1. Pretty-print `duckdb::unique_ptr<T>` as if it were the pointee.

The formatter path avoids calling `duckdb::unique_ptr<T>::operator*()` or `.get()`,
which makes it robust even when those template symbols are not emitted.
"""

from __future__ import annotations

try:
    import lldb  # type: ignore
except ImportError:  # pragma: no cover - imported by LLDB at runtime
    lldb = None


CATEGORY_NAME = "duckdb"
UNIQUE_PTR_REGEX = r"^duckdb::unique_ptr<.+>$"


def __lldb_init_module(debugger, _internal_dict):
    debugger.HandleCommand(f"type category define {CATEGORY_NAME}")
    debugger.HandleCommand(
        f"type summary add --category {CATEGORY_NAME} "
        f"-x '{UNIQUE_PTR_REGEX}' -F duckdb_ptr.duckdb_unique_ptr_summary"
    )
    debugger.HandleCommand(f"type category enable {CATEGORY_NAME}")


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


def _find_pointer_descendant(value):
    if value is None or not value.IsValid():
        return None

    pending = [value]
    seen = set()
    scanned = 0

    while pending and scanned < 64:
        current = pending.pop()
        if current is None or not current.IsValid():
            continue

        key = _value_key(current)
        if key in seen:
            continue
        seen.add(key)
        scanned += 1

        for field_name in ("pointer", "__ptr_"):
            direct = current.GetChildMemberWithName(field_name)
            if direct and direct.IsValid():
                return direct

        child_count = current.GetNumChildren()
        for i in range(child_count):
            child = current.GetChildAtIndex(i)
            if not child.IsValid():
                continue
            child_type = child.GetType()
            if child_type.IsPointerType():
                return child
            pending.append(child)
    return None


def _value_key(value):
    value_id = getattr(value, "GetID", None)
    if callable(value_id):
        return ("id", value_id())

    return (
        value.GetName(),
        value.GetType().GetName() if value.GetType().IsValid() else None,
        value.GetValue(),
        value.GetValueAsUnsigned(0),
        value.GetNumChildren(),
    )


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
