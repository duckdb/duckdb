import argparse
import logging
import os
import pathlib
import re
from types import NoneType
from typing import Dict, List, NotRequired, TypedDict, Union

from generate_c_api import (
    EXT_API_DEFINITION_PATTERN,
    get_extension_api_version,
    parse_capi_function_definitions,
    parse_ext_api_definitions,
)


class FunctionDefParam(TypedDict):
    type: str
    name: str


class FunctionDefComment(TypedDict):
    description: str
    param_comments: dict[str, str]
    return_value: str


class FunctionDef(TypedDict):
    name: str
    group: str
    deprecated: bool
    group_deprecated: bool
    return_type: str
    params: list[FunctionDefParam]
    comment: FunctionDefComment


class FunctionGroup(TypedDict):
    group: str
    deprecated: bool
    entries: list[FunctionDef]


class DuckDBApiInfo(TypedDict):
    version: str
    commit: NotRequired[str]


def parse_c_type(type_str: str, type: list[str] = []):
    """Parses simple C types (no function pointer or array types) and returns a list of the type components.

    Args:
        type_str: A C type string to parse, e.g.: "const char* const"
        type: List to track components, used for recursion. Defaults to [].

    Returns:
        list: A list of the type components, e.g.: "const char* const" -> ["Const Ptr", "const char"]
    """
    type_str = type_str.strip()
    ptr_pattern = r"^(.*)\*(\s*const\s*)?$"

    if (m1 := re.match(ptr_pattern, type_str)) is not None:
        before_ptr = m1.group(1)
        is_const = bool(m1.group(2))
        type.append("Const Ptr" if is_const else "Ptr")
        return parse_c_type(before_ptr, type)

    type.append(type_str)
    return type


JULIA_RESERVED_KEYWORDS = {
    "function",
    "if",
    "else",
    "while",
    "for",
    "try",
    "catch",
    "finally",
    "return",
    "break",
    "continue",
    "end",
    "begin",
    "quote",
    "let",
    "local",
    "global",
    "const",
    "do",
    "struct",
    "mutable",
    "abstract",
    "type",
    "module",
    "using",
    "import",
    "export",
    "public",
}

JULIA_BASE_TYPE_MAP = {
    # Julia Standard Types
    "char": "Char",
    "int": "Int",
    "int8_t": "Int8",
    "int16_t": "Int16",
    "int32_t": "Int32",
    "int64_t": "Int64",
    "uint8_t": "UInt8",
    "uint16_t": "UInt16",
    "uint32_t": "UInt32",
    "uint64_t": "UInt64",
    "double": "Float64",
    "float": "Float32",
    "bool": "Bool",
    "void": "Cvoid",
    "size_t": "Csize_t",
    # DuckDB specific types
    "idx_t": "idx_t",
    "duckdb_type": "DUCKDB_TYPE",
    "duckdb_string_t": "duckdb_string_t",  # INLINE prefix with pointer string type
    "duckdb_string": "duckdb_string",  # Pointer + size type
    "duckdb_table_function": "duckdb_table_function",  # actually struct pointer
    "duckdb_table_function_t": "duckdb_table_function_ptr",  # function pointer type
    "duckdb_cast_function": "duckdb_cast_function",  # actually struct pointer
    "duckdb_cast_function_t": "duckdb_cast_function_ptr",  # function pointer type
}


# TODO this the original order of the functions in `api.jl` and is only used to keep the PR review small
JULIA_API_ORIGINAL_ORDER = [
    "duckdb_open",
    "duckdb_open_ext",
    "duckdb_close",
    "duckdb_connect",
    "duckdb_disconnect",
    "duckdb_create_config",
    "duckdb_config_count",
    "duckdb_get_config_flag",
    "duckdb_set_config",
    "duckdb_destroy_config",
    "duckdb_query",
    "duckdb_destroy_result",
    "duckdb_column_name",
    "duckdb_column_type",
    "duckdb_column_logical_type",
    "duckdb_column_count",
    "duckdb_row_count",
    "duckdb_rows_changed",
    "duckdb_column_data",
    "duckdb_nullmask_data",
    "duckdb_result_error",
    "duckdb_result_get_chunk",
    "duckdb_result_is_streaming",
    "duckdb_stream_fetch_chunk",
    "duckdb_result_chunk_count",
    "duckdb_value_boolean",
    "duckdb_value_int8",
    "duckdb_value_int16",
    "duckdb_value_int32",
    "duckdb_value_int64",
    "duckdb_value_hugeint",
    "duckdb_value_uhugeint",
    "duckdb_value_uint8",
    "duckdb_value_uint16",
    "duckdb_value_uint32",
    "duckdb_value_uint64",
    "duckdb_value_float",
    "duckdb_value_double",
    "duckdb_value_date",
    "duckdb_value_time",
    "duckdb_value_timestamp",
    "duckdb_value_interval",
    "duckdb_value_varchar",
    "duckdb_value_varchar_internal",
    "duckdb_value_is_null",
    "duckdb_malloc",
    "duckdb_free",
    "duckdb_vector_size",
    "duckdb_from_time_tz",
    "duckdb_prepare",
    "duckdb_destroy_prepare",
    "duckdb_prepare_error",
    "duckdb_nparams",
    "duckdb_param_type",
    "duckdb_bind_boolean",
    "duckdb_bind_int8",
    "duckdb_bind_int16",
    "duckdb_bind_int32",
    "duckdb_bind_int64",
    "duckdb_bind_hugeint",
    "duckdb_bind_uhugeint",
    "duckdb_bind_uint8",
    "duckdb_bind_uint16",
    "duckdb_bind_uint32",
    "duckdb_bind_uint64",
    "duckdb_bind_float",
    "duckdb_bind_double",
    "duckdb_bind_date",
    "duckdb_bind_time",
    "duckdb_bind_timestamp",
    "duckdb_bind_interval",
    "duckdb_bind_varchar",
    "duckdb_bind_varchar_length",
    "duckdb_bind_blob",
    "duckdb_bind_null",
    "duckdb_execute_prepared",
    "duckdb_pending_prepared",
    "duckdb_pending_prepared_streaming",
    "duckdb_pending_execute_check_state",
    "duckdb_destroy_pending",
    "duckdb_pending_error",
    "duckdb_pending_execute_task",
    "duckdb_execute_pending",
    "duckdb_pending_execution_is_finished",
    "duckdb_destroy_value",
    "duckdb_create_varchar",
    "duckdb_create_varchar_length",
    "duckdb_create_int64",
    "duckdb_get_varchar",
    "duckdb_get_int64",
    "duckdb_create_logical_type",
    "duckdb_create_decimal_type",
    "duckdb_get_type_id",
    "duckdb_decimal_width",
    "duckdb_decimal_scale",
    "duckdb_decimal_internal_type",
    "duckdb_enum_internal_type",
    "duckdb_enum_dictionary_size",
    "duckdb_enum_dictionary_value",
    "duckdb_list_type_child_type",
    "duckdb_struct_type_child_count",
    "duckdb_union_type_member_count",
    "duckdb_struct_type_child_name",
    "duckdb_union_type_member_name",
    "duckdb_struct_type_child_type",
    "duckdb_union_type_member_type",
    "duckdb_destroy_logical_type",
    "duckdb_create_data_chunk",
    "duckdb_destroy_data_chunk",
    "duckdb_data_chunk_reset",
    "duckdb_data_chunk_get_column_count",
    "duckdb_data_chunk_get_size",
    "duckdb_data_chunk_set_size",
    "duckdb_data_chunk_get_vector",
    "duckdb_vector_get_column_type",
    "duckdb_vector_get_data",
    "duckdb_vector_get_validity",
    "duckdb_vector_ensure_validity_writable",
    "duckdb_list_vector_get_child",
    "duckdb_list_vector_get_size",
    "duckdb_struct_vector_get_child",
    "duckdb_union_vector_get_member",
    "duckdb_vector_assign_string_element",
    "duckdb_vector_assign_string_element_len",
    "duckdb_create_table_function",
    "duckdb_destroy_table_function",
    "duckdb_table_function_set_name",
    "duckdb_table_function_add_parameter",
    "duckdb_table_function_set_extra_info",
    "duckdb_table_function_set_bind",
    "duckdb_table_function_set_init",
    "duckdb_table_function_set_local_init",
    "duckdb_table_function_set_function",
    "duckdb_table_function_supports_projection_pushdown",
    "duckdb_register_table_function",
    "duckdb_bind_get_extra_info",
    "duckdb_bind_add_result_column",
    "duckdb_bind_get_parameter_count",
    "duckdb_bind_get_parameter",
    "duckdb_bind_set_bind_data",
    "duckdb_bind_set_cardinality",
    "duckdb_bind_set_error",
    "duckdb_init_get_extra_info",
    "duckdb_init_get_bind_data",
    "duckdb_init_set_init_data",
    "duckdb_init_get_column_count",
    "duckdb_init_get_column_index",
    "duckdb_init_set_max_threads",
    "duckdb_init_set_error",
    "duckdb_function_get_extra_info",
    "duckdb_function_get_bind_data",
    "duckdb_function_get_init_data",
    "duckdb_function_get_local_init_data",
    "duckdb_function_set_error",
    "duckdb_add_replacement_scan",
    "duckdb_replacement_scan_set_function_name",
    "duckdb_replacement_scan_add_parameter",
    "duckdb_replacement_scan_set_error",
    "duckdb_appender_create",
    "duckdb_appender_error",
    "duckdb_appender_flush",
    "duckdb_appender_close",
    "duckdb_appender_destroy",
    "duckdb_appender_begin_row",
    "duckdb_appender_end_row",
    "duckdb_append_bool",
    "duckdb_append_int8",
    "duckdb_append_int16",
    "duckdb_append_int32",
    "duckdb_append_int64",
    "duckdb_append_hugeint",
    "duckdb_append_uhugeint",
    "duckdb_append_uint8",
    "duckdb_append_uint16",
    "duckdb_append_uint32",
    "duckdb_append_uint64",
    "duckdb_append_float",
    "duckdb_append_double",
    "duckdb_append_date",
    "duckdb_append_time",
    "duckdb_append_timestamp",
    "duckdb_append_interval",
    "duckdb_append_varchar",
    "duckdb_append_varchar_length",
    "duckdb_append_blob",
    "duckdb_append_null",
    "duckdb_execute_tasks",
    "duckdb_create_task_state",
    "duckdb_execute_tasks_state",
    "duckdb_execute_n_tasks_state",
    "duckdb_finish_execution",
    "duckdb_task_state_is_finished",
    "duckdb_destroy_task_state",
    "duckdb_execution_is_finished",
    "duckdb_create_scalar_function",
    "duckdb_destroy_scalar_function",
    "duckdb_scalar_function_set_name",
    "duckdb_scalar_function_add_parameter",
    "duckdb_scalar_function_set_return_type",
    "duckdb_scalar_function_set_function",
    "duckdb_register_scalar_function",
]


class JuliaApiTarget:
    indent: int = 0
    linesep: str = os.linesep
    type_maps: dict[str, str] = {}  # C to Julia
    inverse_type_maps: dict[str, list[str]] = {}  # Julia to C
    deprecated_functions: list[str] = []
    type_map: dict[str, str]

    # Functions to skip
    skipped_functions = set()
    skip_deprecated_functions = False

    # Explicit function order
    manual_order: Union[List[str], NoneType] = None

    overwrite_function_signatures = {}

    # Functions that use indices either as ARG or RETURN and should be converted to 1-based indexing
    auto_1base_index: bool
    auto_1base_index_return_functions = set()
    auto_1base_index_ignore_functions = set()

    def __init__(
        self,
        file,
        indent=0,
        auto_1base_index=True,
        auto_1base_index_return_functions=set(),
        auto_1base_index_ignore_functions=set(),
        skipped_functions=set(),
        skip_deprecated_functions=False,
        type_map={},
        overwrite_function_signatures={},
    ):
        # check if file is a string or a file object
        if isinstance(file, str) or isinstance(file, pathlib.Path):
            self.filename = pathlib.Path(file)
        else:
            raise ValueError("file must be a string or a path object")
        self.indent = indent
        self.auto_1base_index = auto_1base_index
        self.auto_1base_index_return_functions = auto_1base_index_return_functions
        self.auto_1base_index_ignore_functions = auto_1base_index_ignore_functions
        self.linesep = os.linesep
        self.type_map = type_map
        self.skipped_functions = skipped_functions
        self.skip_deprecated_functions = skip_deprecated_functions
        self.overwrite_function_signatures = overwrite_function_signatures
        super().__init__()

    def __enter__(self):
        self.file = open(self.filename, "w")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.file.close()

    def write_empty_line(self, n=1) -> None:
        """Writes an empty line to the output file."""
        for i in range(n):
            self.file.write(self.linesep)

    def _get_casted_type(self, type_str: str, is_return_arg=False, auto_remove_t_suffix=True):
        type_str = type_str.strip()
        type_definition = parse_c_type(type_str, [])

        def reduce_type(type_list: list[str]):
            if len(type_list) == 0:
                return ""

            t = type_list[0]
            if len(type_list) == 1:
                is_const = False  #  Track that the type is const, even though we cannot use it in Julia
                if t.startswith("const "):
                    t, is_const = t.removeprefix("const "), True

                if t in self.type_map:
                    return self.type_map[t]
                else:
                    if auto_remove_t_suffix and t.endswith("_t"):
                        t = t.removesuffix("_t")
                    if " " in t:
                        raise (ValueError(f"Unknown type: {t}"))
                    return t

            # Handle Pointer types
            if t not in ("Ptr", "Const Ptr"):
                raise ValueError(f"Unexpected non-pointer type: {t}")

            if len(type_list) >= 2 and type_list[1].strip() in (
                "char",
                "const char",
            ):
                return "Cstring"
            else:
                if is_return_arg:
                    # Use Ptr for return types, because they are not tracked by the Julia GC
                    return "Ptr{" + reduce_type(type_list[1:]) + "}"
                else:
                    # Prefer Ref over Ptr for arguments
                    return "Ref{" + reduce_type(type_list[1:]) + "}"

        return reduce_type(type_definition)

    def _is_index_argument(self, name: str, function_obj: FunctionDef):
        # Check if the argument is (likely) an index
        if name not in (
            "index",
            "idx",
            "i",
            "row",
            "col",
            "column",
            "col_idx",
            "column_idx",
            "column_index",
            "row_idx",
            "row_index",
            "chunk_index",
            # "param_idx", # TODO creates errors in bind_param
        ):
            return False

        x = None
        for param in function_obj["params"]:
            if param["name"] == name:
                x = param
                break

        arg_type = self._get_casted_type(x["type"])
        if arg_type not in (
            "Int",
            "Int64",
            "UInt",
            "UInt64",
            "idx_t",
            "idx" "Int32",
            "UInt32",
            "Csize_t",
        ):
            return False

        return True

    def get_argument_names_and_types(self, function_obj: FunctionDef):
        def _get_arg_name(name: str):
            if name in JULIA_RESERVED_KEYWORDS:
                return f"_{name}"
            return name

        arg_names = [_get_arg_name(param["name"]) for param in function_obj["params"]]

        if function_obj["name"] in self.overwrite_function_signatures:
            return_type, arg_types = self.overwrite_function_signatures[function_obj["name"]]
            return arg_names, arg_types

        arg_types = [self._get_casted_type(param["type"]) for param in function_obj["params"]]
        return arg_names, arg_types

    def is_index1_function(self, function_obj: FunctionDef):
        fname = function_obj["name"]

        if not self.auto_1base_index:
            return [False for param in function_obj["params"]], False

        if fname in self.auto_1base_index_ignore_functions:
            return [False for param in function_obj["params"]], False

        is_index1_return = fname in self.auto_1base_index_return_functions
        is_index1_arg = [self._is_index_argument(param["name"], function_obj) for param in function_obj["params"]]
        return is_index1_arg, is_index1_return

    def _write_function_docstring(self, function_obj: FunctionDef):
        r"""_create_function_docstring


        Example:
        ```julia
        \"\"\"
            duckdb_get_int64(value)

        Obtains an int64 of the given value.

        # Arguments
        - `value`: The value

        Returns: The int64 value, or 0 if no conversion is possible
        \"\"\"
        ```

        Args:
            function_obj: _description_
        """

        description = function_obj.get("comment", {}).get("description", "").strip()
        description = description.replace('"', '\\"')  # escape double quotes

        index1_args, index1_return = self.is_index1_function(function_obj)

        # Arguments
        arg_names, arg_types = self.get_argument_names_and_types(function_obj)

        arg_comments = []
        for ix, (name, param, t, is_index1) in enumerate(
            zip(arg_names, function_obj["params"], arg_types, index1_args)
        ):
            param_comment = function_obj.get("comment", {}).get("param_comments", {}).get(param["name"], "")
            if is_index1:
                parts = [f"`{name}`:", f"`{t}`", "(1-based index)", param_comment]
            else:
                parts = [f"`{name}`:", f"`{t}`", param_comment]
            arg_comments.append(" ".join(parts))

        arg_names_s = ", ".join(arg_names)

        # Return Values
        return_type = self._get_casted_type(function_obj["return_type"], is_return_arg=True)
        if return_type == "Cvoid":
            return_type = "Nothing"  # Cvoid is equivalent to Nothing in Julia
        return_comments = [
            f"`{return_type}`",
            function_obj.get("comment", {}).get("return_value", ""),
        ]
        if index1_return:
            return_comments.append("(1-based index)")
        return_value_comment = " ".join(return_comments)

        self.file.write(f"{'    ' * self.indent}\"\"\"\n")
        self.file.write(f"{'    ' * self.indent}    {function_obj['name']}({arg_names_s})\n")
        self.file.write(f"{'    ' * self.indent}\n")
        self.file.write(f"{'    ' * self.indent}{description}\n")
        self.file.write(f"{'    ' * self.indent}\n")
        self.file.write(f"{'    ' * self.indent}# Arguments\n")
        for i, arg_name in enumerate(arg_names):
            self.file.write(f"{'    ' * self.indent}- {arg_comments[i]}\n")
        self.file.write(f"{'    ' * self.indent}\n")
        self.file.write(f"{'    ' * self.indent}Returns: {return_value_comment}\n")
        self.file.write(f"{'    ' * self.indent}\"\"\"\n")

    def _get_depwarning_message(self, function_obj: FunctionDef):
        description = function_obj.get("comment", {}).get("description", "")
        if not description.startswith("**DEPRECATION NOTICE**:"):
            description = f"**DEPRECATION NOTICE**: {description}"

        # Only use the first line of the description
        notice = description.split("\n")[0]
        notice = notice.replace("\n", " ").replace('"', '\\"').strip()
        return notice

    def _write_function_depwarn(self, function_obj: FunctionDef, indent: int = 0):
        """
        Writes a deprecation warning for a function.

        Example:
        ```julia
            Base.depwarn(
            "The `G` type parameter will be deprecated in a future release. " *
            "Please use `MyType(args...)` instead of `MyType{$G}(args...)`.",
            :MyType,
            )
        ```
        """
        indent = self.indent + indent  # total indent

        notice = self._get_depwarning_message(function_obj)

        self.file.write(f"{'    ' * indent}Base.depwarn(\n")
        self.file.write(f"{'    ' * indent}  \"{notice}\",\n")
        self.file.write(f"{'    ' * indent}    :{function_obj['name']},\n")
        self.file.write(f"{'    ' * indent})\n")

    def _list_to_julia_tuple(self, lst):
        if len(lst) == 0:
            return "()"
        elif len(lst) == 1:
            return f"({lst[0]},)"
        else:
            return f"({', '.join(lst)})"

    def _write_function_definition(self, function_obj: FunctionDef):
        fname = function_obj["name"]
        index1_args, index1_return = self.is_index1_function(function_obj)

        arg_names, arg_types = self.get_argument_names_and_types(function_obj)
        arg_types_tuple = self._list_to_julia_tuple(arg_types)
        arg_names_definition = ", ".join(arg_names)

        arg_names_call = []
        for arg_name, is_index1 in zip(arg_names, index1_args):
            if is_index1:
                arg_names_call.append(f"{arg_name} - 1")
            else:
                arg_names_call.append(arg_name)
        arg_names_call = ", ".join(arg_names_call)

        return_type = self._get_casted_type(function_obj["return_type"], is_return_arg=True)

        self.file.write(f"{'    ' * self.indent}function {fname}({arg_names_definition})\n")

        if function_obj.get("group_deprecated", False) or function_obj.get("deprecated", False):
            self._write_function_depwarn(function_obj, indent=1)

        self.file.write(
            f"{'    ' * self.indent}    return ccall((:{fname}, libduckdb), {return_type}, {arg_types_tuple}, {arg_names_call}){' + 1' if index1_return else ''}\n"
        )
        self.file.write(f"{'    ' * self.indent}end\n")

    def write_function(self, function_obj: FunctionDef):
        if function_obj["name"] in self.skipped_functions:
            return

        if function_obj.get("group_deprecated", False) or function_obj.get("deprecated", False):
            self.deprecated_functions.append(function_obj["name"])

        self._write_function_docstring(function_obj)
        self._write_function_definition(function_obj)

    def write_footer(self):
        self.write_empty_line(n=1)
        s = """
# !!!!!!!!!!!!
# WARNING: this file is autogenerated by scripts/generate_c_api_julia.py, manual changes will be overwritten
# !!!!!!!!!!!!
"""
        self.file.write(s)
        self.write_empty_line()

    def write_header(self, version=""):
        s = """
###############################################################################
# 
# DuckDB Julia API
# 
# !!!!!!!!!!!!
# WARNING: this file is autogenerated by scripts/generate_c_api_julia.py, manual changes will be overwritten
# !!!!!!!!!!!!
#
###############################################################################

using Base.Libc

if "JULIA_DUCKDB_LIBRARY" in keys(ENV)
    libduckdb = ENV["JULIA_DUCKDB_LIBRARY"]
else
    using DuckDB_jll
end
"""
        if version[0] == "v":
            # remove the v prefix and use Julia Version String
            version = version[1:]

        self.file.write(s)
        self.file.write("\n")
        self.file.write(f'DUCKDB_API_VERSION = v"{version}"\n')
        self.file.write("\n")

    def write_functions(
        self,
        version,
        function_groups: List[FunctionGroup],
        function_map: Dict[str, FunctionDef],
    ):
        self._analyze_types(function_groups)  # Create the julia type map
        self.write_header(version)
        self.write_empty_line()
        if self.manual_order is not None:
            current_group = None
            for f in self.manual_order:
                if f not in function_map:
                    print(f"WARNING: Function {f} not found in function_map")
                    continue

                if current_group != function_map[f]["group"]:
                    current_group = function_map[f]["group"]
                    self.write_group_start(current_group)
                    self.write_empty_line()

                self.write_function(function_map[f])
                self.write_empty_line()

            # Write new functions
            self.write_empty_line(n=1)
            self.write_group_start("New Functions")
            self.write_empty_line(n=2)
            current_group = None
            for group in function_groups:
                for fn in group["entries"]:
                    if fn["name"] in self.manual_order:
                        continue
                    if current_group != group["group"]:
                        current_group = group["group"]
                        self.write_group_start(current_group)
                        self.write_empty_line()

                    self.write_function(fn)
                    self.write_empty_line()

        else:
            for group in function_groups:
                self.write_group_start(group["group"])
                self.write_empty_line()
                for fn in group["entries"]:
                    self.write_function(fn)
                    self.write_empty_line()
                self.write_empty_line()
                self.write_empty_line()

        self.write_footer()

    def _analyze_types(self, groups: List[FunctionGroup]):
        for group in groups:
            for fn in group["entries"]:
                for param in fn["params"]:
                    if param["type"] not in self.type_maps:
                        self.type_maps[param["type"]] = self._get_casted_type(param["type"])
                if fn["return_type"] not in self.type_maps:
                    self.type_maps[fn["return_type"]] = self._get_casted_type(fn["return_type"])

        for k, v in self.type_maps.items():
            if v not in self.inverse_type_maps:
                self.inverse_type_maps[v] = []
            self.inverse_type_maps[v].append(k)
        return

    def write_group_start(self, group):
        group = group.replace("_", " ").strip()
        # make group title uppercase
        group = " ".join([x.capitalize() for x in group.split(" ")])
        self.file.write(f"# {'-' * 80}\n")
        self.file.write(f"# {group}\n")
        self.file.write(f"# {'-' * 80}\n")

    @staticmethod
    def get_function_order(filepath):
        path = pathlib.Path(filepath)
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"File {path} does not exist")

        with open(path, "r") as f:
            lines = f.readlines()

        is_julia_file = path.suffix == ".jl"

        if not is_julia_file:
            # read the file and assume that we have a function name per line
            return [x.strip() for x in lines if x.strip() != ""]

        # find the function definitions
        # TODO this a very simple regex that only supports the long function form `function name(...)`
        function_regex = r"^function\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\("
        function_order = []
        for line in lines:
            line = line.strip()
            if line.startswith("#"):
                continue

            m = re.match(function_regex, line)
            if m is not None:
                function_order.append(m.group(1))
        return function_order


def main():
    """Main function to generate the Julia API."""

    print("Creating Julia API")

    parser = configure_parser()
    args = parser.parse_args()
    print("Arguments:")
    for k, v in vars(args).items():
        print(f"    {k}: {v}")

    julia_path = pathlib.Path(args.output)
    enable_auto_1base_index = args.auto_1_index
    enable_original_order = args.use_original_order

    capi_defintions_dir = pathlib.Path(args.capi_dir)
    ext_api_definition_pattern = str(capi_defintions_dir) + "/apis/v1/*/*.json"
    capi_function_definition_pattern = str(capi_defintions_dir) + "/functions/**/*.json"
    ext_api_definitions = parse_ext_api_definitions(ext_api_definition_pattern)
    ext_api_version = get_extension_api_version(ext_api_definitions)
    function_groups, function_map = parse_capi_function_definitions(capi_function_definition_pattern)

    overwrite_function_signatures = {
        # Must be Ptr{Cvoid} and not Ref
        "duckdb_free": (
            "Cvoid",
            ("Ptr{Cvoid}",),
        ),
        "duckdb_bind_blob": (
            "duckdb_state",
            ("duckdb_prepared_statement", "idx_t", "Ptr{Cvoid}", "idx_t"),
        ),
        "duckdb_vector_assign_string_element_len": (
            "Cvoid",
            (
                "duckdb_vector",
                "idx_t",
                "Ptr{UInt8}",
                "idx_t",
            ),  # Must be Ptr{UInt8} instead of Cstring to allow '\0' in the middle
        ),
    }

    with JuliaApiTarget(
        julia_path,
        indent=0,
        auto_1base_index=enable_auto_1base_index,  # WARNING: every arg named "col/row/index" or similar will be 1-based indexed, so the argument is subtracted by 1
        auto_1base_index_return_functions={"duckdb_init_get_column_index"},
        auto_1base_index_ignore_functions={
            "duckdb_parameter_name",  # Parameter names start at 1
            "duckdb_param_type",  # Parameter types (like names) start at 1
            "duckdb_param_logical_type",  # ...
            "duckdb_bind_get_parameter",  # Would be breaking API change
        },
        skipped_functions={},
        type_map=JULIA_BASE_TYPE_MAP,
        overwrite_function_signatures=overwrite_function_signatures,
    ) as printer:
        if enable_original_order:
            print("INFO: Using the original order of the functions from the old API file.")
            printer.manual_order = JULIA_API_ORIGINAL_ORDER

        printer.write_functions(ext_api_version, function_groups, function_map)

        if args.print_type_mapping:
            print("Type maps: (Julia Type -> C Type)")
            K = list(printer.inverse_type_maps.keys())
            K.sort()
            for k in K:
                if k.startswith("Ptr") or k.startswith("Ref"):
                    continue
                v = ", ".join(printer.inverse_type_maps[k])
                print(f"    {k} -> {v}")

        print("Julia API generated successfully!")
        print("Please review the mapped types and check the generated file:")
        print("Hint: also run './format.sh' to format the file and reduce the diff.")
        print(f"Output: {julia_path}")


def configure_parser():
    parser = argparse.ArgumentParser(description="Generate the DuckDB Julia API")
    parser.add_argument(
        "--auto-1-index",
        action="store_true",
        default=True,
        help="Automatically convert 0-based indices to 1-based indices",
    )
    parser.add_argument(
        "--use-original-order",
        action="store_true",
        default=False,
        help="Use the original order of the functions from the old API file. New functions will be appended at the end.",
    )

    parser.add_argument(
        "--print-type-mapping",
        action="store_true",
        default=False,
        help="Print the type mapping from C to Julia",
    )

    parser.add_argument(
        "--capi-dir",
        type=str,
        required=True,
        help="Path to the input C API definitions. Should be a directory containing JSON files.",
    )
    parser.add_argument(
        "output",
        type=str,
        # default="src/api.jl",
        help="Path to the output file",
    )
    return parser


if __name__ == "__main__":
    main()
