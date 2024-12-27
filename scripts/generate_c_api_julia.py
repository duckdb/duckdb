from abc import abstractmethod
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


class AbstractApiTarget:
    @abstractmethod
    def declare_typedefs(self, type_defs):
        pass

    def write_empty_line(self):
        pass

    @abstractmethod
    def write_function(self, function_obj: FunctionDef):
        pass

    @abstractmethod
    def write_group_start(self, group: str):
        pass

    def write_header(self, version):
        """Writes the header of the file."""
        pass

    def write_footer(self):
        pass

    def write_functions(
        self,
        version,
        function_groups: List[FunctionGroup],
        function_maps: Dict[str, FunctionDef],
    ):
        self.write_header(version)
        self.write_empty_line()

        for group in function_groups:
            self.write_group_start(group["group"])
            self.write_empty_line()
            for fn in group["entries"]:
                self.write_function(fn)
                self.write_empty_line()
            self.write_empty_line()
            self.write_empty_line()
        self.write_footer()


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
}


class JuliaApiTarget(AbstractApiTarget):
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

    def write_empty_line(self) -> None:
        """Writes an empty line to the output file."""
        self.file.write(self.linesep)

    def _get_casted_type(
        self, type_str: str, is_return_arg=False, auto_remove_t_suffix=True
    ):
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
            return_type, arg_types = self.overwrite_function_signatures[
                function_obj["name"]
            ]
            return arg_names, arg_types

        arg_types = [
            self._get_casted_type(param["type"]) for param in function_obj["params"]
        ]
        return arg_names, arg_types

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
        arg_names, arg_types = self.get_argument_names_and_types(function_obj)
        arg_names_s = ", ".join(arg_names)
        arg_comments = [
            function_obj.get("comment", {})
            .get("param_comments", {})
            .get(param["name"], "")
            for param in function_obj["params"]
        ]

        self.file.write(f"{'    ' * self.indent}\"\"\"\n")
        self.file.write(
            f"{'    ' * self.indent}    {function_obj['name']}({arg_names_s})\n"
        )
        self.file.write(f"{'    ' * self.indent}\n")
        self.file.write(f"{'    ' * self.indent}{description}\n")
        self.file.write(f"{'    ' * self.indent}\n")
        self.file.write(f"{'    ' * self.indent}# Arguments\n")
        for i, arg_name in enumerate(arg_names):
            self.file.write(
                f"{'    ' * self.indent}- `{arg_name}`: {arg_comments[i]}\n"
            )
        self.file.write(f"{'    ' * self.indent}\n")
        self.file.write(
            f"{'    ' * self.indent}Returns: {function_obj['comment'].get('return_value', '')}\n"
        )
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

    def _write_function_definition(self, function_obj: FunctionDef):
        """_create_function_definition


        Example:
        ```julia
            function duckdb_get_int64(handle)
                return ccall((:duckdb_get_int64, libduckdb), Int64, (duckdb_value,), handle)
            end
        ```

        Args:
            function_obj: _description_
        """

        fname = function_obj["name"]
        arg_names, arg_types = self.get_argument_names_and_types(function_obj)
        if len(arg_types) == 0:
            arg_types_s = ""
        elif len(arg_types) == 1:
            arg_types_s = arg_types[0] + ","
        else:
            arg_types_s = ", ".join(arg_types)

        arg_names_s = ", ".join(arg_names)  # for function definition
        # for function call
        arg_names_call = ", ".join(
            [
                f"{arg_name} - 1"  # 1-based index
                if self.auto_1base_index
                and fname not in self.auto_1base_index_ignore_functions
                and self._is_index_argument(arg_name, function_obj)
                else arg_name
                for arg_name in arg_names
            ]
        )

        return_type = self._get_casted_type(
            function_obj["return_type"], is_return_arg=True
        )

        is_index1_function = (
            fname not in self.auto_1base_index_ignore_functions
            and self.auto_1base_index
            and fname in self.auto_1base_index_return_functions
        )

        self.file.write(f"{'    ' * self.indent}function {fname}({arg_names_s})\n")

        if function_obj.get("group_deprecated", False) or function_obj.get(
            "deprecated", False
        ):
            self._write_function_depwarn(function_obj, indent=1)

        # !!!!!!!! REMOVE !!!!!
        # self.file.write(f"{'    ' * self.indent}    println(\"{fname}()\")\n")

        self.file.write(
            f"{'    ' * self.indent}    return ccall((:{fname}, libduckdb), {return_type}, ({arg_types_s}), {arg_names_call}){' + 1' if is_index1_function else ''}\n"
        )
        self.file.write(f"{'    ' * self.indent}end\n")

    def write_function(self, function_obj: FunctionDef):
        if function_obj["name"] in self.skipped_functions:
            return

        if function_obj.get("group_deprecated", False):
            self.deprecated_functions.append(function_obj["name"])

        self._write_function_docstring(function_obj)
        self._write_function_definition(function_obj)

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
        self._analyze_types(function_groups)

        self.write_header(version)
        self.write_empty_line()

        if self.manual_order is not None:
            current_group = None
            for f in self.manual_order:
                if f not in function_map:
                    # raise ValueError(f"Function {f} not found in function_map")
                    logging.warning(f"Function {f} not found in function_map")
                    continue

                if current_group != function_map[f]["group"]:
                    current_group = function_map[f]["group"]
                    self.write_group_start(current_group)
                    self.write_empty_line()

                self.write_function(function_map[f])
                self.write_empty_line()

            # Write new functions
            self.write_group_start("New Functions")
            self.write_empty_line()
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
                        self.type_maps[param["type"]] = self._get_casted_type(
                            param["type"]
                        )
                if fn["return_type"] not in self.type_maps:
                    self.type_maps[fn["return_type"]] = self._get_casted_type(
                        fn["return_type"]
                    )

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
        # scan the file and get the order of the functions
        path = pathlib.Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"File {path} does not exist")

        with open(path, "r") as f:
            lines = f.readlines()

        # find the function definitions
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

    ext_api_definitions = parse_ext_api_definitions(EXT_API_DEFINITION_PATTERN)
    ext_api_version = get_extension_api_version(ext_api_definitions)
    function_groups, function_map = parse_capi_function_definitions()

    print("Creating Julia API")
    ROOT = pathlib.Path(__file__).parent.parent
    julia_path = ROOT / "tools" / "juliapkg" / "src" / "api.jl"
    julia_path_old = ROOT / "tools" / "juliapkg" / "src" / "api_old.jl"

    if not julia_path_old.exists():
        raise FileNotFoundError(
            f"File {julia_path_old} does not exist. Ideally rename the current file 'api.jl' to 'api_old.jl'"
        )

    with (
        JuliaApiTarget(
            julia_path,
            indent=0,
            auto_1base_index=True,  # WARNING: every arg named "col/row/index" or similar will be 1-based indexed, so the argument is subtracted by 1
            auto_1base_index_return_functions={"duckdb_init_get_column_index"},
            auto_1base_index_ignore_functions={
                "duckdb_parameter_name",
                "duckdb_param_type",
                "duckdb_param_logical_type",
            },
            skipped_functions={},
            type_map=JULIA_BASE_TYPE_MAP,
            overwrite_function_signatures={
                "duckdb_free": (
                    "Cvoid",
                    ("Ptr{Cvoid}",),
                ),  # Must be Ptr{Cvoid} and not Ref
                "duckdb_bind_blob": (
                    "duckdb_state",
                    ("duckdb_prepared_statement", "idx_t", "Ptr{Cvoid}", "idx_t"),
                ),
            },
        ) as printer
    ):
        keep_old_order = True

        if keep_old_order:
            manual_order = printer.get_function_order(julia_path_old)
            printer.manual_order = manual_order

        printer.write_functions(ext_api_version, function_groups, function_map)

        print("Type maps:")
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


if __name__ == "__main__":
    main()
