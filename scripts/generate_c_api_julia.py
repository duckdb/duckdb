from abc import abstractmethod
import os
import pathlib
import re
from typing import Dict, List, NotRequired, TypedDict

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
    type_pattern = r"^((const)\s*)?([_a-zA-Z]*)"

    if (m1 := re.match(ptr_pattern, type_str)) is not None:
        before_ptr = m1.group(1)
        is_const = bool(m1.group(2))
        type.append("Const Ptr" if is_const else "Ptr")
        return parse_c_type(before_ptr, type)

    type.append(type_str)  # append the remaining type
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
    # User defined types
    "idx_t": "idx_t",
    "duckdb_type": "DUCKDB_TYPE",
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

    overwrite_function_signatures = {}

    # Functions that return an index
    auto_1base_index: bool
    auto_1base_index_return_functions = set()

    def __init__(
        self,
        file,
        indent=0,
        auto_1base_index=True,
        auto_1base_index_return_functions=set(),
        skipped_functions=set(),
        type_map={},
        overwrite_function_signatures={},
    ):
        # check if file is a string or a file object
        if isinstance(file, str) or isinstance(file, pathlib.Path):
            self.file = open(file, "w+")
            self.filename = file
        elif hasattr(file, "write"):
            self.file = file
            self.filename = ""
        else:
            raise ValueError("file must be a string or a file object")
        self.indent = indent
        self.auto_1base_index = auto_1base_index
        self.auto_1base_index_return_functions = auto_1base_index_return_functions
        self.linesep = os.linesep
        self.type_map = type_map
        self.skipped_functions = skipped_functions
        self.overwrite_function_signatures = overwrite_function_signatures
        super().__init__()

    def write_empty_line(self):
        self.file.write(self.linesep)

    def _get_casted_type(self, type_str: str, is_return_arg=False):
        type_str = type_str.strip()
        type_definition = parse_c_type(type_str, [])

        def reduce_type(type_list: list[str]):
            if len(type_list) == 0:
                return ""

            t = type_list[0]
            if len(type_list) == 1:
                if t.startswith("const "):
                    t = t.removeprefix("const ")
                if t in self.type_map:
                    return self.type_map[t]
                else:
                    if " " in t:
                        raise (ValueError(f"Unknown type: {t}"))

                    # Remove _t suffix
                    if t.endswith("_t"):
                        t = t.removesuffix("_t")
                    return t

            # Handle Pointer types
            if t not in ("Ptr", "Const Ptr"):
                raise ValueError(f"Unknown type: {t}")

            if len(type_list) >= 2 and type_list[1].strip() in (
                "char",
                "const char",
            ):
                if is_return_arg:
                    return "Ptr{UInt8}"  # TODO maybe this can be improved
                else:
                    return "Cstring"
            else:
                if is_return_arg:
                    return "Ptr{" + reduce_type(type_list[1:]) + "}"
                else:
                    return "Ref{" + reduce_type(type_list[1:]) + "}"

        return reduce_type(type_definition)

    def _get_argument_name(self, name: str):
        if name in JULIA_RESERVED_KEYWORDS:
            return f"{name}_"
        return name

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
            # "param_idx",
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
        arg_names = [
            self._get_argument_name(param["name"]) for param in function_obj["params"]
        ]

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

        self.file.write(f"{'    ' * self.indent}\"\"\"\n")
        self.file.write(
            f"{'    ' * self.indent}    {function_obj['name']}({arg_names_s})\n"
        )
        self.file.write(f"{'    ' * self.indent}\n")
        self.file.write(f"{'    ' * self.indent}{description}\n")
        self.file.write(f"{'    ' * self.indent}\n")
        self.file.write(f"{'    ' * self.indent}# Arguments\n")
        for i, arg_name in enumerate(arg_names):
            self.file.write(f"{'    ' * self.indent}- `{arg_name}`: {arg_types[i]}\n")
        self.file.write(f"{'    ' * self.indent}\n")
        self.file.write(
            f"{'    ' * self.indent}Returns: {function_obj['comment'].get('return_value', '')}\n"
        )
        self.file.write(f"{'    ' * self.indent}\"\"\"\n")

    def _write_function_depwarn(self, function_obj: FunctionDef, indent: int = 0):
        """
        Example:
         Base.depwarn(
        "The `G` type parameter will be deprecated in a future release. " *
        "Please use `MyType(args...)` instead of `MyType{$G}(args...)`.",
        :MyType,
        )
        """
        indent = self.indent + indent  # total indent

        notice = function_obj.get("comment", {}).get("description", "")
        notice = (
            notice.replace("\n", " ").replace('"', '\\"').strip()
        )  # escape double quotes

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
                and self._is_index_argument(arg_name, function_obj)
                else arg_name
                for arg_name in arg_names
            ]
        )

        return_type = self._get_casted_type(
            function_obj["return_type"], is_return_arg=True
        )

        is_index1_function = (
            self.auto_1base_index and fname in self.auto_1base_index_return_functions
        )

        self.file.write(f"{'    ' * self.indent}function {fname}({arg_names_s})\n")

        if function_obj.get("group_deprecated", False) or function_obj.get("deprecated", False):
            self._write_function_depwarn(function_obj, indent=1)

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

    def write_footer(self):
        pass
        # self.file.write(f"# {'-' * 80}\n")
        # self.file.write("# Deprecations\n")
        # self.file.write(f"# {'-' * 80}\n")
        # self.file.write("\n")
        # for fname in self.deprecated_functions:
        #     self.file.write(
        #         f"# {'    ' * self.indent}Base.@deprecate_binding {fname} nothing\n"
        #     )

    def write_functions(
        self,
        version,
        function_groups: List[FunctionGroup],
        function_map: Dict[str, FunctionDef],
    ):
        self._analyze_types(function_groups)
        super().write_functions(version, function_groups, function_map)

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
        self.file.write(f"# {'-' * 80}\n")
        self.file.write(f"# {group}\n")
        self.file.write(f"# {'-' * 80}\n")


def main():
    ext_api_definitions = parse_ext_api_definitions(EXT_API_DEFINITION_PATTERN)
    ext_api_version = get_extension_api_version(ext_api_definitions)
    function_groups, function_map = parse_capi_function_definitions()

    print("Creating Julia API")
    ROOT = pathlib.Path(__file__).parent.parent
    julia_path = ROOT / "tools" / "juliapkg" / "src" / "api.jl"
    printer = JuliaApiTarget(
        julia_path,
        indent=0,
        auto_1base_index=True,  # WARNING: every arg named "col/row/index" or similar will be 1-based indexed, so the argument is subtracted by 1
        auto_1base_index_return_functions={"duckdb_init_get_column_index"},
        skipped_functions={},
        type_map=JULIA_BASE_TYPE_MAP,
        overwrite_function_signatures={
            "duckdb_free": ("Cvoid", ("Ptr{Cvoid}",)),  # Must be Ptr{Cvoid} and not Ref
            "duckdb_bind_blob": (
                "duckdb_state",
                ("duckdb_prepared_statement", "idx_t", "Ptr{Cvoid}", "idx_t"),
            ),
        },
    )
    printer.write_functions(ext_api_version, function_groups, function_map)

    print("Type maps:")
    K = list(printer.inverse_type_maps.keys())
    K.sort()
    for k in K:
        v = ", ".join(printer.inverse_type_maps[k])
        print(f"    {k} -> {v}")
    print("Julia API generated successfully!")
    print("Please review the mapped types and check the generated file:")
    print(f"Output: {julia_path}")


if __name__ == "__main__":
    main()
