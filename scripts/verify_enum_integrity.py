import clang.cindex
import textwrap
import os
from typing import Optional


def visit_enum(cursor):
    enum_name = cursor.spelling

    enum_constants = dict()
    for enum_const in cursor.get_children():
        if enum_const.kind == clang.cindex.CursorKind.ENUM_CONSTANT_DECL:
            name = enum_const.spelling
            tokens = enum_const.get_tokens()
            if len(list(tokens)) == 1:
                raise Exception(f"Enum constant '{name}' in '{enum_name}' does not have an explicit value assignment.")
            value = enum_const.enum_value
            if value in enum_constants:
                other_constant = enum_constants[value]
                error = f"""
                    Enum '{enum_name}' contains a duplicate value:
                    Value {value} is defined for both '{other_constant}' and '{name}'
                """
                error = textwrap.dedent(error)
                raise Exception(error)
            enum_constants[value] = name
    print(f"Succesfully verified the integrity of enum {enum_name} ({len(enum_constants)} entries)")


def parse_enum(file_path, clang_path: Optional[str] = None):
    if clang_path:
        clang.cindex.Config.set_library_path(clang_path)

    # Create index
    index = clang.cindex.Index.create()

    # Parse the file
    tu = index.parse(file_path)

    # Traverse the AST
    for cursor in tu.cursor.walk_preorder():
        try:
            kind = cursor.kind
            is_enum = kind == clang.cindex.CursorKind.ENUM_DECL
        except:
            is_enum = False
        if not is_enum:
            continue
        visit_enum(cursor)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Parse a C header file and check enum integrity.")
    parser.add_argument("file_path", type=str, help="Path to the C header file")
    parser.add_argument("--library_path", type=str, help="Path to the clang library", default=None)

    args = parser.parse_args()
    file_path = args.file_path

    if not os.path.exists(file_path):
        raise Exception(f"Error: file '{file_path}' does not exist")

    enum_dict = parse_enum(file_path, args.library_path)
