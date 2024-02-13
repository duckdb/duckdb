import clang.cindex
import textwrap


class EnumVisitor:
    def visit_enum(self, cursor):
        enum_name = cursor.spelling

        # Initialize expected value for the first enum constant
        enum_constants = dict()
        for enum_const in cursor.get_children():
            if enum_const.kind == clang.cindex.CursorKind.ENUM_CONSTANT_DECL:
                name = enum_const.spelling
                tokens = enum_const.get_tokens()
                if len(list(tokens)) == 1:
                    raise ValueError(
                        f"Enum constant '{name}' in '{enum_name}' does not have an explicit value assignment."
                    )
                value = enum_const.enum_value
                if value in enum_constants:
                    other_constant = enum_constants[value]
                    error = f"""
                        Enum '{enum_name}' contains a duplicate value:
                        Value {value} is defined for both '{other_constant}' and '{name}'
                    """
                    error = textwrap.dedent(error)
                    raise ValueError(error)
                enum_constants[value] = name


def parse_enum(file_path):
    enum_visitor = EnumVisitor()

    # Create index
    index = clang.cindex.Index.create()

    # Parse the file
    tu = index.parse(file_path)

    # Traverse the AST
    for cursor in tu.cursor.walk_preorder():
        if cursor.kind == clang.cindex.CursorKind.ENUM_DECL:
            enum_visitor.visit_enum(cursor)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Parse a C file and check enum uniqueness.")
    parser.add_argument("file_path", type=str, help="Path to the C file")

    args = parser.parse_args()
    file_path = args.file_path

    try:
        enum_dict = parse_enum(file_path)
    except ValueError as e:
        print(f"Error: {e}")
