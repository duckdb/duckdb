from cxxheaderparser.parser import CxxParser, ParserOptions
from cxxheaderparser.visitor import CxxVisitor
from cxxheaderparser.preprocessor import make_pcpp_preprocessor
from cxxheaderparser.parserstate import NamespaceBlockState
from cxxheaderparser.types import EnumDecl
import textwrap
import os


class Visitor:
    def on_enum(self, state: NamespaceBlockState, cursor: EnumDecl) -> None:
        enum_name = cursor.typename.segments[0].format()
        if '<' in enum_name:
            raise Exception(
                "Enum '{}' is an anonymous enum, please name it\n".format(cursor.doxygen[3:] if cursor.doxygen else '')
            )

        enum_constants = dict()
        for enum_const in cursor.values:
            name = enum_const.name.format()
            if enum_const.value is None:
                raise Exception(f"Enum constant '{name}' in '{enum_name}' does not have an explicit value assignment.")
            value = enum_const.value.format()
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

    def __getattr__(self, name):
        return lambda *args, **kwargs: True


def parse_enum(file_path):
    # Create index
    parser = CxxParser(
        file_path,
        None,
        visitor=Visitor(),
        options=ParserOptions(preprocessor=make_pcpp_preprocessor()),
    )
    parser.parse()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Parse a C header file and check enum integrity.")
    parser.add_argument("file_path", type=str, help="Path to the C header file")

    args = parser.parse_args()
    file_path = args.file_path

    if not os.path.exists(file_path):
        raise Exception(f"Error: file '{file_path}' does not exist")

    enum_dict = parse_enum(file_path)
