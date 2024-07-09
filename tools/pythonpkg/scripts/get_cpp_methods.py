# Requires `python3 -m pip install cxxheaderparser pcpp`
import os

import cxxheaderparser.parser
import cxxheaderparser.visitor
import cxxheaderparser.preprocessor
from typing import List, Dict

scripts_folder = os.path.dirname(os.path.abspath(__file__))


class FunctionParam:
    def __init__(self, name: str, proto: str):
        self.proto = proto
        self.name = name


class ConnectionMethod:
    def __init__(self, name: str, params: List[FunctionParam], is_void: bool):
        self.name = name
        self.params = params
        self.is_void = is_void


class Visitor:
    def __init__(self, class_name: str):
        self.methods_dict = {}
        self.class_name = class_name

    def __getattr__(self, name):
        return lambda *state: True

    def on_class_start(self, state):
        name = state.class_decl.typename.segments[0].format()
        return name == self.class_name

    def on_class_method(self, state, node):
        name = node.name.format()
        return_type = node.return_type
        is_void = return_type and return_type.format() == "void"
        params = [
            FunctionParam(
                x.name,
                x.type.format() + " " + x.name + (" = " + x.default.format() if x.default else ""),
            )
            for x in node.parameters
        ]

        self.methods_dict[name] = ConnectionMethod(name, params, is_void)


def get_methods(class_name: str) -> Dict[str, ConnectionMethod]:
    CLASSES = {
        "DuckDBPyConnection": os.path.join(
            scripts_folder,
            "..",
            "src",
            "include",
            "duckdb_python",
            "pyconnection",
            "pyconnection.hpp",
        ),
        "DuckDBPyRelation": os.path.join(scripts_folder, "..", "src", "include", "duckdb_python", "pyrelation.hpp"),
    }
    # Create a dictionary to store method names and prototypes
    methods_dict = {}

    path = CLASSES[class_name]

    visitor = Visitor(class_name)
    preprocessor = cxxheaderparser.preprocessor.make_pcpp_preprocessor(retain_all_content=True)
    tu = cxxheaderparser.parser.CxxParser(
        path,
        None,
        visitor,
        options=cxxheaderparser.parser.ParserOptions(
            preprocessor=preprocessor,
        ),
    )
    tu.parse()

    return visitor.methods_dict


if __name__ == "__main__":
    print("This module should not called directly, please use `make generate-files` instead")
    exit(1)
