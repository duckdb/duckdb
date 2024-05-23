import clang.cindex
import os

scripts_folder = os.path.dirname(os.path.abspath(__file__))
SOURCE_PATH = os.path.join(scripts_folder, '..', 'src', 'include', 'duckdb_python', 'pyconnection', 'pyconnection.hpp')
assert os.path.isfile(SOURCE_PATH)

with open(SOURCE_PATH, 'r') as file:
    file_contents = file.read()

def get_string(input: clang.cindex.SourceRange) -> str:
    return file_contents[input.start.offset:input.end.offset]

class FunctionParam:
    def __init__(self, proto, name):
        self.proto = proto
        self.name = name

class ConnectionMethod:
    def __init__(
            self,
            name,
            arguments,
            is_void
        ):
        self.name = name
    
    def 


# Function to recursively traverse the AST and extract method names and prototypes
def traverse(node, methods_dict):
    if node.kind == clang.cindex.CursorKind.STRUCT_DECL or node.kind == clang.cindex.CursorKind.CLASS_DECL:
        if node.spelling != "DuckDBPyConnection":
            return
        # Recursively traverse children
        for child in node.get_children():
            traverse(child, methods_dict)
    elif node.kind == clang.cindex.CursorKind.CXX_METHOD:
        # Get method name
        method_name = node.spelling
        # Get method prototype
        method_return_type = node.type.get_result().spelling
        print(method_return_type)
        params = [(x.spelling, get_string(x.extent)) for x in node.get_arguments()]

        #method_params = [(param.spelling, param.type.spelling) for param in node.get_children() if param.kind == clang.cindex.CursorKind.PARM_DECL]
        method_proto = f"{method_return_type} {method_name}(" + ", ".join([f"{param}" for param in params]) + ")"
        methods_dict[method_name] = method_proto
    else:
        # Recursively traverse children
        for child in node.get_children():
            traverse(child, methods_dict)

# Initialize Clang Index
index = clang.cindex.Index.create()
# Parse the C++ code
tu = index.parse(SOURCE_PATH, args=['-std=c++11'])

# Create a dictionary to store method names and prototypes
methods_dict = {}

def walk(node, depth):
    print(('\t' * depth) + 'KIND: %s | NAME: %s | TYPE: %s' % (node.kind, node.spelling or node.displayname, node.type.spelling))

    for c in node.get_children():
        walk(c, depth + 1)

#walk(tu.cursor, 0)

# Traverse the AST
traverse(tu.cursor, methods_dict)

# Print the dictionary
for name in methods_dict:
    print(f'{methods_dict[name]}')

