import os

script_dir = os.path.dirname(__file__)
from typing import List, Dict
import json

# Load existing JSON data from a file if it exists
json_data = {}
json_cache_path = os.path.join(script_dir, "cache_data.json")
try:
    with open(json_cache_path, "r") as file:
        json_data = json.load(file)
except FileNotFoundError:
    print("Please first use 'generate_import_cache_json.py' first to generate json")


# deal with leaf nodes?? Those are just PythonImportCacheItem
def get_class_name(path: str) -> str:
    parts: List[str] = path.replace('_', '').split('.')
    parts = [x.title() for x in parts]
    return ''.join(parts) + 'CacheItem'


def get_filename(name: str) -> str:
    return name.replace('_', '').lower() + '_module.hpp'


def get_variable_name(name: str) -> str:
    if name in ['short', 'ushort']:
        return name + '_'
    return name


def collect_items_of_module(module: dict, collection: Dict):
    global json_data
    children = module['children']
    collection[module['full_path']] = module
    for child in children:
        collect_items_of_module(json_data[child], collection)


class CacheItem:
    def __init__(self, module: dict, items):
        self.name = module['name']
        self.module = module
        self.items = items
        self.class_name = get_class_name(module['full_path'])

    def get_full_module_path(self):
        if self.module['type'] != 'module':
            return ''
        full_path = self.module['full_path']
        return f"""
public:
\tstatic constexpr const char *Name = "{full_path}";
"""

    def get_optionally_required(self):
        if 'required' not in self.module:
            return ''
        string = f"""
protected:
\tbool IsRequired() const override final {{
\t\treturn {str(self.module['required']).lower()};
\t}}
"""
        return string

    def get_variables(self):
        variables = []
        for key in self.module['children']:
            item = self.items[key]
            name = item['name']
            var_name = get_variable_name(name)
            if item['children'] == []:
                class_name = 'PythonImportCacheItem'
            else:
                class_name = get_class_name(item['full_path'])
            variables.append(f'\t{class_name} {var_name};')
        return '\n'.join(variables)

    def get_initializer(self):
        variables = []
        for key in self.module['children']:
            item = self.items[key]
            name = item['name']
            var_name = get_variable_name(name)
            if item['children'] == []:
                initialization = f'{var_name}("{name}", this)'
                variables.append(initialization)
            else:
                if item['type'] == 'module':
                    arguments = ''
                else:
                    arguments = 'this'
                initialization = f'{var_name}({arguments})'
                variables.append(initialization)
        if self.module['type'] != 'module':
            constructor_params = f'"{self.name}"'
            constructor_params += ', parent'
        else:
            full_path = self.module['full_path']
            constructor_params = f'"{full_path}"'
        return f'PythonImportCacheItem({constructor_params}), ' + ', '.join(variables) + '{}'

    def get_constructor(self):
        if self.module['type'] == 'module':
            return f'{self.class_name}()'
        return f'{self.class_name}(optional_ptr<PythonImportCacheItem> parent)'

    def to_string(self):
        return f"""
struct {self.class_name} : public PythonImportCacheItem {{
{self.get_full_module_path()}
public:
\t{self.get_constructor()} : {self.get_initializer()}
\t~{self.class_name}() override {{}}

{self.get_variables()}
{self.get_optionally_required()}
}};
"""


def collect_classes(items: Dict) -> List:
    output: List = []
    for item in items.values():
        if item['children'] == []:
            continue
        output.append(CacheItem(item, items))
    return output


class ModuleFile:
    def __init__(self, module: dict):
        self.module = module
        self.file_name = get_filename(module['name'])
        self.items = {}
        collect_items_of_module(module, self.items)
        self.classes = collect_classes(self.items)
        self.classes.reverse()

    def get_classes(self):
        classes = []
        for item in self.classes:
            classes.append(item.to_string())
        return ''.join(classes)

    def to_string(self):
        string = f"""
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/{self.file_name}
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {{
{self.get_classes()}
}} // namespace duckdb
"""
        return string


files: List[ModuleFile] = []
for name, value in json_data.items():
    if value['full_path'] != value['name']:
        continue
    files.append(ModuleFile(value))

for file in files:
    content = file.to_string()
    path = f'src/include/duckdb_python/import_cache/modules/{file.file_name}'
    import_cache_path = os.path.join(script_dir, '..', path)
    with open(import_cache_path, "w") as f:
        f.write(content)


def get_root_modules(files: List[ModuleFile]):
    modules = []
    for file in files:
        name = file.module['name']
        class_name = get_class_name(name)
        modules.append(f'\t{class_name} {name};')
    return '\n'.join(modules)


# Generate the python_import_cache.hpp file
# adding all the root modules with their 'name'
import_cache_file = f"""
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/python_import_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb_python/import_cache/python_import_cache_modules.hpp"

namespace duckdb {{

struct PythonImportCache {{
public:
    explicit PythonImportCache() {{
    }}
    ~PythonImportCache();

public:
{get_root_modules(files)}

public:
    py::handle AddCache(py::object item);

private:
    vector<py::object> owned_objects;
}};

}} // namespace duckdb

"""

import_cache_path = os.path.join(script_dir, '..', 'src/include/duckdb_python/import_cache/python_import_cache.hpp')
with open(import_cache_path, "w") as f:
    f.write(import_cache_file)


def get_module_file_path_includes(files: List[ModuleFile]):
    includes = []
    for file in files:
        includes.append(f'#include "duckdb_python/import_cache/modules/{file.file_name}"')
    return '\n'.join(includes)


module_includes = get_module_file_path_includes(files)

modules_header = os.path.join(
    script_dir, '..', 'src/include/duckdb_python/import_cache/python_import_cache_modules.hpp'
)
with open(modules_header, "w") as f:
    f.write(module_includes)

# Generate the python_import_cache_modules.hpp file
# listing all the generated header files
