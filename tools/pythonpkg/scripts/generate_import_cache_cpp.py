import os

script_dir = os.path.dirname(__file__)
from typing import List, Dict, Union
import json

# Load existing JSON data from a file if it exists
json_data = {}
try:
    with open("cache_data.json", "r") as file:
        json_data = json.load(file)
except FileNotFoundError:
    print("Please first use 'generate_import_cache_json.py' first to generate json")
    pass


# deal with leaf nodes?? Those are just PythonImportCacheItem
def get_class_name(path: str) -> str:
    parts: List[str] = path.replace('_', '').split('.')
    parts = [x.title() for x in parts]
    return ''.join(parts) + 'CacheItem'


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

    def get_variables(self):
        variables = []
        for key in self.module['children']:
            # TODO: overwrite for [short, ushort]
            item = self.items[key]
            name = item['name']
            if item['children'] == []:
                class_name = 'PythonImportCacheItem'
            else:
                class_name = get_class_name(item['full_path'])
            variables.append(f'\t{class_name} {name};')
        return '\n'.join(variables)

    def get_initializer(self):
        variables = []
        for key in self.module['children']:
            item = self.items[key]
            name = item['name']
            if item['children'] == []:
                initialization = f'{name}("{name}", this)'
                variables.append(initialization)
            else:
                if item['type'] == 'module':
                    arguments = ''
                else:
                    arguments = 'this'
                initialization = f'{name}({arguments})'
                variables.append(initialization)
        return f'PythonImportCacheItem("{self.name}"), ' + ', '.join(variables) + '{}'

    def get_constructor(self):
        if self.module['type'] == 'module':
            return f'{self.class_name}()'
        return f'{self.class_name}(optional_ptr<PythonImportCacheItem> parent)'

    def to_string(self):
        return f"""
struct {self.class_name} : public PythonImportCacheItem {{
public:
\tstatic constexpr const char *Name = "{self.name}";

public:
\t{self.get_constructor()} : {self.get_initializer()}
\t~{self.class_name}() override {{}}

{self.get_variables()}
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
        self.file_name = module['name'] + '_module.hpp'
        self.items = {}
        collect_items_of_module(module, self.items)
        self.classes = collect_classes(self.items)

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


files = []
for name, value in json_data.items():
    if value['full_path'] != value['name']:
        continue
    files.append(ModuleFile(value))

for file in files:
    print(file.to_string())

# Generate the python_import_cache.hpp file
# adding all the root modules with their 'name'

# Generate the python_import_cache_modules.hpp file
# listing all the generated header files
