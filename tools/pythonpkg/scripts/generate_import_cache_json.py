from inspect import getmodule
import os
script_dir = os.path.dirname(__file__)
from typing import List, Dict, Union
import json

lines: List[str] = [file for file in open(f'{script_dir}/imports.py').read().split('\n') if file != '']

class ImportCacheAttribute:
    def __init__(self, full_path: str):
        parts = full_path.split('.')
        self.type = "attribute"
        self.name = parts[-1]
        self.full_path = full_path
        self.children: Dict[str, "ImportCacheAttribute"] = {}

    def has_item(self, item_name: str) -> bool:
        return item_name in self.children

    def get_item(self, item_name: str) -> "ImportCacheAttribute":
        assert item_name in self.children
        return self.children[item_name]

    def add_item(self, item: "ImportCacheAttribute"):
        assert not self.has_item(item.name)
        self.children[item.name] = item

    def __repr__(self) -> str:
        return str(self.children)

    def to_json(self):
        return {
            "type": self.type,
            "full_path": self.full_path,
            "name": self.name,
            "children": list(self.children.keys())
        }

class ImportCacheModule:
    def __init__(self, full_path):
        parts = full_path.split('.')
        self.type = "module"
        self.name = parts[-1]
        self.full_path = full_path
        self.items: Dict[str, Union[ImportCacheAttribute, "ImportCacheModule"]] = {}

    def add_item(self, item: Union[ImportCacheAttribute, "ImportCacheModule"]):
        assert not self.has_item(item.name)
        self.items[item.name] = item

    def get_item(self, item_name: str) -> Union[ImportCacheAttribute, "ImportCacheModule"]:
        assert self.has_item(item_name)
        return self.items[item_name]

    def to_json(self):
        return {
            "type": self.type,
            "full_path": self.full_path,
            "name": self.name,
            "children": list(self.items.keys())
        }

    def has_item(self, item_name: str) -> bool:
        return item_name in self.items

    def __repr__(self) -> str:
        return str(self.items)

    def root_module(self) -> bool:
        return self.name == self.full_path

class ImportCacheGenerator:
    def __init__(self):
        self.modules: Dict[str, ImportCacheModule] = {}
    
    def add_module(self, path: str):
        assert path.startswith('import')
        path = path[7:]
        module = ImportCacheModule(path)
        self.modules[module.name] = module

        # Add it to the parent module if present
        parts = path.split('.')
        if (len(parts) == 1):
            return

        # This works back from the furthest child module to the top level module
        child_module = module
        for i in range(1, len(parts)):
            parent_path = '.'.join(parts[:len(parts)-i])
            parent_module = self.add_or_get_module(parts[i])
            parent_module.add_item(child_module)
            child_module = parent_module

    def add_or_get_module(self, module_name: str) -> ImportCacheModule:
        if module_name not in self.modules:
            self.add_module(f'import {module_name}')
        return self.get_module(module_name)

    def get_module(self, module_name: str) -> ImportCacheModule:
        if module_name not in self.modules:
            raise ValueError("Import the module before registering its attributes!")
        return self.modules[module_name]

    def get_item(self, item_name: str) -> Union[ImportCacheModule, ImportCacheAttribute]:
        parts = item_name.split('.')
        if (len(parts) == 1):
            return self.get_module(item_name)

        parent = self.get_module(parts[0])
        for i in range(1, len(parts)):
            child_path = '.'.join(parts[:i+1])
            if parent.has_item(parts[i]):
                parent = parent.get_item(parts[i])
            else:
                attribute = ImportCacheAttribute(child_path)
                parent.add_item(attribute)
                parent = attribute
        return parent

    def add_attribute(self, path: str):
        assert not path.startswith('import')
        parts = path.split('.')
        assert len(parts) >= 2
        self.get_item(path)

    def to_json(self):
        json_data = {}
        for module_name, module in self.modules.items():
            json_data[module_name] = module.to_json()
            for item_name, item in module.items.items():
                json_data[item_name] = item.to_json()
        return json_data


generator = ImportCacheGenerator()

for line in lines:
    if line.startswith('#'):
        continue
    if line.startswith('import'):
        generator.add_module(line)
    else:
        generator.add_attribute(line)

# Load existing JSON data from a file if it exists
existing_json_data = {}
try:
    with open("cache_data.json", "r") as file:
        existing_json_data = json.load(file)
except FileNotFoundError:
    pass

# Merge the existing JSON data with the new data
existing_json_data.update(generator.to_json())

# Save the merged JSON data back to the file
with open("cache_data.json", "w") as file:
    json.dump(existing_json_data, file, indent=4)

# Print the merged JSON data
#print(json.dumps(existing_json_data, indent=4))
