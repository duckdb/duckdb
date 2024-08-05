import os
import json
import re
from typing import Set, List
from functools import total_ordering

# define paths
BASE_DIR = os.path.dirname(__file__)
JSON_PATH = os.path.join(BASE_DIR, "..", "src/common", "settings.json")
DUCKDB_SETTINGS_HEADER_FILE = os.path.join(BASE_DIR, "..", "src/include/duckdb/main", "settings.hpp")
DUCKDB_LOGICAL_TYPES_FILE = os.path.join(BASE_DIR, "..", "src/include/duckdb/common", "types.hpp")
DUCKDB_SETTINGS_SCOPE_FILE = os.path.join(BASE_DIR, "..", "src/main", "config.cpp")

# markers
START_MARKER = "// Start of the auto-generated list of settings"
END_MARKER = "// End of the auto-generated list of settings"
SEPARATOR = "//===--------------------------------------------------------------------===//\n"


def extract_valid_input_types(file_path: str) -> List[str]:
    with open(file_path, 'r') as file:
        content = file.read()
    enum_block_match = re.search(r'enum class LogicalTypeId : uint8_t {([^}]*)}', content)
    if not enum_block_match:
        return []
    enum_block = enum_block_match.group(1)
    # regex to find individual LogicalTypeId entries
    return re.findall(r'\b(\w+)\s*=\s*\d+', enum_block)


valid_input_types_list = extract_valid_input_types(DUCKDB_LOGICAL_TYPES_FILE)


@total_ordering
class Setting:
    # set to track names of written settings in order to prevent duplicates
    written_settings: Set[str] = set()

    def __init__(
        self, name: str, type: str, scope: str, description: str, aliases: List[str], verification: bool = False
    ):
        self.name = self._is_valid_name(name)
        self.type = self._get_type(type)
        self.scope = self._is_valid_scope(scope)
        self.description = description
        self.verification = verification
        self.function_name = self._get_function_name()
        self.aliases = self._get_aliases(aliases)

    # all comparisons are based on setting name
    def __eq__(self, other) -> bool:
        return isinstance(other, Setting) and self.name == other.name

    def __lt__(self, other) -> bool:
        return isinstance(other, Setting) and self.name < other.name

    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self):
        return f"struct {self.function_name} -> {self.name}, {self.type}, {self.scope}, {self.description}, {self.verification}, {self.aliases}"

    # validate setting name for correct format and uniquenes
    def _is_valid_name(self, name: str) -> str:
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise ValueError(f"'{name}' cannot be used as setting name - invalid character")
        if name in Setting.written_settings:
            raise ValueError(f"'{name}' cannot be used as setting name - already exists")
        Setting.written_settings.add(name)
        return name

    # validate setting scope to be one of the accepted values
    def _is_valid_scope(self, scope: str) -> str:
        scope = scope.upper()
        if scope in {"GLOBAL", "LOCAL", "GLOBAL_LOCAL"}:
            return scope
        return "INVALID"

    # validate and return the correct type format
    def _get_type(self, type) -> str:
        if type in valid_input_types_list:
            return f"LogicalTypeId::{type}"
        raise ValueError(f"Invalid input type: '{type}'")

    # dalidate and return a set of the aliases
    def _get_aliases(self, aliases: List[str]) -> List[str]:
        return [self._is_valid_name(alias) for alias in aliases]

    # generate a function name for the setting
    def _get_function_name(self) -> str:
        camel_case_name = ''.join(word.capitalize() for word in re.split(r'[-_]', self.name))
        if camel_case_name.endswith("Setting"):
            return f"{camel_case_name}"
        return f"{camel_case_name}Setting"

    # generate the cpp definition for the setting
    def extract_definition(self) -> str:
        definition = (
            f"struct {self.function_name} {{\n"
            f"    static constexpr const char *Name = \"{self.name}\";\n"
            f"    static constexpr const char *Description = \"{self.description}\";\n"
            f"    static constexpr const LogicalTypeId InputType = {self.type};\n"
        )
        if self.scope == "GLOBAL" or self.scope == "GLOBAL_LOCAL":
            definition += (
                f"    static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);\n"
            )
            definition += f"    static void ResetGlobal(DatabaseInstance *db, DBConfig &config);\n"
        if self.scope == "LOCAL" or self.scope == "GLOBAL_LOCAL":
            definition += f"    static void SetLocal(ClientContext &context, const Value &parameter);\n"
            definition += f"    static void ResetLocal(ClientContext &context);\n"
        definition += f"    static Value GetSetting(const ClientContext &context);\n"
        if self.verification:
            if self.scope == "LOCAL" or self.scope == "GLOBAL_LOCAL":
                definition += f"    static void Verify(ClientContext &context, const Value &input);\n"
            else:
                definition += f"    static void Verify(const Value &input);\n"
        definition += f"}};\n\n"
        return definition


# generate and manage code for settings
class CodeGenerator:
    # generated settings definitions
    settings = list()

    # read settings from a JSON file and create Setting objects
    def _read_settings_from_json(json_path):
        with open(json_path, 'r') as json_file:
            json_data = json.load(json_file)
            for entry in json_data:
                setting = Setting(
                    entry['name'],
                    entry['type'],
                    entry['scope'],
                    entry['description'],
                    entry.get('aliases', []),
                    entry['verification'],
                )
                CodeGenerator.settings.append(setting)

    def __find_start_end_indexes(source_code, start_marker, end_marker, file_path):
        start_matches = list(re.finditer(start_marker, source_code))
        if len(start_matches) == 0:
            raise ValueError(f"Couldn't find start marker in {file_path}")
        elif len(start_matches) > 1:
            raise ValueError(f"Start marker found more than once in {file_path}")
        start_index = start_matches[0].end()

        end_matches = list(re.finditer(end_marker, source_code[start_index:]))
        if len(end_matches) == 0:
            raise ValueError(f"Couldn't find end marker in {file_path}")
        elif len(end_matches) > 1:
            raise ValueError(f"End marker found more than once in {file_path}")
        end_index = start_index + end_matches[0].start()
        return start_index, end_index

    # generate code for settings from a JSON file and insert into the header file
    def generate_code_from_json():
        with open(DUCKDB_SETTINGS_HEADER_FILE, 'r') as source_file:
            source_code = source_file.read()

        # find indexes for the start and end of the auto-generated section
        start_index, end_index = CodeGenerator.__find_start_end_indexes(
            source_code, START_MARKER, END_MARKER, DUCKDB_SETTINGS_HEADER_FILE
        )
        # split source code into sections
        start_section = source_code[: start_index + 1] + SEPARATOR
        end_section = SEPARATOR + source_code[end_index:]
        # generate settings' new content
        CodeGenerator._read_settings_from_json(JSON_PATH)
        new_content = "".join(setting.extract_definition() for setting in CodeGenerator.settings)
        return start_section + new_content + end_section, len(CodeGenerator.settings)

    # generate the scope code for the ConfigurationOption array and insert into the config file
    def generate_scope_code():
        start_marker = r'static const ConfigurationOption internal_options\[\] = \{'
        alias_marker = r',\s*FINAL_SETTING};'
        with open(DUCKDB_SETTINGS_SCOPE_FILE, 'r') as source_file:
            source_code = source_file.read()
        # find indexes for the start and end of the settings array
        start_index, end_index = CodeGenerator.__find_start_end_indexes(
            source_code, start_marker, alias_marker, DUCKDB_SETTINGS_SCOPE_FILE
        )
        # split source code into sections
        before_array = source_code[:start_index]
        after_array = source_code[end_index:]

        # generate new entries for the settings array
        new_entries = []
        for setting in CodeGenerator.settings:
            if setting.scope in ["GLOBAL", "LOCAL", "GLOBAL_LOCAL"]:
                new_entries.append(f"DUCKDB_{setting.scope}({setting.function_name})")
                for alias in setting.aliases:
                    new_entries.append(f"DUCKDB_{setting.scope}_ALIAS(\"{alias}\", {setting.function_name})")
        new_array_section = ',\n    '.join(new_entries)
        return before_array + new_array_section + after_array

    @staticmethod
    def generate():
        # generate new content for the header file
        with open(DUCKDB_SETTINGS_HEADER_FILE, 'r') as source_file:
            new_content, num_of_settings = CodeGenerator.generate_code_from_json()
        with open(DUCKDB_SETTINGS_HEADER_FILE, 'w') as source_file:
            source_file.write("".join(new_content))

        # generate new content for the config file
        with open(DUCKDB_SETTINGS_SCOPE_FILE, 'r') as source_file:
            new_content = CodeGenerator.generate_scope_code()
        with open(DUCKDB_SETTINGS_SCOPE_FILE, 'w') as source_file:
            source_file.write("".join(new_content))
        print(f"Parsed and included {num_of_settings} settings definitions.")


def sort_entries_in_json(path):
    with open(path, 'r') as file:
        data = json.load(file)
    sorted_data = sorted(data, key=lambda x: x['name'])
    with open(path, 'w') as file:
        json.dump(sorted_data, file, indent=4)


def generate():
    sort_entries_in_json(JSON_PATH)
    CodeGenerator.generate()


if __name__ == '__main__':
    generate()
