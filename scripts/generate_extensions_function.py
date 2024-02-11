import os
import csv
import re
import argparse
import glob
from typing import Set, Tuple, cast
import pathlib
from typing import NamedTuple
from typing import List, Dict

os.chdir(os.path.dirname(__file__))

parser = argparse.ArgumentParser(description='Generates/Validates extension_functions.hpp file')

parser.add_argument(
    '--validate',
    action=argparse.BooleanOptionalAction,
    help='If set will validate that extension_entries.hpp is up to date, otherwise it generates the extension_functions.hpp file.',
)

args = parser.parse_args()

EXTENSIONS_PATH = os.path.join("..", "build", "extension_configuration", "extensions.txt")
DUCKDB_PATH = os.path.join("..", 'build', 'release', 'duckdb')


class Function(NamedTuple):
    name: str
    type: str


class ExtensionFunction(NamedTuple):
    extension: str
    name: str
    type: str


class ExtensionSetting(NamedTuple):
    extension: str
    name: str


def check_prerequisites():
    if not os.path.isfile(EXTENSIONS_PATH):
        print(
            "please run `make extension_configuration` with the desired extension configuration before running the script"
        )
        exit(1)
    if not os.path.isfile(DUCKDB_PATH):
        print("please run `make release` with the desired extension configuration before running the script")
        exit(1)


# Parses the extension config files for which extension names there are to be expected
def get_extension_names() -> List[str]:
    extension_names = []
    with open(EXTENSIONS_PATH) as f:
        for line in f:
            extension_name = line.rstrip()
            if "jemalloc" in extension_name:
                # We skip jemalloc as it doesn't produce a loadable extension but is in the config
                continue
            extension_names.append(extension_name)
    return extension_names


def get_query(sql_query, load_query):
    # Optionally perform a LOAD of an extension
    # Then perform a SQL query, fetch the output
    query = f'{DUCKDB_PATH} -csv -unsigned -c "{load_query}{sql_query}" '
    query_result = os.popen(query).read()
    result = query_result.split("\n")[1:-1]
    return result


def get_functions(load="") -> Set[Function]:
    GET_FUNCTIONS_QUERY = """
        select distinct
            function_name,
            function_type
        from duckdb_functions();
    """
    # ['name_1,type_1', ..., 'name_n,type_n']
    results = set(get_query(GET_FUNCTIONS_QUERY, load))

    functions = set()
    for x in results:
        function_name, function_type = x.split(',')
        functions.add(Function(function_name, function_type))
    return functions


def get_settings(load=""):
    GET_SETTINGS_QUERY = """
        select distinct
            name
        from duckdb_settings();
    """
    return set(get_query(GET_SETTINGS_QUERY, load))


class ExtensionData:
    def __init__(self):
        # Map of extension -> ExtensionFunction
        self.function_map: Dict[str, ExtensionFunction] = {}
        # Map of extension -> ExtensionSetting
        self.settings_map: Dict[str, ExtensionSetting] = {}

        # Map of extension -> extension_path
        self.extensions: Dict[str, str] = get_extension_path_map()

        self.stored_functions: Dict[str, Function] = {
            'substrait': [
                Function("from_substrait", "table"),
                Function("get_substrait", "table"),
                Function("get_substrait_json", "table"),
                Function("from_substrait_json", "table"),
            ],
            'arrow': [Function("scan_arrow_ipc", "table"), Function("to_arrow_ipc", "table")],
            'spatial': [],
        }
        self.stored_settings: Dict[str, str] = {'substrait': [], 'arrow': [], 'spatial': []}

    def set_base(self):
        self.base_functions: Set[Function] = get_functions()
        self.base_settings: Set[str] = get_settings()

    def add_extension(self, extension_name: str):
        if extension_name in self.extensions:
            # Perform a LOAD and add the added settings/functions
            extension_path = self.extensions[extension_name]

            print(f"Load {extension_name} at {extension_path}")
            load = f"LOAD '{extension_path}';"

            extension_functions = get_functions(load)
            extension_settings = get_settings(load)

            extension_data.add_settings(extension_name, extension_settings)
            extension_data.add_functions(extension_name, extension_functions)
        elif extension_name in self.stored_functions or extension_name in self.stored_settings:
            # Retrieve the list of settings/functions from our hardcoded list
            extension_functions = self.stored_functions[extension_name]
            extension_settings = self.stored_settings[extension_name]

            print(f"Loading {extension_name} from stored functions: {extension_functions}")
            extension_data.add_settings(extension_name, extension_settings)
            extension_data.add_functions(extension_name, extension_functions)
        else:
            print(f"Missing extension {extension_name} and not found in stored_functions/stored_settings")
            exit(1)

    def add_settings(self, extension_name: str, settings_list: List[str]):
        extension_name = extension_name.lower()

        added_settings: Set[str] = set(settings_list) - self.base_settings
        settings_to_add: Dict[str, str] = {}
        for setting in added_settings:
            setting_name = setting.lower()
            settings_to_add[setting_name] = ExtensionSetting(extension_name, setting_name)

        self.settings_map.update(settings_to_add)

    def add_functions(self, extension_name: str, function_list: List[Function]):
        extension_name = extension_name.lower()

        added_functions: Set[Function] = set(function_list) - self.base_functions
        functions_to_add: Dict[str, ExtensionFunction] = {}
        for function in added_functions:
            function_name = function.name.lower()
            functions_to_add[function_name] = ExtensionFunction(extension_name, function_name, function.type)

        self.function_map.update(functions_to_add)


# Get the slice of the file containing the var (assumes // END_OF_<varname> comment after var)
def get_slice_of_file(var_name, file_str):
    begin = file_str.find(var_name)
    end = file_str.find("END_OF_" + var_name)
    return file_str[begin:end]


# Parses the extension_entries.hpp file
def parse_extension_entries(file_path):
    file = open(file_path, 'r')
    pattern = re.compile("{\"(.*?)\", \"(.*?)\"}[,}\n]")
    file_blob = file.read()

    # Get the extension functions
    ext_functions_file_blob = get_slice_of_file("EXTENSION_FUNCTIONS", file_blob)
    cur_function_map = dict(pattern.findall(ext_functions_file_blob))

    # Get the extension settings
    ext_settings_file_blob = get_slice_of_file("EXTENSION_SETTINGS", file_blob)
    cur_settings_map = dict(pattern.findall(ext_settings_file_blob))

    # Get the extension types
    ext_copy_functions_blob = get_slice_of_file("EXTENSION_COPY_FUNCTIONS", file_blob)
    cur_copy_functions_map = dict(pattern.findall(ext_copy_functions_blob))

    # Get the extension types
    ext_types_file_blob = get_slice_of_file("EXTENSION_TYPES", file_blob)
    cur_types_map = dict(pattern.findall(ext_types_file_blob))

    return {
        'functions': cur_function_map,
        'settings': cur_settings_map,
        'types': cur_types_map,
        'copy_functions': cur_copy_functions_map,
    }


def print_map_diff(d1, d2):
    s1 = set(d1.items())
    s2 = set(d2.items())
    diff = str(s1 ^ s2)
    print("Diff between maps: " + diff + "\n")


def get_extension_path_map() -> Dict[str, str]:
    extension_paths: Dict[str, str] = {}
    extension_dir = pathlib.Path('../build/release/extension')
    # extension_dir = pathlib.Path('/tmp/') / '**/*.duckdb_extension'
    for location in glob.iglob(str(extension_dir / '**/*.duckdb_extension'), recursive=True):
        name, _ = os.path.splitext(os.path.basename(location))
        print(f"Located extension: {name} in path: '{location}'")
        extension_paths[name] = location
    return extension_paths


if __name__ == '__main__':
    # check_prerequisites()

    functions = {}
    extension_names: List[str] = get_extension_names()

    extension_data = ExtensionData()
    # Collect the list of functions/settings without any extensions loaded
    extension_data.set_base()

    for extension_name in extension_names:
        # For every extension, add the functions/settings added by the extension
        extension_data.add_extension(extension_name)

    ext_hpp = os.path.join("..", "src", "include", "duckdb", "main", "extension_entries.hpp")
    if args.validate:
        parsed_entries = parse_extension_entries(ext_hpp)
        if function_map != parsed_entries['functions']:
            print("Function map mismatches:")
            print("Found in " + str(DUCKDB_PATH) + ": " + str(sorted(function_map)) + "\n")
            print("Parsed from extension_entries.hpp: " + str(parsed_entries['functions']) + "\n")
            print_map_diff(function_map, parsed_entries['functions'])
            exit(1)
        if settings_map != parsed_entries['settings']:
            print("Settings map mismatches:")
            print("Found: " + str(settings_map) + "\n")
            print("Parsed from extension_entries.hpp: " + str(parsed_entries['settings']) + "\n")
            print_map_diff(settings_map, parsed_entries['settings'])
            exit(1)

        print("All entries found: ")
        print(" > functions: " + str(len(parsed_entries['functions'])))
        print(" > settings:  " + str(len(parsed_entries['settings'])))
    else:
        # extension_functions
        file = open(ext_hpp, 'w')
        header = """//===----------------------------------------------------------------------===//
    //                         DuckDB
    //
    // duckdb/main/extension_entries.hpp
    //
    //
    //===----------------------------------------------------------------------===//

    #pragma once

    #include \"duckdb/common/unordered_map.hpp\"

    // NOTE: this file is generated by scripts/generate_extensions_function.py. Check out the check-load-install-extensions 
    //       job in .github/workflows/LinuxRelease.yml on how to use it 

    namespace duckdb { 

    struct ExtensionEntry {
        char name[48];
        char extension[48];
    };

    struct ExtensionFunctionEntry {
        char name[48];
        char extension[48];
        char type[48];
    };

    static constexpr ExtensionFunctionEntry EXTENSION_FUNCTIONS[] = {
    """
        file.write(header)
        # functions
        sorted_function = sorted(function_map)

        for function_name in sorted_function:
            extension_name, function_type = function_map[function_name]
            file.write("    {")
            file.write(f'"{function_name}", "{extension_name}", "{function_type}"')
            file.write("}, \n")
        file.write("}; // END_OF_EXTENSION_FUNCTIONS\n")

        # settings
        header = """
    static constexpr ExtensionEntry EXTENSION_SETTINGS[] = {
    """
        file.write(header)
        # Sort Function Map
        sorted_settings = sorted(settings_map)

        for settings_name in sorted_settings:
            file.write("    {")
            file.write(f'"{settings_name.lower()}", "{settings_map[settings_name]}"')
            file.write("}, \n")
        footer = """}; // END_OF_EXTENSION_SETTINGS
        
    // Note: these are currently hardcoded in scripts/generate_extensions_function.py
    // TODO: automate by passing though to script via duckdb 
    static constexpr ExtensionEntry EXTENSION_COPY_FUNCTIONS[] = {
        {"parquet", "parquet"}, 
        {"json", "json"}
    }; // END_OF_EXTENSION_COPY_FUNCTIONS

    // Note: these are currently hardcoded in scripts/generate_extensions_function.py
    // TODO: automate by passing though to script via duckdb 
    static constexpr ExtensionEntry EXTENSION_TYPES[] = {
        {"json", "json"}, 
        {"inet", "inet"}, 
        {"geometry", "spatial"}
    }; // END_OF_EXTENSION_TYPES

    // Note: these are currently hardcoded in scripts/generate_extensions_function.py
    // TODO: automate by passing though to script via duckdb
    static constexpr ExtensionEntry EXTENSION_COLLATIONS[] = {
        {"af", "icu"},    {"am", "icu"},    {"ar", "icu"},     {"ar_sa", "icu"}, {"as", "icu"},    {"az", "icu"},
        {"be", "icu"},    {"bg", "icu"},    {"bn", "icu"},     {"bo", "icu"},    {"br", "icu"},    {"bs", "icu"},
        {"ca", "icu"},    {"ceb", "icu"},   {"chr", "icu"},    {"cs", "icu"},    {"cy", "icu"},    {"da", "icu"},
        {"de", "icu"},    {"de_at", "icu"}, {"dsb", "icu"},    {"dz", "icu"},    {"ee", "icu"},    {"el", "icu"},
        {"en", "icu"},    {"en_us", "icu"}, {"eo", "icu"},     {"es", "icu"},    {"et", "icu"},    {"fa", "icu"},
        {"fa_af", "icu"}, {"ff", "icu"},    {"fi", "icu"},     {"fil", "icu"},   {"fo", "icu"},    {"fr", "icu"},
        {"fr_ca", "icu"}, {"fy", "icu"},    {"ga", "icu"},     {"gl", "icu"},    {"gu", "icu"},    {"ha", "icu"},
        {"haw", "icu"},   {"he", "icu"},    {"he_il", "icu"},  {"hi", "icu"},    {"hr", "icu"},    {"hsb", "icu"},
        {"hu", "icu"},    {"hy", "icu"},    {"id", "icu"},     {"id_id", "icu"}, {"ig", "icu"},    {"is", "icu"},
        {"it", "icu"},    {"ja", "icu"},    {"ka", "icu"},     {"kk", "icu"},    {"kl", "icu"},    {"km", "icu"},
        {"kn", "icu"},    {"ko", "icu"},    {"kok", "icu"},    {"ku", "icu"},    {"ky", "icu"},    {"lb", "icu"},
        {"lkt", "icu"},   {"ln", "icu"},    {"lo", "icu"},     {"lt", "icu"},    {"lv", "icu"},    {"mk", "icu"},
        {"ml", "icu"},    {"mn", "icu"},    {"mr", "icu"},     {"ms", "icu"},    {"mt", "icu"},    {"my", "icu"},
        {"nb", "icu"},    {"nb_no", "icu"}, {"ne", "icu"},     {"nl", "icu"},    {"nn", "icu"},    {"om", "icu"},
        {"or", "icu"},    {"pa", "icu"},    {"pa_in", "icu"},  {"pl", "icu"},    {"ps", "icu"},    {"pt", "icu"},
        {"ro", "icu"},    {"ru", "icu"},    {"sa", "icu"},     {"se", "icu"},    {"si", "icu"},    {"sk", "icu"},
        {"sl", "icu"},    {"smn", "icu"},   {"sq", "icu"},     {"sr", "icu"},    {"sr_ba", "icu"}, {"sr_me", "icu"},
        {"sr_rs", "icu"}, {"sv", "icu"},    {"sw", "icu"},     {"ta", "icu"},    {"te", "icu"},    {"th", "icu"},
        {"tk", "icu"},    {"to", "icu"},    {"tr", "icu"},     {"ug", "icu"},    {"uk", "icu"},    {"ur", "icu"},
        {"uz", "icu"},    {"vi", "icu"},    {"wae", "icu"},    {"wo", "icu"},    {"xh", "icu"},    {"yi", "icu"},
        {"yo", "icu"},    {"yue", "icu"},   {"yue_cn", "icu"}, {"zh", "icu"},    {"zh_cn", "icu"}, {"zh_hk", "icu"},
        {"zh_mo", "icu"}, {"zh_sg", "icu"}, {"zh_tw", "icu"},  {"zu", "icu"}}; // END_OF_EXTENSION_COLLATIONS

    // Note: these are currently hardcoded in scripts/generate_extensions_function.py
    // TODO: automate by passing though to script via duckdb
    static constexpr ExtensionEntry EXTENSION_FILE_PREFIXES[] = {
        {"http://", "httpfs"},
        {"https://", "httpfs"},
        {"s3://", "httpfs"},
        {"s3a://", "httpfs"},
        {"s3n://", "httpfs"},
        {"gcs://", "httpfs"},
        {"gs://", "httpfs"},
        {"r2://", "httpfs"}
    //    {"azure://", "azure"}
    }; // END_OF_EXTENSION_FILE_PREFIXES

    // Note: these are currently hardcoded in scripts/generate_extensions_function.py
    // TODO: automate by passing though to script via duckdb
    static constexpr ExtensionEntry EXTENSION_FILE_POSTFIXES[] = {
        {".parquet", "parquet"},
        {".json", "json"},
        {".jsonl", "json"},
        {".ndjson", "json"},
        {".shp", "spatial"},
        {".gpkg", "spatial"},
        {".fgb", "spatial"}
    }; // END_OF_EXTENSION_FILE_POSTFIXES

    // Note: these are currently hardcoded in scripts/generate_extensions_function.py
    // TODO: automate by passing though to script via duckdb
    static constexpr ExtensionEntry EXTENSION_FILE_CONTAINS[] = {
        {".parquet?", "parquet"},
        {".json?", "json"},
        {".ndjson?", ".jsonl?"},
        {".jsonl?", ".ndjson?"}
    }; // EXTENSION_FILE_CONTAINS

    // Note: these are currently hardcoded in scripts/generate_extensions_function.py
    // TODO: automate by passing though to script via duckdb
    static constexpr ExtensionEntry EXTENSION_SECRET_TYPES[] = {{"s3", "httpfs"},
                                                                {"r2", "httpfs"},
                                                                {"gcs", "httpfs"},
                                                                {"azure", "azure"}}; // EXTENSION_SECRET_TYPES
                                                                
                                                                
    // Note: these are currently hardcoded in scripts/generate_extensions_function.py
    // TODO: automate by passing though to script via duckdb
    static constexpr ExtensionEntry EXTENSION_SECRET_PROVIDERS[] = {{"s3/config", "httpfs"},
                                                                    {"gcs/config", "httpfs"},
                                                                    {"r2/config", "httpfs"},
                                                                    {"s3/credential_chain", "aws"},
                                                                    {"gcs/credential_chain", "aws"},
                                                                    {"r2/credential_chain", "aws"},
                                                                    {"azure/config", "azure"},
                                                                    {"azure/credential_chain", "azure"}}; // EXTENSION_SECRET_PROVIDERS

    static constexpr const char *AUTOLOADABLE_EXTENSIONS[] = {
    //    "azure",
        "autocomplete",
        "excel",
        "fts",
        "httpfs",
        // "inet", 
        // "icu",
        "json",
        "parquet",
        "sqlsmith",
        "tpcds",
        "tpch",
        "visualizer"
    }; // END_OF_AUTOLOADABLE_EXTENSIONS

    } // namespace duckdb"""
        file.write(footer)

        file.close()
