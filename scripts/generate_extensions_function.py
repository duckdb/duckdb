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
parser.add_argument(
    '--extension_dir',
    action='store',
    help="The root directory to look for the '<extension_name>/<extension>.duckdb_extension' files, relative to the location of this script",
    default='../build/release/repository',
)

args = parser.parse_args()

EXTENSIONS_PATH = os.path.join("..", "build", "extension_configuration", "extensions.csv")
DUCKDB_PATH = os.path.join("..", 'build', 'release', 'duckdb')
HEADER_PATH = os.path.join("..", "src", "include", "duckdb", "main", "extension_entries.hpp")

from enum import Enum


class CatalogType(str, Enum):
    SCALAR = "CatalogType::SCALAR_FUNCTION_ENTRY"
    TABLE = "CatalogType::TABLE_FUNCTION_ENTRY"
    AGGREGATE = "CatalogType::AGGREGATE_FUNCTION_ENTRY"
    PRAGMA = "CatalogType::PRAGMA_FUNCTION_ENTRY"
    MACRO = "CatalogType::MACRO_ENTRY"
    TABLE_MACRO = "CatalogType::TABLE_MACRO_ENTRY"


def catalog_type_from_type(catalog_type: str) -> CatalogType:
    TYPE_MAP = {
        CatalogType.SCALAR.value: CatalogType.SCALAR,
        CatalogType.TABLE.value: CatalogType.TABLE,
        CatalogType.AGGREGATE.value: CatalogType.AGGREGATE,
        CatalogType.PRAGMA.value: CatalogType.PRAGMA,
        CatalogType.MACRO.value: CatalogType.MACRO,
        CatalogType.TABLE_MACRO.value: CatalogType.TABLE_MACRO,
    }
    if catalog_type not in TYPE_MAP:
        raise Exception(f"Unrecognized function type: '{catalog_type}'")
    return TYPE_MAP[catalog_type]


def catalog_type_from_string(catalog_type: str) -> CatalogType:
    TYPE_MAP = {
        CatalogType.SCALAR.name.lower(): CatalogType.SCALAR,
        CatalogType.TABLE.name.lower(): CatalogType.TABLE,
        CatalogType.AGGREGATE.name.lower(): CatalogType.AGGREGATE,
        CatalogType.PRAGMA.name.lower(): CatalogType.PRAGMA,
        CatalogType.MACRO.name.lower(): CatalogType.MACRO,
        CatalogType.TABLE_MACRO.name.lower(): CatalogType.TABLE_MACRO,
    }
    if catalog_type not in TYPE_MAP:
        raise Exception(f"Unrecognized function type: '{catalog_type}'")
    return TYPE_MAP[catalog_type]


class Function(NamedTuple):
    name: str
    type: CatalogType


class ExtensionFunction(NamedTuple):
    extension: str
    name: str
    type: CatalogType

    @staticmethod
    def create_map(input: List[Tuple[str, str, str]]) -> Dict[Function, "ExtensionFunction"]:
        output: Dict[Function, "ExtensionFunction"] = {}
        for x in input:
            key = Function(x[0], catalog_type_from_type(x[2]))
            output[key] = ExtensionFunction(x[1], key.name, key.type)
        return output


class ExtensionSetting(NamedTuple):
    extension: str
    name: str

    @staticmethod
    def create_map(input: List[Tuple[str, str]]) -> Dict[str, "ExtensionSetting"]:
        output: Dict[str, "ExtensionSetting"] = {}
        for x in input:
            output[x[0]] = ExtensionSetting(x[1], x[0])
        return output


class ExtensionCopyFunction(NamedTuple):
    extension: str
    name: str

    @staticmethod
    def create_map(input: List[Tuple[str, str]]) -> Dict[str, "ExtensionCopyFunction"]:
        output: Dict[str, "ExtensionCopyFunction"] = {}
        for x in input:
            output[x[0]] = ExtensionCopyFunction(x[1], x[0])
        return output


class ExtensionType(NamedTuple):
    extension: str
    name: str

    @staticmethod
    def create_map(input: List[Tuple[str, str]]) -> Dict[str, "ExtensionType"]:
        output: Dict[str, "ExtensionType"] = {}
        for x in input:
            output[x[0]] = ExtensionType(x[1], x[0])
        return output


class ParsedEntries:
    def __init__(self, file_path):
        self.path = file_path
        self.functions = {}
        self.settings = {}
        self.types = {}
        self.copy_functions = {}

        def parse_contents(input) -> list:
            # Split the string by comma and remove any leading or trailing spaces
            elements = input.split(",")
            # Strip any leading or trailing spaces and surrounding double quotes from each element
            elements = [element.strip().strip('"') for element in elements]
            return elements

        file = open(file_path, 'r')
        pattern = re.compile("{(.*(?:, )?)}[,}\n]")
        file_blob = file.read()

        # Get the extension functions
        ext_functions_file_blob = get_slice_of_file("EXTENSION_FUNCTIONS", file_blob)
        res = pattern.findall(ext_functions_file_blob)
        res = [parse_contents(x) for x in res]
        res = [(x[0], x[1], x[2]) for x in res]
        self.functions = ExtensionFunction.create_map(res)

        # Get the extension settings
        ext_settings_file_blob = get_slice_of_file("EXTENSION_SETTINGS", file_blob)
        res = pattern.findall(ext_settings_file_blob)
        res = [parse_contents(x) for x in res]
        res = [(x[0], x[1]) for x in res]
        self.settings = ExtensionSetting.create_map(res)

        # Get the extension types
        ext_copy_functions_blob = get_slice_of_file("EXTENSION_COPY_FUNCTIONS", file_blob)
        res = pattern.findall(ext_copy_functions_blob)
        res = [parse_contents(x) for x in res]
        res = [(x[0], x[1]) for x in res]
        self.copy_functions = ExtensionCopyFunction.create_map(res)

        # Get the extension types
        ext_types_file_blob = get_slice_of_file("EXTENSION_TYPES", file_blob)
        res = pattern.findall(ext_types_file_blob)
        res = [parse_contents(x) for x in res]
        res = [(x[0], x[1]) for x in res]
        self.types = ExtensionType.create_map(res)

    def filter_entries(self, extensions: List[str]):
        self.functions = {k: v for k, v in self.functions.items() if v.extension not in extensions}
        self.copy_functions = {k: v for k, v in self.copy_functions.items() if v.extension not in extensions}
        self.settings = {k: v for k, v in self.settings.items() if v.extension not in extensions}
        self.types = {k: v for k, v in self.types.items() if v.extension not in extensions}


def check_prerequisites():
    if not os.path.isfile(EXTENSIONS_PATH) or not os.path.isfile(DUCKDB_PATH):
        print(
            "please run 'DISABLE_BUILTIN_EXTENSIONS=1 BUILD_ALL_EXT=1 make release', you might have to manually add DONT_LINK to all extension_configs"
        )
        exit(1)
    if not os.path.isdir(args.extension_dir):
        print(f"provided --extension_dir '{args.extension_dir}' is not a valid directory")
        exit(1)


# Parses the extension config files for which extension names there are to be expected
def get_extension_names() -> List[str]:
    extension_names = []
    with open(EXTENSIONS_PATH) as f:
        # Skip the csv header
        next(f)
        for line in f:
            extension_name = line.split(',')[0].rstrip()
            if "jemalloc" in extension_name:
                # We skip jemalloc as it doesn't produce a loadable extension but is in the config
                continue
            extension_names.append(extension_name)
    return extension_names


def get_query(sql_query, load_query) -> list:
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
        function_name, function_type = [y.lower() for y in x.split(',')]
        functions.add(Function(function_name, catalog_type_from_string(function_type)))
    return functions


def get_settings(load="") -> Set[str]:
    GET_SETTINGS_QUERY = """
        select distinct
            name
        from duckdb_settings();
    """
    return set(get_query(GET_SETTINGS_QUERY, load))


class ExtensionData:
    def __init__(self):
        # Map of extension -> ExtensionFunction
        self.function_map: Dict[Function, ExtensionFunction] = {}
        # Map of extension -> ExtensionSetting
        self.settings_map: Dict[str, ExtensionSetting] = {}

        # Map of extension -> extension_path
        self.extensions: Dict[str, str] = get_extension_path_map()

        self.stored_functions: Dict[str, List[Function]] = {
            'substrait': [
                Function("from_substrait", CatalogType.TABLE),
                Function("get_substrait", CatalogType.TABLE),
                Function("get_substrait_json", CatalogType.TABLE),
                Function("from_substrait_json", CatalogType.TABLE),
            ],
            'arrow': [Function("scan_arrow_ipc", CatalogType.TABLE), Function("to_arrow_ipc", CatalogType.TABLE)],
            'spatial': [],
        }
        self.stored_settings: Dict[str, List[str]] = {'substrait': [], 'arrow': [], 'spatial': []}

    def set_base(self):
        self.base_functions: Set[Function] = get_functions()
        self.base_settings: Set[str] = get_settings()

    def add_entries(self, entries: ParsedEntries):
        self.function_map.update(entries.functions)
        self.settings_map.update(entries.settings)

    def add_extension(self, extension_name: str):
        if extension_name in self.extensions:
            # Perform a LOAD and add the added settings/functions
            extension_path = self.extensions[extension_name]

            print(f"Load {extension_name} at {extension_path}")
            load = f"LOAD '{extension_path}';"

            extension_functions = list(get_functions(load))
            extension_settings = list(get_settings(load))

            self.add_settings(extension_name, extension_settings)
            self.add_functions(extension_name, extension_functions)
        elif extension_name in self.stored_functions or extension_name in self.stored_settings:
            # Retrieve the list of settings/functions from our hardcoded list
            extension_functions = self.stored_functions[extension_name]
            extension_settings = self.stored_settings[extension_name]

            print(f"Loading {extension_name} from stored functions: {extension_functions}")
            self.add_settings(extension_name, extension_settings)
            self.add_functions(extension_name, extension_functions)
        else:
            error = f"""Missing extension {extension_name} and not found in stored_functions/stored_settings
Please double check if '{args.extension_dir}' is the right location to look for ./**/*.duckdb_extension files"""
            print(error)
            exit(1)

    def add_settings(self, extension_name: str, settings_list: List[str]):
        extension_name = extension_name.lower()

        added_settings: Set[str] = set(settings_list) - self.base_settings
        settings_to_add: Dict[str, ExtensionSetting] = {}
        for setting in added_settings:
            setting_name = setting.lower()
            settings_to_add[setting_name] = ExtensionSetting(extension_name, setting_name)

        self.settings_map.update(settings_to_add)

    def add_functions(self, extension_name: str, function_list: List[Function]):
        extension_name = extension_name.lower()

        added_functions: Set[Function] = set(function_list) - self.base_functions
        functions_to_add: Dict[Function, ExtensionFunction] = {}
        for function in added_functions:
            functions_to_add[function] = ExtensionFunction(extension_name, function.name, function.type)

        self.function_map.update(functions_to_add)

    def validate(self):
        parsed_entries = ParsedEntries(HEADER_PATH)
        if self.function_map != parsed_entries.functions:
            print("Function map mismatches:")
            print_map_diff(self.function_map, parsed_entries.functions)
            exit(1)
        if self.settings_map != parsed_entries.settings:
            print("Settings map mismatches:")
            print_map_diff(self.settings_map, parsed_entries.settings)
            exit(1)

        print("All entries found: ")
        print(" > functions: " + str(len(parsed_entries.functions)))
        print(" > settings:  " + str(len(parsed_entries.settings)))

    def verify_export(self):
        if len(self.function_map) == 0 or len(self.settings_map) == 0:
            print(
                """
The provided configuration produced an empty function map or empty settings map
This is likely caused by building DuckDB with extensions linked in
"""
            )
            exit(1)

    def export_functions(self) -> str:
        result = """
    static constexpr ExtensionFunctionEntry EXTENSION_FUNCTIONS[] = {\n"""
        sorted_function = sorted(self.function_map)

        for func in sorted_function:
            function: ExtensionFunction = self.function_map[func]
            result += "\t{"
            result += f'"{function.name}", "{function.extension}", {function.type.value}'
            result += "},\n"
        result += "}; // END_OF_EXTENSION_FUNCTIONS\n"
        return result

    def export_settings(self) -> str:
        result = """
    static constexpr ExtensionEntry EXTENSION_SETTINGS[] = {\n"""
        sorted_settings = sorted(self.settings_map)

        for settings_name in sorted_settings:
            setting: ExtensionSetting = self.settings_map[settings_name]
            result += "\t{"
            result += f'"{settings_name.lower()}", "{setting.extension}"'
            result += "},\n"
        result += "}; // END_OF_EXTENSION_SETTINGS\n"
        return result


# Get the slice of the file containing the var (assumes // END_OF_<varname> comment after var)
def get_slice_of_file(var_name, file_str):
    begin = file_str.find(var_name)
    end = file_str.find("END_OF_" + var_name)
    return file_str[begin:end]


def print_map_diff(d1, d2):
    s1 = sorted(set(d1.items()))
    s2 = sorted(set(d2.items()))

    diff1 = str(set(s1) - set(s2))
    diff2 = str(set(s2) - set(s1))
    print("Diff between maps: " + diff1 + "\n")
    print("Diff between maps: " + diff2 + "\n")


def get_extension_path_map() -> Dict[str, str]:
    extension_paths: Dict[str, str] = {}
    # extension_dir = pathlib.Path('../build/release/extension')
    extension_dir = args.extension_dir
    for location in glob.iglob(extension_dir + '/**/*.duckdb_extension', recursive=True):
        name, _ = os.path.splitext(os.path.basename(location))
        print(f"Located extension: {name} in path: '{location}'")
        extension_paths[name] = location
    return extension_paths


def write_header(data: ExtensionData):
    INCLUDE_HEADER = """//===----------------------------------------------------------------------===//
    //                         DuckDB
    //
    // duckdb/main/extension_entries.hpp
    //
    //
    //===----------------------------------------------------------------------===//

    #pragma once

    #include \"duckdb/common/unordered_map.hpp\"
    #include \"duckdb/common/enums/catalog_type.hpp\"

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
        CatalogType type;
    };
    """

    INCLUDE_FOOTER = """
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
         {"http://", "httpfs"}, {"https://", "httpfs"}, {"s3://", "httpfs"}, {"s3a://", "httpfs"}, {"s3n://", "httpfs"},
         {"gcs://", "httpfs"},  {"gs://", "httpfs"},    {"r2://", "httpfs"}, {"azure://", "azure"}, {"az://", "azure"},
         {"abfss://", "azure"}, {"hf://", "httpfs"}
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
                                                                {"azure", "azure"},
                                                                {"huggingface", "httpfs"},
                                                                {"bearer", "httpfs"}
    }; // EXTENSION_SECRET_TYPES
                                                                
                                                                
    // Note: these are currently hardcoded in scripts/generate_extensions_function.py
    // TODO: automate by passing though to script via duckdb
    static constexpr ExtensionEntry EXTENSION_SECRET_PROVIDERS[] = {{"s3/config", "httpfs"},
                                                                    {"gcs/config", "httpfs"},
                                                                    {"r2/config", "httpfs"},
                                                                    {"s3/credential_chain", "aws"},
                                                                    {"gcs/credential_chain", "aws"},
                                                                    {"r2/credential_chain", "aws"},
                                                                    {"azure/access_token", "azure"},
                                                                    {"azure/config", "azure"},
                                                                    {"azure/credential_chain", "azure"}, 
                                                                    {"azure/service_principal", "azure"}, 
                                                                    {"huggingface/config", "httfps"},
                                                                    {"huggingface/credential_chain", "httpfs"}, 
                                                                    {"bearer/config", "httpfs"}
}; // EXTENSION_SECRET_PROVIDERS

    static constexpr const char *AUTOLOADABLE_EXTENSIONS[] = {
    "aws",
    "azure",
    "autocomplete",
    "delta",
    "excel",
    "fts",
    "httpfs",
    "inet",
    "icu",
    "json",
    "parquet",
    "sqlite_scanner",
    "sqlsmith",
    "postgres_scanner",
    "tpcds",
    "tpch"
    }; // END_OF_AUTOLOADABLE_EXTENSIONS

    } // namespace duckdb"""

    data.verify_export()

    file = open(HEADER_PATH, 'w')
    file.write(INCLUDE_HEADER)

    exported_functions = data.export_functions()
    file.write(exported_functions)

    exported_settings = data.export_settings()
    file.write(exported_settings)
    file.write(INCLUDE_FOOTER)
    file.close()


# Extensions that can be autoloaded, but are not buildable by DuckDB CI
HARDCODED_EXTENSION_FUNCTIONS = ExtensionFunction.create_map(
    [
        ("delta_scan", "delta", "CatalogType::TABLE_FUNCTION_ENTRY"),
    ]
)


def main():
    check_prerequisites()

    extension_names: List[str] = get_extension_names()

    extension_data = ExtensionData()
    # Collect the list of functions/settings without any extensions loaded
    extension_data.set_base()

    # TODO: add 'purge' option to ignore existing entries ??
    parsed_entries = ParsedEntries(HEADER_PATH)
    parsed_entries.filter_entries(extension_names)

    for extension_name in extension_names:
        print(extension_name)
        # For every extension, add the functions/settings added by the extension
        extension_data.add_extension(extension_name)

    # Add the entries we initially parsed from the HEADER_PATH
    extension_data.add_entries(parsed_entries)

    # Add hardcoded extension entries (
    for key, value in HARDCODED_EXTENSION_FUNCTIONS.items():
        extension_data.function_map[key] = value

    if args.validate:
        extension_data.validate()
        return

    write_header(extension_data)


if __name__ == '__main__':
    main()
