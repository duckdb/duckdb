import os
import json

# define paths
BASE_DIR = os.path.dirname(__file__)
JSON_PATH = os.path.join(BASE_DIR, "..", "src/common", "settings.json")
DUCKDB_SETTINGS_HEADER_FILE = os.path.join(BASE_DIR, "..", "src/include/duckdb/main", "settings.hpp")
DUCKDB_LOGICAL_TYPES_FILE = os.path.join(BASE_DIR, "..", "src/include/duckdb/common", "types.hpp")
DUCKDB_SETTINGS_SCOPE_FILE = os.path.join(BASE_DIR, "..", "src/main", "config.cpp")

# markers
START_MARKER = "// Start of the auto-generated list of settings definitions"
END_MARKER = "// End of the auto-generated list of settings definitions"
SEPARATOR = "//===--------------------------------------------------------------------===//\n"


def generate_cpp_code(setting):
    name = setting["name"]
    description = setting["description"]
    input_type = setting.get("input_type", "")
    setting_type = setting.get("type", "")
    scope = setting["scope"]
    verification = setting["verification"]

    cpp_code = f"//===--------------------------------------------------------------------===//\n"
    cpp_code += f"// {name.replace('_', ' ').title()}\n"
    cpp_code += f"//===--------------------------------------------------------------------===//\n"
    cpp_code += f"void {name.capitalize()}Setting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {{\n"
    
    if scope == "global":
        if input_type == "BOOLEAN":
            cpp_code += f"    config.options.{name} = input.GetValue<bool>();\n"
        elif input_type == "VARCHAR":
            cpp_code += f"    auto parameter = StringUtil::Lower(input.ToString());\n"
            if name == "access_mode":
                cpp_code += f"    if (parameter == \"automatic\") {{\n"
                cpp_code += f"        config.options.{name} = AccessMode::AUTOMATIC;\n"
                cpp_code += f"    }} else if (parameter == \"read_only\") {{\n"
                cpp_code += f"        config.options.{name} = AccessMode::READ_ONLY;\n"
                cpp_code += f"    }} else if (parameter == \"read_write\") {{\n"
                cpp_code += f"        config.options.{name} = AccessMode::READ_WRITE;\n"
                cpp_code += f"    }} else {{\n"
                cpp_code += f"        throw InvalidInputException(\n"
                cpp_code += f"            \"Unrecognized parameter for option {name.upper()} '%s'. Expected READ_ONLY or READ_WRITE.\", parameter);\n"
                cpp_code += f"    }}\n"
            else:
                cpp_code += f"    config.options.{name} = parameter;\n"
        else:
            cpp_code += f"    // Add other input types handling here...\n"

    cpp_code += f"}}\n\n"

    cpp_code += f"void {name.capitalize()}Setting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {{\n"
    cpp_code += f"    config.options.{name} = DBConfig().options.{name};\n"
    cpp_code += f"}}\n\n"

    cpp_code += f"Value {name.capitalize()}Setting::GetSetting(const ClientContext &context) {{\n"
    cpp_code += f"    auto &config = DBConfig::GetConfig(context);\n"
    if input_type == "BOOLEAN":
        cpp_code += f"    return Value::BOOLEAN(config.options.{name});\n"
    elif input_type == "VARCHAR":
        if name == "access_mode":
            cpp_code += f"    switch (config.options.{name}) {{\n"
            cpp_code += f"    case AccessMode::AUTOMATIC:\n"
            cpp_code += f"        return \"automatic\";\n"
            cpp_code += f"    case AccessMode::READ_ONLY:\n"
            cpp_code += f"        return \"read_only\";\n"
            cpp_code += f"    case AccessMode::READ_WRITE:\n"
            cpp_code += f"        return \"read_write\";\n"
            cpp_code += f"    default:\n"
            cpp_code += f"        throw InternalException(\"Unknown {name} setting\");\n"
            cpp_code += f"    }}\n"
        else:
            cpp_code += f"    return Value(config.options.{name});\n"
    cpp_code += f"}}\n"

    return cpp_code

def generate():
    # Iterate over each setting and generate the corresponding C++ code
    with open(JSON_PATH, 'r') as json_file:
        json_data = json.load(json_file)
        for entry in json_data:
            cpp_code = generate_cpp_code(entry)
            print(cpp_code)

    # TODO: write
    # with open(DUCKDB_SETTINGS_SCOPE_FILE, 'w') as source_file:
    #     source_file.write("".join(new_content))


if __name__ == '__main__':
    raise ValueError("Please use 'generate_settings.py' instead of running the individual script(s)")

