from .config import SettingsList, find_start_end_indexes, write_content_to_file

# markers
START_MARKER = "// Start of the auto-generated list of settings structures"
END_MARKER = "// End of the auto-generated list of settings structures"
SEPARATOR = "//===--------------------------------------------------------------------===//\n"

"""
- generate the cpp definition for the setting, resulting in a struct like this:

struct <struct_name> {
	using SETTING_TYPE = <setting_type>;
	static constexpr const char *Name = <name>;
	static constexpr const char *Description = <description>;
	static constexpr const LogicalTypeId InputType = LogicalTypeId::<input_type>;
	static void Set<scope>(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void Reset<scope>(DatabaseInstance *db, DBConfig &config);
	static SETTING_TYPE GetSetting(const ClientContext &context);
	static void Verify(const Value &input);
};
"""


def extract_definition(setting) -> str:
    definition = (
        f"struct {setting.struct_name} {{\n"
        f"    using SETTING_TYPE = {setting.setting_type};\n"
        f"    static constexpr const char *Name = \"{setting.name}\";\n"
        f"    static constexpr const char *Description = \"{setting.description}\";\n"
        f"    static constexpr const LogicalTypeId InputType = {setting.input_type};\n"
    )
    if setting.scope == "GLOBAL" or setting.scope == "GLOBAL_LOCAL":
        definition += f"    static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);\n"
        definition += f"    static void ResetGlobal(DatabaseInstance *db, DBConfig &config);\n"
    if setting.scope == "LOCAL" or setting.scope == "GLOBAL_LOCAL":
        definition += f"    static void SetLocal(ClientContext &context, const Value &parameter);\n"
        definition += f"    static void ResetLocal(ClientContext &context);\n"
    definition += f"    static SETTING_TYPE GetSetting(const ClientContext &context);\n"
    if setting.verification:
        if setting.scope == "LOCAL" or setting.scope == "GLOBAL_LOCAL":
            definition += f"    static void Verify(ClientContext &context, const Value &input);\n"
        else:
            definition += f"    static void Verify(const Value &input);\n"
    definition += f"}};\n\n"
    return definition


# generate settings code for the the header file
def generate_content(header_file_path):
    with open(header_file_path, 'r') as source_file:
        source_code = source_file.read()

    # find start and end indexes of the auto-generated section
    start_index, end_index = find_start_end_indexes(source_code, START_MARKER, END_MARKER, header_file_path)

    # split source code into sections
    start_section = source_code[: start_index + 1] + SEPARATOR + "\n"
    end_section = SEPARATOR + source_code[end_index:]

    new_content = "".join(extract_definition(setting) for setting in SettingsList)
    return start_section + new_content + end_section


def generate():
    from .config import DUCKDB_SETTINGS_HEADER_FILE

    new_content = generate_content(DUCKDB_SETTINGS_HEADER_FILE)
    write_content_to_file(new_content, DUCKDB_SETTINGS_HEADER_FILE)
    print(f"- Updating {DUCKDB_SETTINGS_HEADER_FILE}")


if __name__ == '__main__':
    raise ValueError("Please use 'generate_settings.py' instead of running the individual script(s)")
