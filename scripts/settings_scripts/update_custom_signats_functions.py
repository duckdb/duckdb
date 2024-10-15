from .config import (
    SEPARATOR,
    SRC_CODE_START_MARKER,
    SRC_CODE_END_MARKER,
    SRC_CODE_IMPLEMENTATION_COMMENT,
    SettingsList,
    find_start_end_indexes,
    get_setting_heading,
)


def find_missing_settings(existing_settings):
    missing_settings = {
        setting.struct_name: setting for setting in SettingsList if setting.struct_name not in existing_settings.keys()
    }
    return missing_settings


def is_seperator(line):
    if SEPARATOR in line:
        return True
    return False


def is_namespace_end_pattern(line):
    if "} // namespace duckdb" in line:
        return True
    return False


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix) :]
    return text


def remove_suffix(text, suffix):
    if text.endswith(suffix):
        return text[: -len(suffix)]
    return text


def find_existing_settings_dict(custom_settings_path):
    settings_dict = {}
    current_setting = None
    with open(custom_settings_path, 'r') as file:
        lines = file.readlines()
        for line_num, line in enumerate(lines):
            if is_seperator(line) and is_seperator(lines[line_num + 2]):
                if current_setting is not None:
                    settings_dict[current_setting]['end_line'] = line_num - 1

                # start a new setting
                current_setting = (
                    remove_suffix(remove_prefix(lines[line_num + 1], "//"), "\n").replace(' ', '') + "Setting"
                )
                settings_dict[current_setting] = {'start_line': line_num, 'end_line': None}

            # check if the line indicates the end of the namespace
            if is_namespace_end_pattern(line):
                if current_setting is not None:
                    settings_dict[current_setting]['end_line'] = line_num - 1
                break  # stop parsing as we reached the end of the namespace
    return settings_dict


def get_set_custom_funcs(scope, setting):
    parameters = ""
    if scope == "Global":
        parameters = "DatabaseInstance *db, DBConfig &config, const Value &input"
    elif scope == "Local":
        parameters = "ClientContext &context, const Value &input"
    new_setting = f"void {setting.struct_name}::Set{scope}({parameters}) {{\n"
    new_setting += SRC_CODE_IMPLEMENTATION_COMMENT + "}\n\n"
    if scope == "Global":
        parameters = "DatabaseInstance *db, DBConfig &config"
    elif scope == "Local":
        parameters = "ClientContext &context"
    new_setting += f"void {setting.struct_name}::Reset{scope}({parameters}) {{\n"
    new_setting += SRC_CODE_IMPLEMENTATION_COMMENT + "}\n\n"
    return new_setting


def get_new_setting_def(setting):
    new_setting = ""
    if setting.scope == "GLOBAL" or setting.scope == "GLOBAL_LOCAL":
        new_setting += get_set_custom_funcs("Global", setting)
    if setting.scope == "LOCAL" or setting.scope == "GLOBAL_LOCAL":
        if setting.add_verification_in_SET:
            new_setting += (
                f"bool {setting.struct_name}::OnLocalSet(ClientContext &context, const Value &input) {{\n"
            )
            new_setting += SRC_CODE_IMPLEMENTATION_COMMENT + "}\n\n"
        if setting.add_verification_in_RESET:
            new_setting += f"bool {setting.struct_name}::OnLocalReset(ClientContext &context) {{\n"
            new_setting += SRC_CODE_IMPLEMENTATION_COMMENT + "}\n\n"
        new_setting += get_set_custom_funcs("Local", setting)
    new_setting += f"Value {setting.struct_name}::GetSetting(const ClientContext &context) {{\n"
    new_setting += SRC_CODE_IMPLEMENTATION_COMMENT + "}\n\n"
    return new_setting


def create_content_for_custom_funcs(custom_settings_path, existing_settings, missing_settings):
    """
    Generates the content of all the missing settings and place them in an alphabetically order in the custom_settings.cpp file.

    For each setting:
      - If the setting is missing, it generates the appropriate code.
      - For global or local settings, it adds the necessary VerifyDBInstanceSET and VerifyDBInstanceRESET functions.
      - If a custom value conversion is needed, the corresponding function is generated.
    - If the setting already exists, it copies its content from the source file.
    """

    keys1 = list(existing_settings.keys())
    keys2 = list(missing_settings.keys())
    combined_keys = sorted(keys1 + keys2)

    with open(custom_settings_path, 'r') as source_file:
        lines = source_file.readlines()

    new = 0
    old = 0
    new_content = ""
    for struct_name in combined_keys:
        if struct_name in missing_settings:
            setting = missing_settings[struct_name]
            # add the setting in the new content
            if setting.add_verification_in_SET or setting.add_verification_in_RESET or setting.custom_value_conversion:
                new_content += get_setting_heading(setting.struct_name)
                new += 1
            if setting.add_verification_in_SET:
                if setting.scope == "GLOBAL":
                    new_content += f"bool {setting.struct_name}::OnGlobalSet(DatabaseInstance *db, DBConfig &config, const Value &input) {{\n"
                    new_content += SRC_CODE_IMPLEMENTATION_COMMENT + "}\n\n"
                if setting.scope == "LOCAL":
                    new_content += f"bool {setting.struct_name}::OnLocalSet(ClientContext &context, const Value &input) {{\n"
                    new_content += SRC_CODE_IMPLEMENTATION_COMMENT + "}\n\n"
            if setting.add_verification_in_RESET:
                if setting.scope == "GLOBAL":
                    new_content += f"bool {setting.struct_name}::OnGlobalReset(DatabaseInstance *db, DBConfig &config) {{\n"
                    new_content += SRC_CODE_IMPLEMENTATION_COMMENT + "}\n\n"
                if setting.scope == "LOCAL":
                    new_content += f"bool {setting.struct_name}::OnLocalReset(ClientContext &context) {{\n"
                    new_content += SRC_CODE_IMPLEMENTATION_COMMENT + "}\n\n"
            if setting.custom_value_conversion:
                new_content += get_new_setting_def(setting)
        else:
            start_line = existing_settings[struct_name]['start_line']
            end_line = existing_settings[struct_name]['end_line']
            new_content += "".join(lines[start_line:end_line]) + "\n"
            old += 1
    return new_content, new, old


def find_start_end_indexes_in_this_file(source_code, file):
    start_index, end_index = find_start_end_indexes(source_code, SRC_CODE_START_MARKER, SRC_CODE_END_MARKER, file)
    first_separator_index = source_code.find(SEPARATOR, start_index) - 2
    if first_separator_index > -1:
        start_index = first_separator_index
    return start_index, end_index


def add_custom_functions(custom_settings_path):
    # returns a dict. with key: <setting.struct_name>, value: {'start_line': <num>, 'end_line': <num>} of all the setting in the custom_settings.cpp file
    existing_settings = find_existing_settings_dict(custom_settings_path)
    # returns a dict with key: <setting.struct_name>, value: <setting structure> with the settings struct names that miss from the existing_settings dict
    missing_settings = find_missing_settings(existing_settings)

    with open(custom_settings_path, 'r') as source_file:
        source_code = source_file.read()

    # split source code into sections
    start_index, end_index = find_start_end_indexes_in_this_file(source_code, custom_settings_path)
    start_section = source_code[: start_index + 1] + "\n"
    end_section = source_code[end_index:]

    new_content, new, old = create_content_for_custom_funcs(custom_settings_path, existing_settings, missing_settings)
    return start_section + new_content + end_section, new, old


if __name__ == '__main__':
    raise ValueError("Please use 'generate_settings.py' instead of running the individual script(s)")
