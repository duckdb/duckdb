from .config import SettingsList, VALID_SCOPE_VALUES, find_start_end_indexes, write_content_to_file

# markers
START_MARKER = r'static const ConfigurationOption internal_options\[\] = \{'
END_MARKER = r',\s*FINAL_SETTING};'


# generate the scope code for the ConfigurationOption array and insert into the config file
def generate_scope_code(file):
    with open(file, 'r') as source_file:
        source_code = source_file.read()

    # find the start and end indexes of the settings' scope array
    start_index, end_index = find_start_end_indexes(source_code, START_MARKER, END_MARKER, file)

    # split source code into sections
    before_array = source_code[:start_index] + "\n    "
    after_array = source_code[end_index:]

    # generate new entries for the settings array
    new_entries = []
    for setting in SettingsList:
        if setting.scope in VALID_SCOPE_VALUES:  # valid setting_scope values
            new_entries.append(f"DUCKDB_{setting.scope}({setting.struct_name})")
            for alias in setting.aliases:
                new_entries.append(f"DUCKDB_{setting.scope}_ALIAS(\"{alias}\", {setting.struct_name})")
        else:
            raise ValueError(f"Setting {setting.name} has invalid input scope value")

    new_array_section = ',\n    '.join(new_entries)
    return before_array + new_array_section + after_array


def generate():
    from .config import DUCKDB_SETTINGS_SCOPE_FILE

    print(f"Updating {DUCKDB_SETTINGS_SCOPE_FILE}")
    new_content = generate_scope_code(DUCKDB_SETTINGS_SCOPE_FILE)
    write_content_to_file(new_content, DUCKDB_SETTINGS_SCOPE_FILE)


if __name__ == '__main__':
    raise ValueError("Please use 'generate_settings.py' instead of running the individual script(s)")
