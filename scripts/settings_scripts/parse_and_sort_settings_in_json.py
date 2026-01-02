import json
from .config import Setting, SettingsList, JSON_PATH


# sort settings in json by name
def sort_json_data(path):
    with open(path, 'r') as file:
        data = json.load(file)
    sorted_data = sorted(data, key=lambda x: x['name'])
    with open(path, 'w') as file:
        json.dump(sorted_data, file, indent=4)
    return sorted_data


# parse json data and stores each entry as a settings object in the global list SettingsList
def add_all_settings_to_global_list():
    valid_entries = [
        'name',
        'description',
        'type',
        'scope',
        'internal_setting',
        'on_callbacks',
        'custom_implementation',
        'struct',
        'aliases',
        'default_scope',
        'default_value',
    ]

    print(f"Parsing and sorting the settings data in {JSON_PATH}")
    clear_global_settings_list()
    json_data = sort_json_data(JSON_PATH)
    # store all the settings in the SettingsList
    for entry in json_data:
        for field_entry in entry:
            if field_entry not in valid_entries:
                raise ValueError(
                    f"Found entry unexpected entry \"{field_entry}\" in setting, expected entry to be in {', '.join(valid_entries)}"
                )
        setting = Setting(
            name=entry['name'],
            description=entry['description'],
            sql_type=entry['type'],
            internal_setting=entry.get('internal_setting', entry['name']),
            scope=entry.get('scope', None),
            struct_name=entry.get('struct', ''),
            on_callbacks=entry.get('on_callbacks', []),
            custom_implementation=entry.get('custom_implementation', False),
            aliases=entry.get('aliases', []),
            default_scope=entry.get('default_scope', None),
            default_value=entry.get('default_value', None),
        )
        SettingsList.append(setting)


def clear_global_settings_list():
    SettingsList.clear()
