import json
from .config import Setting, SettingsList, JSON_PATH


# sort settings in json by name
def sort_and_read_json_data(path):
    with open(path, 'r') as file:
        data = json.load(file)
    sorted_data = sorted(data, key=lambda x: x['name'])
    with open(path, 'w') as file:
        json.dump(sorted_data, file, indent=4)
    return sorted_data


# parse json data and stores each entry as a settings object in the global list SettingsList
def add_all_settings_to_global_list():
    print(f"Parsing and sorting the settings data in {JSON_PATH}")
    clear_global_settings_list()
    json_data = sort_and_read_json_data(JSON_PATH)
    # store all the settings in the SettingsList
    for entry in json_data:
        add_verif_SET = (
            True
            if entry.get('add_verification_in_SET', False) or entry.get('add_verification_in_both_SetReset', False)
            else False
        )
        add_verif_RESET = (
            True
            if entry.get('add_verification_in_RESET', False) or entry.get('add_verification_in_both_SetReset', False)
            else False
        )
        setting = Setting(
            name=entry['name'],
            description=entry['description'],
            type=entry.get('type', ""),
            sql_type=entry['sql_type'],
            scope=entry['scope'],
            add_verification_in_SET=add_verif_SET,
            add_verification_in_RESET=add_verif_RESET,
            custom_value_conversion=entry.get('custom_conversion_and_validation', False),
            aliases=entry.get('aliases', []),
        )
        SettingsList.append(setting)


def clear_global_settings_list():
    SettingsList.clear()
