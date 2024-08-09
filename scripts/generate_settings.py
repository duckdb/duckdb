from settings_scripts import parse_and_sort_json, update_header_file, update_scopes, generate_funcs
from settings_scripts.config import SettingsList

if __name__ == '__main__':
    parse_and_sort_json()
    update_header_file()
    update_scopes()
    generate_funcs()
    print(f"Parsed and included {len(SettingsList)} setting(s)!")
