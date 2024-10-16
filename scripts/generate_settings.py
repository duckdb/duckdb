from settings_scripts import parse_and_sort_json_file, update_header_file, update_scopes, update_src_code
from settings_scripts.config import SettingsList, make_format

if __name__ == '__main__':
    parse_and_sort_json_file()
    update_header_file()
    update_scopes()
    update_src_code()
    make_format()
    print(f"- Successfully parsed and included {len(SettingsList)} setting(s)!")
