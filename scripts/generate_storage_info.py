import json
import os

scripts_dir = os.path.dirname(os.path.abspath(__file__))
VERSION_MAP_PATH = scripts_dir + "/../src/storage/version_map.json"
STORAGE_INFO_PATH = scripts_dir + "/../src/storage/storage_info.cpp"
START_MARKER = "// START OF {type} VERSION INFO"
END_MARKER = "// END OF {type} VERSION INFO"


def generate_version_info_array(storage_versions, type, name, default):
    result = []
    name_upper = name.upper()
    if 'latest' in storage_versions:
        latest_value = storage_versions['latest']
        result.append(f"const uint64_t LATEST_{name_upper} = {latest_value};")

    result.append(f"const uint64_t DEFAULT_{name_upper} = {default};")

    result.append(f"static const {type} {name}[] = {{")

    for version_name, storage_version in storage_versions.items():
        result.append(f'\t{{"{version_name}", {storage_version}}},')

    result.append("\t{nullptr, 0}")
    result.append("};\n")

    return "\n".join(result)


def main():

    with open(VERSION_MAP_PATH, 'r') as json_file:
        version_map = json.load(json_file)

    with open(STORAGE_INFO_PATH, "r") as cpp_file:
        content = cpp_file.read()

    for key in version_map['serialization']['values'].keys():
        if key in ['latest']:
            continue
        if key not in version_map['storage']['values'].keys():
            print(f'Key {key} found in serialization version but not in storage version')
            exit(1)
    types = ['storage', 'serialization']
    for type in version_map:
        if type not in types:
            print("Unexpected key {type}")
            exit(1)
        capitalized_type = type.capitalize()
        upper_type = type.upper()
        array_code = generate_version_info_array(
            version_map[type]['values'],
            f'{capitalized_type}VersionInfo',
            f'{type}_version_info',
            version_map[type]['default'],
        )

        start_marker = START_MARKER.format(type=upper_type)
        start_index = content.find(start_marker)
        if start_index == -1:
            print(f"storage_info.cpp is corrupted, could not find the START_MARKER for {type}")
            exit(1)

        end_marker = END_MARKER.format(type=upper_type)
        end_index = content.find(end_marker)
        if end_index == -1:
            print(f"storage_info.cpp is corrupted, could not find the END_MARKER for {type}")
            exit(1)
        content = content[: start_index + len(start_marker)] + "\n" + array_code + content[end_index:]

    with open(STORAGE_INFO_PATH, "w") as cpp_file:
        cpp_file.write(content)


if __name__ == "__main__":
    main()
