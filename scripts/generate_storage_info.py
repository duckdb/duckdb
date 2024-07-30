import json
import os

scripts_dir = os.path.dirname(os.path.abspath(__file__))
VERSION_MAP_PATH = scripts_dir + "/../src/storage/version_map.json"
STORAGE_INFO_PATH = scripts_dir + "/../src/storage/storage_info.cpp"
START_MARKER = "// START OF {type} VERSION INFO"
END_MARKER = "// END OF {type} VERSION INFO"


def generate_version_info_array(storage_versions, type_and_name):

    result = []
    result.append(f"static const {type_and_name}[] = {{")

    for version_name, storage_version in storage_versions.items():
        result.append(f'\t{{"{version_name}", {storage_version}}},')

    result.append("\t{nullptr, 0}")
    result.append("};\n")

    return "\n".join(result)


def main():

    with open(VERSION_MAP_PATH, 'r') as json_file:
        version_map = json.load(json_file)
    storage_version_info = generate_version_info_array(
        version_map['storage'], 'StorageVersionInfo storage_version_info'
    )
    serialization_version_info = generate_version_info_array(
        version_map['serialization'], 'SerializationVersionInfo serialization_version_info'
    )

    with open(STORAGE_INFO_PATH, "r") as cpp_file:
        content = cpp_file.read()

    for type in version_map:
        capitalized_type = type.capitalize()
        upper_type = type.upper()
        array_code = generate_version_info_array(
            version_map[type], f'{capitalized_type}VersionInfo {type}_version_info'
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
