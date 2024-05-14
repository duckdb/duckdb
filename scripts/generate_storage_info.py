import json
import os

scripts_dir = os.path.dirname(os.path.abspath(__file__))
VERSION_MAP_PATH = scripts_dir + "/../src/storage/version_map.json"
STORAGE_INFO_PATH = scripts_dir + "/../src/storage/storage_info.cpp"
START_MARKER = "// START OF STORAGE VERSION INFO"
END_MARKER = "// END OF STORAGE VERSION INFO"


def generate_storage_version_info(json_file_path):
    with open(json_file_path, 'r') as json_file:
        version_map = json.load(json_file)

    result = []
    result.append("static const StorageVersionInfo storage_version_info[] = {")

    for version_name, storage_version in version_map.items():
        result.append(f'\t{{"{version_name}", {storage_version}}},')

    result.append("\t{nullptr, 0}")
    result.append("};\n")

    return "\n".join(result)


def main():
    storage_version_info = generate_storage_version_info(VERSION_MAP_PATH)

    with open(STORAGE_INFO_PATH, "r") as cpp_file:
        content = cpp_file.read()

    start_index = content.find(START_MARKER)
    end_index = content.find(END_MARKER) + len(END_MARKER)
    updated_content = content[: start_index + len(START_MARKER)] + "\n" + storage_version_info + content[end_index:]

    with open(STORAGE_INFO_PATH, "w") as cpp_file:
        cpp_file.write(updated_content)


if __name__ == "__main__":
    main()
