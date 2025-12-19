import json
import os

scripts_dir = os.path.dirname(os.path.abspath(__file__))
VERSION_MAP_PATH = os.path.join(scripts_dir, "../src/storage/version_map.json")
STORAGE_INFO_PATH = os.path.join(scripts_dir, "../src/storage/storage_info.cpp")
STORAGE_ENUM_PATH = os.path.join(scripts_dir, "../src/include/duckdb/storage/storage_info.hpp")

START_MARKER = "// START OF {type} VERSION INFO"
END_MARKER = "// END OF {type} VERSION INFO"


def to_enum_name(version_name):
    return version_name.upper().replace('.', '_')


def generate_storage_enum(storage_versions):
    result = []
    result.append("enum class StorageVersion : uint64_t {")
    current = ""
    for version_name, storage_version in storage_versions.items():
        if version_name == 'latest':
            continue
        result.append(f"    {to_enum_name(version_name)} = {storage_version},")
        current = storage_version

    latest = "LATEST"
    result.append(f"    {to_enum_name(latest)} = {current},")
    result.append("    INVALID = 0")
    result.append("};")
    return "\n".join(result)


def generate_serialization_enum(serialization_versions):
    result = []
    result.append("enum class SerializationVersionDeprecated : uint64_t {")
    current = ""
    for version_name, serialization_version in serialization_versions.items():
        if version_name == 'latest':
            continue
        result.append(f"    {to_enum_name(version_name)} = {serialization_version},")
        current = serialization_version

    latest = "LATEST"
    result.append(f"    {to_enum_name(latest)} = {current},")
    result.append("    INVALID = 0")
    result.append("};")
    return "\n".join(result)


def generate_storage_array(storage_versions):
    result = []
    result.append("static const StorageVersionInfo storage_version_info[] = {")

    current = ""
    for version_name, _ in storage_versions.items():
        if version_name == 'latest':
            continue
        result.append(f'\t{{"{version_name}", StorageVersion::{to_enum_name(version_name)}}},')
        current = version_name

    latest = "latest"
    result.append(f'\t{{"{latest}", StorageVersion::{to_enum_name(current)}}},')
    result.append("\t{nullptr, StorageVersion::INVALID}")
    result.append("};")
    return "\n".join(result)


def update_file(path, marker_type, new_content):
    if not os.path.exists(path):
        print(f"Error: {path} not found.")
        return
    with open(path, "r") as f:
        content = f.read()

    start_marker = START_MARKER.format(type=marker_type.upper())
    end_marker = END_MARKER.format(type=marker_type.upper())

    start_idx = content.find(start_marker)
    end_idx = content.find(end_marker)

    if start_idx == -1 or end_idx == -1:
        print(f"Markers for {marker_type} not found in {path}")
        return

    new_file_content = content[: start_idx + len(start_marker)] + "\n" + new_content + "\n" + content[end_idx:]
    with open(path, "w") as f:
        f.write(new_file_content)


def main():
    if not os.path.exists(VERSION_MAP_PATH):
        print(f"Error: Version map not found at {VERSION_MAP_PATH}")
        return

    with open(VERSION_MAP_PATH, 'r') as json_file:
        version_map = json.load(json_file)

    storage_values = version_map['storage']['values']
    serialization_values = version_map['serialization']['values']

    enum_code = generate_storage_enum(storage_values)
    update_file(STORAGE_ENUM_PATH, "ENUM", enum_code)

    enum_code_serialization = generate_serialization_enum(serialization_values)
    update_file(STORAGE_ENUM_PATH, "SER_ENUM", enum_code_serialization)

    array_code = generate_storage_array(storage_values)
    update_file(STORAGE_INFO_PATH, "STORAGE_ARRAY", array_code)

    print(f"Successfully updated all version info in {STORAGE_INFO_PATH}")


if __name__ == "__main__":
    main()
