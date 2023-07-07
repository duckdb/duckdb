import json
import sys

# Pass vcpkg.json files to merge their dependencies and produce a single vcpkg.json with their
# combined & deduplicated dependencies. Note that this script is very dumb and some manual merging may be required
# to combine extensions from multiple builds in the case of colliding dependencies.

# Also: note that due to the fact that the httpfs extension currently can not use the latest openssl version (3.1),
# we need to pin the openssl version requiring us to also pin the vcpkg version here. When updating the vcpkg git hash
# we probably want to change it here and in ('.github/actions/build_extensions/action.yml') at the same time

merged_dependencies = []
merged_overlay_ports = []


def prefix_overlay_ports(overlay_ports, path_to_vcpkg_json):
    def prefix_overlay_port(overlay_port):
        vcpkg_prefix_path = path_to_vcpkg_json[0:path_to_vcpkg_json.find("/vcpkg.json")]
        return vcpkg_prefix_path + overlay_port

    return map(prefix_overlay_port, overlay_ports)


for file in sys.argv[1:]:
    f = open(file)
    data = json.load(f)
    merged_dependencies += data['dependencies']
    if 'vcpkg-configuration' in data:
        if 'overlay-ports' in data['vcpkg-configuration']:
            merged_overlay_ports += prefix_overlay_ports(data['vcpkg-configuration']['overlay-ports'], file)

deduplicated_dependencies = list(set(merged_dependencies))


data = {
    "description": f"Auto-generated vcpkg.json for combined DuckDB extension build",
    "builtin-baseline": "501db0f17ef6df184fcdbfbe0f87cde2313b6ab1",
    "dependencies": deduplicated_dependencies,
    "overrides": [
        {
            "name": "openssl",
            "version": "3.0.8"
        }
    ]
}

if merged_overlay_ports:
    data['vcpkg-configuration'] = {
        'overlay-ports': merged_overlay_ports
    }

# Print output
print("Writing to 'build/extension_configuration/vcpkg.json': ")
print(data["dependencies"])

with open('build/extension_configuration/vcpkg.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)