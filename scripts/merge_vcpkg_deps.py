import json
import sys

# Pass vcpkg.json files to merge their dependencies and produce a single vcpkg.json with their
# combined & deduplicated dependencies. Note that this script is very dumb and some manual merging may be required
# to combine extensions from multiple builds in the case of colliding dependencies.

# Also: note that due to the fact that the httpfs extension currently can not use the latest openssl version (3.1),
# we need to pin the openssl version requiring us to also pin the vcpkg version here. When updating the vcpkg git hash
# we probably want to change it here and in ('.github/actions/build_extensions/action.yml') at the same time

dependencies_str = []
dependencies_dict = []
merged_overlay_ports = []


def prefix_overlay_ports(overlay_ports, path_to_vcpkg_json):
    def prefix_overlay_port(overlay_port):
        vcpkg_prefix_path = path_to_vcpkg_json[0 : path_to_vcpkg_json.find("/vcpkg.json")]
        return vcpkg_prefix_path + overlay_port

    return map(prefix_overlay_port, overlay_ports)


for file in sys.argv[1:]:
    f = open(file)
    data = json.load(f)

    if 'dependencies' in data:
        for dep in data['dependencies']:
            if type(dep) is str:
                dependencies_str.append(dep)
            elif type(dep) is dict:
                dependencies_dict.append(dep)
            else:
                raise Exception(f"Unknown entry type found in dependencies: '{dep}'")

    if 'vcpkg-configuration' in data:
        if 'overlay-ports' in data['vcpkg-configuration']:
            merged_overlay_ports += prefix_overlay_ports(data['vcpkg-configuration']['overlay-ports'], file)

final_deduplicated_deps = list()
dedup_set = set()

for dep in dependencies_dict:
    if dep['name'] not in dedup_set:
        final_deduplicated_deps.append(dep)
        # TODO: deduplication is disabled for now, just let vcpkg handle duplicates in deps
        # dedup_set.add(dep['name'])

for dep in dependencies_str:
    if dep not in dedup_set:
        final_deduplicated_deps.append(dep)
        # TODO: deduplication is disabled for now, just let vcpkg handle duplicates in deps
        # dedup_set.add(dep)

data = {
    "description": f"Auto-generated vcpkg.json for combined DuckDB extension build",
    "builtin-baseline": "a1a1cbc975abf909a6c8985a6a2b8fe20bbd9bd6",
    "dependencies": final_deduplicated_deps,
    "overrides": [{"name": "openssl", "version": "3.0.8"}],
}

if merged_overlay_ports:
    data['vcpkg-configuration'] = {'overlay-ports': merged_overlay_ports}

# Print output
print("Writing to 'build/extension_configuration/vcpkg.json': ")
print(data["dependencies"])

with open('build/extension_configuration/vcpkg.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)
