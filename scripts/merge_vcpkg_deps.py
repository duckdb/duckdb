import json
import sys

# Pass vcpkg.json files to merge their dependencies and produce a single vcpkg.json with their
# combined & deduplicated dependencies. Note that this script is very dumb and some manual merging may be required
# to combine extensions from multiple builds in the case of colliding dependencies.

# Also: note that due to the fact that the httpfs extension currently can not use the latest openssl version (3.1),
# we need to pin the openssl version requiring us to also pin the vcpkg version here. When updating the vcpkg git hash
# we probably want to change it here and in ('.github/actions/build_extensions/action.yml') at the same time

merged_dependencies = []

for file in sys.argv[1:]:
    f = open(file)
    data = json.load(f)
    merged_dependencies += data['dependencies']

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

# Print output
print("Writing to 'build/vcpkg_merged_manifest/vcpkg.json': ")
print(data["dependencies"])

with open('build/vcpkg_merged_manifest/vcpkg.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)