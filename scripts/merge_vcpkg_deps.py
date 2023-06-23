import json
import sys

# Pass vcpkg.json files to merge their dependencies and produce a single vcpkg.json with their
# combined & deduplicated dependencies. Note that this script is very dumb and some manual labour may be required
# to combine extensions from multiple builds

merged_dependencies = []

for file in sys.argv[1:]:
    f = open(file)
    data = json.load(f)
    merged_dependencies += data['dependencies']

deduplicated_dependencies = list(set(merged_dependencies))

data = {
    "description": f"Auto-generated vcpkg.json for combined DuckDB extension build",
    "dependencies": deduplicated_dependencies
}

# Print output
print("Writing to 'build/vcpkg_merged_manifest/vcpkg.json': ")
print(data["dependencies"])

with open('build/vcpkg_merged_manifest/vcpkg.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)