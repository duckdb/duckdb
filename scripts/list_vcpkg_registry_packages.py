import argparse
import requests

parser = argparse.ArgumentParser(description='Generate the list of packages provided by the registry at <baseline>.')
parser.add_argument(
    '--baseline',
    action='store',
    help='The baseline (git commit) of the vcpkg-duckdb-ports',
    required=True,
)
args = parser.parse_args()

GITHUB_API = "https://api.github.com/repos/duckdb/vcpkg-duckdb-ports/git/trees"


def main():
    # Get the tree recursively for the commit
    response = requests.get(f"{GITHUB_API}/{args.baseline}?recursive=1")
    response.raise_for_status()

    # Extract package names from ports directory
    packages = set()
    for item in response.json()['tree']:
        path = item['path']
        if path.startswith('ports/'):
            parts = path.split('/')
            if len(parts) > 2:
                packages.add(parts[1])
    print(sorted(list(packages)))


if __name__ == '__main__':
    main()
