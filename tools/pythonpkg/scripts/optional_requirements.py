import os
import subprocess
import argparse


def install_package(package_name, is_optional):
    try:
        subprocess.run(['pip', 'install', '--prefer-binary', package_name], check=True)
    except subprocess.CalledProcessError:
        if is_optional:
            print(f'WARNING: Failed to install (optional) "{package_name}", might require manual review')
            return
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import test dependencies')
    parser.add_argument('--exclude', action='append', help='Exclude a package from installation', default=[])

    args = parser.parse_args()

    # Failing to install this package does not constitute a build failure
    OPTIONAL_PACKAGES = ["pyarrow", "torch", "polars", "adbc_driver_manager", "tensorflow"]

    for package in args.exclude:
        if package not in OPTIONAL_PACKAGES:
            print(f"Unrecognized exclude list item '{package}', has to be one of {', '.join(OPTIONAL_PACKAGES)}")
            exit(1)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    requirements_path = os.path.join(script_dir, '..', 'requirements-dev.txt')

    content = open(requirements_path).read()
    packages = [x for x in content.split('\n') if x != '']

    result = []
    for package in packages:
        package_name = package.replace('=', '>').replace('<', '>').split('>')[0]
        if package_name in args.exclude:
            print(f"Skipping {package_name}, as set by the --exclude option")
            continue
        is_optional = package_name in OPTIONAL_PACKAGES
        install_package(package, is_optional)
