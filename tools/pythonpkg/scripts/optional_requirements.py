import os
import subprocess


def install_package(package_name, is_optional):
    try:
        subprocess.run(['pip', 'install', '--prefer-binary', package_name], check=True)
    except subprocess.CalledProcessError:
        if is_optional:
            print(f'WARNING: Failed to install (optional) "{package_name}", might require manual review')
            return
        raise


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    requirements_path = os.path.join(script_dir, '..', 'requirements-dev.txt')

    content = open(requirements_path).read()
    packages = [x for x in content.split('\n') if x != '']

    # Failing to install this package does not constitute a build failure
    optional_packages = ["pyarrow", "torch", "polars", "adbc_driver_manager", "tensorflow"]

    result = []
    for package in packages:
        package_name = package.replace('=', '>').split('>')[0]
        is_optional = package_name in optional_packages
        install_package(package, is_optional)
