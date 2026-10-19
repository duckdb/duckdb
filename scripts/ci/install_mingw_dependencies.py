import os
import subprocess
from pathlib import Path


def main():
    chocolatey_bin = Path(os.environ["ChocolateyInstall"]) / "bin"
    tools = ("ccache", "make")
    subprocess.run(["choco", "install", *tools, "--no-progress"], check=True)

    # A failed feed lookup can report success without installing the tools.
    for tool in tools:
        subprocess.run([str(chocolatey_bin / f"{tool}.exe"), "--version"], check=True)


if __name__ == "__main__":
    main()
