#!/usr/bin/env python3
"""Assemble a repository from separately downloaded extension artifacts."""

import argparse
import filecmp
import shutil
from pathlib import Path


def is_release_core(artifact: Path) -> bool:
    return artifact.name.startswith("release-core-extensions-")


def merge_extension_artifacts(artifacts_dir: Path, repository_dir: Path) -> None:
    artifacts = sorted(
        artifacts_dir.glob("*-extensions-*"),
        key=lambda artifact: (not is_release_core(artifact), artifact.name),
    )
    selected: dict[Path, tuple[Path, Path]] = {}
    for artifact in artifacts:
        for source in sorted(artifact.rglob("*")):
            if source.is_symlink():
                raise ValueError(f"Extension artifact contains a symlink: {source}")
            if not source.is_file():
                continue
            relative = source.relative_to(artifact)
            previous = selected.get(relative)
            if previous is not None:
                previous_artifact, previous_source = previous
                if is_release_core(previous_artifact) and not is_release_core(artifact):
                    print(f"Using {artifact.name}/{relative} instead of {previous_artifact.name}/{relative}")
                else:
                    if not filecmp.cmp(previous_source, source, shallow=False):
                        raise ValueError(
                            f"Conflicting extension artifact files for {relative}: "
                            f"{previous_artifact.name} and {artifact.name}"
                        )
                    continue
            selected[relative] = artifact, source

    if not selected:
        raise ValueError(f"No extension artifact files found in {artifacts_dir}")

    # Validate all collisions before creating a fresh repository; consumers only run after this step succeeds.
    repository_dir.mkdir(parents=True)
    for relative, (_, source) in sorted(selected.items()):
        destination = repository_dir / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    print(f"Assembled {len(selected)} files in {repository_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts_dir", type=Path)
    parser.add_argument("repository_dir", type=Path)
    args = parser.parse_args()
    merge_extension_artifacts(args.artifacts_dir, args.repository_dir)


if __name__ == "__main__":
    main()
