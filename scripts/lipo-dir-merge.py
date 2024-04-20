# Copyright (C) Falko Axmann. All rights reserved.
# Licensed under the GPL v3 license.
#
# This script merges two directories containing static libraries for
# two different architectures into one directory with universal binaries.
# Files that don't end in ".a" will just be copied over from the first directory.
#
# Run it like this:
#  `python3 lipo-dir-merge.py <arm64-dir-tree> <x64-dir-tree> <universal-output-dir>`

import sys
import shutil
import os
import subprocess

#
# Make sure we got enough arguments on the command line
#
if len(sys.argv) < 4:
    print("Not enough args")
    print(f"{sys.argv[0]} <primary directory> <other architecture source> <destination>")
    sys.exit(-1)

# This is where we take most of the files from
primary_path = sys.argv[1]
# This is the directory tree from which we take libraries of the alternative arch
secondary_path = sys.argv[2]
# This is where we copy stuff to
destination_path = sys.argv[3]


# Merge the libraries at `src1` and `src2` and create a
# universal binary at `dst`
def merge_libs(src1, src2, dst):
    subprocess.run(["lipo", "-create", src1, src2, "-output", dst])

# Find the library at `src` in the `secondary_path` and then
# merge the two versions, creating a universal binary at `dst`.
def find_and_merge_libs(src, dst):
    rel_path = os.path.relpath(src, primary_path)
    lib_in_secondary = os.path.join(secondary_path, rel_path)

    if os.path.exists(lib_in_secondary) == False:
        print("Lib not found in secondary source: {lib_in_secondary}")
        return
    
    merge_libs(src, lib_in_secondary, dst)

# Either copy the file at `src` to `dst`, or, if it is a static
# library, merge it with its version from `secondary_path` and
# write the universal binary to `dst`.
def copy_file_or_merge_libs(src, dst, *, follow_symlinks=True):
    _, file_ext = os.path.splitext(src)
    if file_ext == ".a":
        find_and_merge_libs(src, dst)
    else:
        shutil.copy2(src, dst, follow_symlinks=follow_symlinks)

# Use copytree to do most of the work, with our own `copy_function` doing a little bit
# of magic in case of static libraries.
shutil.copytree(primary_path, destination_path, copy_function=copy_file_or_merge_libs)