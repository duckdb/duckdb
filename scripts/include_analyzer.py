import amalgamation
import os
import re
import sys
import shutil
from python_helpers import open_utf8

include_counts = {}
include_chains = {}
cached_includes = {}


def analyze_include_file(fpath, already_included_files, prev_include=""):
    if fpath in already_included_files:
        return
    if fpath in amalgamation.always_excluded:
        return
    if fpath not in cached_includes:
        # print(fpath)
        with open_utf8(fpath, 'r') as f:
            text = f.read()
        (statements, includes) = amalgamation.get_includes(fpath, text)
        cached_includes[fpath] = includes
    else:
        includes = cached_includes[fpath]

    if fpath in include_counts:
        include_counts[fpath] += 1
    else:
        include_counts[fpath] = 1

    if fpath not in include_chains:
        include_chains[fpath] = {}
    if prev_include not in include_chains[fpath]:
        include_chains[fpath][prev_include] = 0
    include_chains[fpath][prev_include] += 1

    already_included_files.append(fpath)
    if fpath.endswith('.h') or fpath.endswith('.hpp'):
        prev_include = fpath
    for include in includes:
        analyze_include_file(include, already_included_files, prev_include)


def analyze_includes(dir):
    files = os.listdir(dir)
    files.sort()
    for fname in files:
        if fname in amalgamation.excluded_files:
            continue
        fpath = os.path.join(dir, fname)
        if os.path.isdir(fpath):
            analyze_includes(fpath)
        elif fname.endswith('.cpp') or fname.endswith('.c') or fname.endswith('.cc'):
            analyze_include_file(fpath, [])


for compile_dir in amalgamation.compile_directories:
    analyze_includes(compile_dir)

kws = []
for entry in include_counts.keys():
    kws.append([entry, include_counts[entry]])

kws.sort(key=lambda tup: -tup[1])
for k in range(0, len(kws)):
    include_file = kws[k][0]
    include_count = kws[k][1]
    print("------------------------------------------------------------")
    print(include_file + " (" + str(include_count) + ")")
    print("------------------------------------------------------------")
    print("FILE INCLUDED FROM:")
    chainkws = []
    for chain in include_chains[include_file]:
        chainkws.append([chain, include_chains[include_file][chain]])
        chainkws.sort(key=lambda tup: -tup[1])
    for l in range(0, min(5, len(chainkws))):
        print(chainkws[l])
