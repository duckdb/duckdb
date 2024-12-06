import subprocess
import os
import argparse
import re

parser = argparse.ArgumentParser(description='Publish a Julia release.')
parser.add_argument(
    '--yggdrassil-fork',
    dest='yggdrassil',
    action='store',
    help='Fork of the Julia Yggdrassil repository (https://github.com/JuliaPackaging/Yggdrasil)',
    default='/Users/myth/Programs/Yggdrasil',
)
args = parser.parse_args()

if not os.path.isfile(os.path.join('tools', 'juliapkg', 'release.py')):
    print('This script must be run from the root DuckDB directory (i.e. `python3 tools/juliapkg/release.py`)')
    exit(1)


def run_syscall(syscall, ignore_failure=False):
    res = os.system(syscall)
    if ignore_failure:
        return
    if res != 0:
        print(f'Failed to execute {syscall}: got exit code {str(res)}')
        exit(1)


# helper script to generate a julia release
duckdb_path = os.getcwd()

# fetch the latest tags
os.system('git fetch upstream --tags')

proc = subprocess.Popen(['git', 'show-ref', '--tags'], stdout=subprocess.PIPE)
tags = [x for x in proc.stdout.read().decode('utf8').split('\n') if len(x) > 0 and 'master-builds' not in x]


def extract_tag(x):
    keys = x.split('refs/tags/')[1].lstrip('v').split('.')
    return int(keys[0]) * 10000000 + int(keys[1]) * 10000 + int(keys[2])


tags.sort(key=extract_tag)

# latest tag
splits = tags[-1].split(' ')
hash = splits[0]
tag = splits[1].replace('refs/tags/', '')
if tag[0] != 'v':
    print(f"Tag {tag} does not start with a v?")
    exit(1)

print(f'Creating a Julia release from the latest tag {tag} with commit hash {hash}')

print('> Creating a PR to the Yggdrassil repository (https://github.com/JuliaPackaging/Yggdrasil)')

os.chdir(args.yggdrassil)
run_syscall('git checkout master')
run_syscall('git pull upstream master')
run_syscall(f'git branch -D {tag}', True)
run_syscall(f'git checkout -b {tag}')
tarball_build = os.path.join('D', 'DuckDB', 'build_tarballs.jl')
with open(tarball_build, 'r') as f:
    text = f.read()

text = re.sub('\nversion = v["][0-9.]+["]\n', f'\nversion = v"{tag[1:]}"\n', text)
text = re.sub(
    'GitSource[(]["]https[:][/][/]github[.]com[/]duckdb[/]duckdb[.]git["][,] ["][a-zA-Z0-9]+["][)]',
    f'GitSource("https://github.com/duckdb/duckdb.git", "{hash}")',
    text,
)

with open(tarball_build, 'w+') as f:
    f.write(text)

run_syscall(f'git add {tarball_build}')
run_syscall(f'git commit -m "[DuckDB] Bump to {tag}"')
run_syscall(f'git push --set-upstream origin {tag}')
run_syscall(
    f'gh pr create --title "[DuckDB] Bump to {tag}" --repo "https://github.com/JuliaPackaging/Yggdrasil" --body ""'
)

print('PR has been created.\n')
print(f'Next up we need to bump the version and DuckDB_jll version to {tag} in `tools/juliapkg/Project.toml`')
print('This is not yet automated.')
print(
    '> After that PR is merged - we need to post a comment containing the text `@JuliaRegistrator register subdir=tools/juliapkg`'
)
print('> For example, see https://github.com/duckdb/duckdb/commit/0f0461113f3341135471805c9928c4d71d1f5874')
