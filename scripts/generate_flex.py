# use flex to generate the scanner file for the parser
# the following version of bison is used:
# flex 2.5.35 Apple(flex-32)
import os
import subprocess
import re
from sys import platform
import sys
from python_helpers import open_utf8

flex_bin = 'flex'
pg_path = os.path.join('third_party', 'libpg_query')
namespace = 'duckdb_libpgquery'

for arg in sys.argv[1:]:
    if arg.startswith("--flex="):
        flex_bin = arg.replace("--flex=", "")
    elif arg.startswith("--custom_dir_prefix"):
        pg_path = arg.split("=")[1] + pg_path
    elif arg.startswith("--namespace"):
        namespace = arg.split("=")[1]
    else:
        raise Exception("Unrecognized argument: " + arg + ", expected --flex, --custom_dir_prefix, --namespace")

flex_file_path = os.path.join(pg_path, 'scan.l')
target_file = os.path.join(pg_path, 'src_backend_parser_scan.cpp')

proc = subprocess.Popen(
    [flex_bin, '--nounistd', '-o', target_file, flex_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE
)
stdout = proc.stdout.read().decode('utf8')
stderr = proc.stderr.read().decode('utf8')
if proc.returncode != None or len(stderr) > 0:
    print("Flex failed")
    print("stdout: ", stdout)
    print("stderr: ", stderr)
    exit(1)

with open_utf8(target_file, 'r') as f:
    text = f.read()

# convert this from 'int' to 'yy_size_t' to avoid triggering a warning
text = text.replace('int yy_buf_size;\n', 'yy_size_t yy_buf_size;\n')

# add the libpg_query namespace
text = text.replace(
    '''
#ifndef FLEXINT_H
#define FLEXINT_H
''',
    '''
#ifndef FLEXINT_H
#define FLEXINT_H
namespace '''
    + namespace
    + ''' {
''',
)
text = text.replace('register ', '')

text = text + "\n} /* " + namespace + " */\n"

text = re.sub('(?:[(]void[)][ ]*)?fprintf', '//', text)
text = re.sub('exit[(]', 'throw std::runtime_error(msg); //', text)
text = re.sub(r'\n\s*if\s*[(]\s*!\s*yyin\s*[)]\s*\n\s*yyin\s*=\s*stdin;\s*\n', '\n', text)
text = re.sub(r'\n\s*if\s*[(]\s*!\s*yyout\s*[)]\s*\n\s*yyout\s*=\s*stdout;\s*\n', '\n', text)

file_null = 'NULL' if platform == 'linux' else '[(]FILE [*][)] 0'

text = re.sub(
    rf'[#]ifdef\s*YY_STDINIT\n\s*yyin = stdin;\n\s*yyout = stdout;\n[#]else\n\s*yyin = {file_null};\n\s*yyout = {file_null};\n[#]endif',
    '    yyin = (FILE *) 0;\n    yyout = (FILE *) 0;',
    text,
)

if 'stdin;' in text:
    print("STDIN not removed!")
    # exit(1)

if 'stdout' in text:
    print("STDOUT not removed!")
    # exit(1)

if 'fprintf(' in text:
    print("PRINTF not removed!")
    # exit(1)

if 'exit(' in text:
    print("EXIT not removed!")
    # exit(1)

with open_utf8(target_file, 'w+') as f:
    f.write(text)
