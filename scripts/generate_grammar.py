# use bison to generate the parser files
# the following version of bison is used:
# bison (GNU Bison) 2.3
import os
import subprocess
import re
import sys
from python_helpers import open_utf8

bison_location = "bison"
base_dir = 'third_party/libpg_query/grammar'
pg_dir = 'third_party/libpg_query'
namespace = 'duckdb_libpgquery'

counterexamples = False
run_update = False
verbose = False
for arg in sys.argv[1:]:
    if arg.startswith("--bison="):
        bison_location = arg.replace("--bison=", "")
    elif arg.startswith("--counterexamples"):
        counterexamples = True
    elif arg.startswith("--update"):
        run_update = True
    # allow a prefix to the source and target directories
    elif arg.startswith("--custom_dir_prefix"):
        base_dir = arg.split("=")[1] + base_dir
        pg_dir = arg.split("=")[1] + pg_dir
    elif arg.startswith("--namespace"):
        namespace = arg.split("=")[1]
    elif arg.startswith("--verbose"):
        verbose = True
    else:
        raise Exception(
            "Unrecognized argument: "
            + arg
            + ", expected --counterexamples, --bison=/loc/to/bison, --custom_dir_prefix, --namespace, --verbose"
        )

template_file = os.path.join(base_dir, 'grammar.y')
target_file = os.path.join(base_dir, 'grammar.y.tmp')
header_file = os.path.join(base_dir, 'grammar.hpp')
source_file = os.path.join(base_dir, 'grammar.cpp')
type_dir = os.path.join(base_dir, 'types')
rule_dir = os.path.join(base_dir, 'statements')
result_source = os.path.join(base_dir, 'grammar_out.cpp')
result_header = os.path.join(base_dir, 'grammar_out.hpp')
target_source_loc = os.path.join(pg_dir, 'src_backend_parser_gram.cpp')
target_header_loc = os.path.join(pg_dir, 'include/parser/gram.hpp')
kwlist_header = os.path.join(pg_dir, 'include/parser/kwlist.hpp')


# parse the keyword lists
def read_list_from_file(fname):
    with open_utf8(fname, 'r') as f:
        return [x.strip() for x in f.read().split('\n') if len(x.strip()) > 0]


kwdir = os.path.join(base_dir, 'keywords')
unreserved_keywords = read_list_from_file(os.path.join(kwdir, 'unreserved_keywords.list'))
colname_keywords = read_list_from_file(os.path.join(kwdir, 'column_name_keywords.list'))
func_name_keywords = read_list_from_file(os.path.join(kwdir, 'func_name_keywords.list'))
type_name_keywords = read_list_from_file(os.path.join(kwdir, 'type_name_keywords.list'))
reserved_keywords = read_list_from_file(os.path.join(kwdir, 'reserved_keywords.list'))


def strip_p(x):
    if x.endswith("_P"):
        return x[:-2]
    else:
        return x


unreserved_keywords.sort(key=lambda x: strip_p(x))
colname_keywords.sort(key=lambda x: strip_p(x))
func_name_keywords.sort(key=lambda x: strip_p(x))
type_name_keywords.sort(key=lambda x: strip_p(x))
reserved_keywords.sort(key=lambda x: strip_p(x))

statements = read_list_from_file(os.path.join(base_dir, 'statements.list'))
statements.sort()
if len(statements) < 0:
    print("Need at least one statement")
    exit(1)

# verify there are no duplicate keywords and create big sorted list of keywords
kwdict = {}
for kw in unreserved_keywords:
    kwdict[kw] = 'UNRESERVED_KEYWORD'

for kw in colname_keywords:
    kwdict[kw] = 'COL_NAME_KEYWORD'

for kw in func_name_keywords:
    kwdict[kw] = 'TYPE_FUNC_NAME_KEYWORD'

for kw in type_name_keywords:
    kwdict[kw] = 'TYPE_FUNC_NAME_KEYWORD'

for kw in reserved_keywords:
    kwdict[kw] = 'RESERVED_KEYWORD'

kwlist = [(x, kwdict[x]) for x in kwdict.keys()]
# sorting uppercase is different from lowercase: A-Z < _ < a-z
kwlist.sort(key=lambda x: strip_p(x[0].lower()))

# now generate kwlist.h
# PG_KEYWORD("abort", ABORT_P, UNRESERVED_KEYWORD)
kwtext = (
    """
namespace """
    + namespace
    + """ {
#define PG_KEYWORD(a,b,c) {a,b,c},

const PGScanKeyword ScanKeywords[] = {
"""
)
for tpl in kwlist:
    kwtext += 'PG_KEYWORD("%s", %s, %s)\n' % (strip_p(tpl[0]).lower(), tpl[0], tpl[1])
kwtext += (
    """
};

const int NumScanKeywords = lengthof(ScanKeywords);
} // namespace """
    + namespace
    + """
"""
)

with open_utf8(kwlist_header, 'w+') as f:
    f.write(kwtext)


# generate the final main.y.tmp file
# first read the template file
with open_utf8(template_file, 'r') as f:
    text = f.read()

# now perform a series of replacements in the file to construct the final yacc file


def get_file_contents(fpath, add_line_numbers=False):
    with open_utf8(fpath, 'r') as f:
        result = f.read()
        if add_line_numbers:
            return '#line 1 "%s"\n' % (fpath,) + result
        else:
            return result


# grammar.hpp
text = text.replace("{{{ GRAMMAR_HEADER }}}", get_file_contents(header_file, True))

# grammar.cpp
text = text.replace("{{{ GRAMMAR_SOURCE }}}", get_file_contents(source_file, True))

# keyword list
kw_token_list = "%token <keyword> " + " ".join([x[0] for x in kwlist])

text = text.replace("{{{ KEYWORDS }}}", kw_token_list)

# statements
stmt_list = "stmt: " + "\n\t| ".join(statements) + "\n\t| /*EMPTY*/\n\t{ $$ = NULL; }\n"
text = text.replace("{{{ STATEMENTS }}}", stmt_list)

# keywords
# keywords can EITHER be reserved, unreserved, or some combination of (col_name, type_name, func_name)
# that means duplicates are ONLY allowed between (col_name, type_name and func_name)
# having a keyword be both reserved and unreserved is an error
# as is having a keyword both reserved and col_name, for example
# verify that this is the case
reserved_dict = {}
unreserved_dict = {}
other_dict = {}
for r in reserved_keywords:
    if r in reserved_dict:
        print("Duplicate keyword " + r + " in reserved keywords")
        exit(1)
    reserved_dict[r] = True

for ur in unreserved_keywords:
    if ur in unreserved_dict:
        print("Duplicate keyword " + ur + " in unreserved keywords")
        exit(1)
    if ur in reserved_dict:
        print("Keyword " + ur + " is marked as both unreserved and reserved")
        exit(1)
    unreserved_dict[ur] = True


def add_to_other_keywords(kw, list_name):
    global unreserved_dict
    global reserved_dict
    global other_dict
    if kw in unreserved_dict:
        print("Keyword " + kw + " is marked as both unreserved and " + list_name)
        exit(1)
    if kw in reserved_dict:
        print("Keyword " + kw + " is marked as both reserved and " + list_name)
        exit(1)
    other_dict[kw] = True


for cr in colname_keywords:
    add_to_other_keywords(cr, "colname")

type_func_name_dict = {}
for tr in type_name_keywords:
    add_to_other_keywords(tr, "typename")
    type_func_name_dict[tr] = True

for fr in func_name_keywords:
    add_to_other_keywords(fr, "funcname")
    type_func_name_dict[fr] = True

type_func_name_keywords = list(type_func_name_dict.keys())
type_func_name_keywords.sort()

all_keywords = list(reserved_dict.keys()) + list(unreserved_dict.keys()) + list(other_dict.keys())
all_keywords.sort()

other_keyword = list(other_dict.keys())
other_keyword.sort()

kw_definitions = "unreserved_keyword: " + " | ".join(unreserved_keywords) + "\n"
kw_definitions += "col_name_keyword: " + " | ".join(colname_keywords) + "\n"
kw_definitions += "func_name_keyword: " + " | ".join(func_name_keywords) + "\n"
kw_definitions += "type_name_keyword: " + " | ".join(type_name_keywords) + "\n"
kw_definitions += "other_keyword: " + " | ".join(other_keyword) + "\n"
kw_definitions += "type_func_name_keyword: " + " | ".join(type_func_name_keywords) + "\n"
kw_definitions += "reserved_keyword: " + " | ".join(reserved_keywords) + "\n"
text = text.replace("{{{ KEYWORD_DEFINITIONS }}}", kw_definitions)


# types
def concat_dir(dname, extension, add_line_numbers=False):
    result = ""
    for fname in os.listdir(dname):
        fpath = os.path.join(dname, fname)
        if os.path.isdir(fpath):
            result += concat_dir(fpath, extension)
        else:
            if not fname.endswith(extension):
                continue
            result += get_file_contents(fpath, add_line_numbers)
    return result


type_definitions = concat_dir(type_dir, ".yh")
# add statement types as well
for stmt in statements:
    type_definitions += "%type <node> " + stmt + "\n"

text = text.replace("{{{ TYPES }}}", type_definitions)

# grammar rules
grammar_rules = concat_dir(rule_dir, ".y", True)

text = text.replace("{{{ GRAMMAR RULES }}}", grammar_rules)

# finally write the yacc file into the target file
with open_utf8(target_file, 'w+') as f:
    f.write(text)

# generate the bison
cmd = [bison_location]
if counterexamples:
    print("Attempting to print counterexamples (-Wcounterexamples)")
    cmd += ["-Wcounterexamples"]
if run_update:
    cmd += ["--update"]
if verbose:
    cmd += ["--verbose"]
cmd += ["-o", result_source, "-d", target_file]
print(' '.join(cmd))
proc = subprocess.Popen(cmd, stderr=subprocess.PIPE)
res = proc.wait(timeout=10)  # ensure CI does not hang as was seen when running with Bison 3.x release.

if res != 0:
    text = proc.stderr.read().decode('utf8')
    print(text)
    if 'shift/reduce' in text and not counterexamples:
        print("---------------------------------------------------------------------")
        print("In case of shift/reduce conflicts, try re-running with --counterexamples")
        print("Note: this requires a more recent version of Bison (e.g. version 3.8)")
        print("On a Macbook you can obtain this using \"brew install bison\"")
    if counterexamples and 'time limit exceeded' in text:
        print("---------------------------------------------------------------------")
        print(
            "The counterexamples time limit was exceeded. This likely means that no useful counterexample was generated."
        )
        print("")
        print("The counterexamples time limit can be increased by setting the TIME_LIMIT environment variable, e.g.:")
        print("export TIME_LIMIT=100")
    exit(1)


os.rename(result_source, target_source_loc)
os.rename(result_header, target_header_loc)

with open_utf8(target_source_loc, 'r') as f:
    text = f.read()

text = text.replace('#include "grammar_out.hpp"', '#include "include/parser/gram.hpp"')
text = text.replace('yynerrs = 0;', 'yynerrs = 0; (void)yynerrs;')

with open_utf8(target_source_loc, 'w+') as f:
    f.write(text)
