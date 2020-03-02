# use bison to generate the parser files
# the following version of bison is used:
# bison (GNU Bison) 2.3
import os, subprocess, re

bison_location     = "bison"
base_dir           = 'third_party/libpg_query/grammar'
pg_dir             = 'third_party/libpg_query'
template_file      = os.path.join(base_dir, 'grammar.y')
target_file        = os.path.join(base_dir, 'grammar.y.tmp')
header_file        = os.path.join(base_dir, 'grammar.hpp')
source_file        = os.path.join(base_dir, 'grammar.cpp')
type_dir           = os.path.join(base_dir, 'types')
rule_dir           = os.path.join(base_dir, 'statements')
result_source      = os.path.join(base_dir, 'grammar_out.cpp')
result_header      = os.path.join(base_dir, 'grammar_out.hpp')
target_source_loc  = os.path.join(pg_dir, 'src_backend_parser_gram.cpp')
target_header_loc  = os.path.join(pg_dir, 'include/parser/gram.hpp')
kwlist_header      = os.path.join(pg_dir, 'include/parser/kwlist.hpp')

# parse the keyword lists
def read_list_from_file(fname):
    with open(fname, 'r') as f:
        return f.read().split('\n')

kwdir = os.path.join(base_dir, 'keywords')
unreserved_keywords = read_list_from_file(os.path.join(kwdir, 'unreserved_keywords.list'))
colname_keywords = read_list_from_file(os.path.join(kwdir, 'column_name_keywords.list'))
type_func_keywords = read_list_from_file(os.path.join(kwdir, 'type_func_name_keywords.list'))
reserved_keywords = read_list_from_file(os.path.join(kwdir, 'reserved_keywords.list'))

def strip_p(x):
    if x.endswith("_P"):
        return x[:-2]
    else:
        return x

unreserved_keywords.sort(key=lambda x: strip_p(x))
colname_keywords.sort(key=lambda x: strip_p(x))
type_func_keywords.sort(key=lambda x: strip_p(x))
reserved_keywords.sort(key=lambda x: strip_p(x))

statements = read_list_from_file(os.path.join(base_dir, 'statements.list'))
statements.sort()
if len(statements) < 0:
    print("Need at least one statement")
    exit(1)

# verify there are no duplicate keywords and create big sorted list of keywords
kwdict = {}
for kw in zip(unreserved_keywords, colname_keywords, type_func_keywords, reserved_keywords):
    if kw in kwdict:
        print("Duplicate keyword: " + kw)
        exit(1)
    kwdict[kw] = True

kwlist = []
kwlist += [(x, 'UNRESERVED_KEYWORD') for x in unreserved_keywords]
kwlist += [(x, 'COL_NAME_KEYWORD') for x in colname_keywords]
kwlist += [(x, 'TYPE_FUNC_NAME_KEYWORD') for x in type_func_keywords]
kwlist += [(x, 'RESERVED_KEYWORD') for x in reserved_keywords]
kwlist.sort(key=lambda x: strip_p(x[0]))

# now generate kwlist.h
# PG_KEYWORD("abort", ABORT_P, UNRESERVED_KEYWORD)
kwtext = """
#define PG_KEYWORD(a,b,c) {a,b,c},

const PGScanKeyword ScanKeywords[] = {
"""
for tpl in kwlist:
    kwtext += 'PG_KEYWORD("%s", %s, %s)\n' % (strip_p(tpl[0]).lower(), tpl[0], tpl[1])
kwtext += """
};

const int NumScanKeywords = lengthof(ScanKeywords);
"""

with open(kwlist_header, 'w+') as f:
    f.write(kwtext)


# generate the final main.y.tmp file
# first read the template file
with open(template_file, 'r') as f:
    text = f.read()

# now perform a series of replacements in the file to construct the final yacc file

def get_file_contents(fpath, add_line_numbers=False):
    with open(fpath, 'r') as f:
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
kw_definitions = "unreserved_keyword: " + " | ".join(unreserved_keywords) + "\n"
kw_definitions += "col_name_keyword: " + " | ".join(colname_keywords) + "\n"
kw_definitions += "type_func_name_keyword: " + " | ".join(type_func_keywords) + "\n"
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
with open(target_file, 'w+') as f:
    f.write(text)

# generate the bison
cmd = [bison_location, "-o", result_source, "-d", target_file]
print(' '.join(cmd))
proc = subprocess.Popen(cmd)
res = proc.wait()

if res != 0:
	exit(1)

os.rename(result_source, target_source_loc)
os.rename(result_header, target_header_loc)
