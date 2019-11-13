# use bison to generate the parser files of postgres
# the following versions were used:
# bison (GNU Bison) 3.4.2
# flex 2.6.4
bison_location     = "/usr/local/opt/bison/bin/bison"
main_file_location = "third_party/libpg_query"
main_file          = "gram.y"
compile_location = "third_party/libpg_query/src_backend_parser_gram.cpp"
compile_location_hpp = "third_party/libpg_query/src_backend_parser_gram.hpp"
target_location_header = "third_party/libpg_query/include/parser/gram.hpp"
compile_location_location = "third_party/libpg_query/location.hh"
target_location_location = "third_party/libpg_query/include/location.hpp"

import os, subprocess, re

yacc_file = os.path.join(main_file_location, main_file)
yacc_temp_file = yacc_file + ".tmp"
# perform preprocessing of the file
with open(yacc_file, 'r') as f:
	text = f.read()

def extract_regex(regex, text, result_list):
	while True:
		match = re.search(regex, text, re.MULTILINE)
		if match == None:
			return text
		result_list.append(match.groups()[0])
		text = text[:match.start()] + text[match.end():]

unreserved_keywords = []
statements = []
def preprocess_include(include_text):
	# parse statements, keywords and types from the include file
	include_text = extract_regex("^stmt: ([^\n]+)", include_text, statements)
	include_text = extract_regex("^unreserved_keyword: ([^\n]+)", include_text, unreserved_keywords)
	return include_text

# first perform preprocessing of all the includes
while True:
	match = re.search('[%]include ([^\n]+)', text)
	if match == None:
		break
	include_file = match.groups()[0]
	start = match.start()
	end = match.end()
	# read the include file
	with open(os.path.join(main_file_location, include_file), 'r') as f:
		include_text = f.read()
	# preprocess the include file
	include_text = preprocess_include(include_text)

	# now concat to the text
	text = text[:start] + include_text + text[end:]

# now add statements, keywords, tokens and types parsed from the include files into the main file
# text = re.sub("^stmt:", "stmt: " + ' '.join(statements) + ' ', text, 1, re.MULTILINE)
# text = re.sub("^[%]type <node>", "%type <node> " + ' '.join(statements) + ' ', text, 1, re.MULTILINE)
# text = re.sub("^unreserved_keyword:", "unreserved_keyword: " + ' '.join(unreserved_keywords) + ' ', text, 1, re.MULTILINE)
# text = re.sub("^[%]token <keyword>", "%token <keyword> " + ' '.join(unreserved_keywords) + ' ', text, 1, re.MULTILINE)

def add_to_pipe_list(text, terms, list_name, concat=""):
	regex = "^" + list_name + ":([^;]+);"
	match = re.search(regex, text, re.MULTILINE)
	current_list = match.groups()[0]
	current_list = re.sub("[ \t\n]", "", current_list)
	splits = current_list.split("|")
	all_terms = terms + splits
	all_terms.sort()
	new_term_list = list_name + ': '  + '\n\t| '.join(all_terms) + concat + ';\n'
	return text[:match.start()] + new_term_list + text[match.end():]

def replace_function(match):
	return "[" + match.groups()[0] + "]"

def add_to_space_list(text, terms, list_name, concat=""):
	list_regex = re.sub("([^a-zA-Z])", replace_function, list_name)
	regex = "^" + list_regex + "([^;/%]+)"
	match = re.search(regex, text, re.MULTILINE)
	current_list = match.groups()[0]
	current_list = re.sub("[ \t\n]+", " ", current_list).strip()
	splits = current_list.split(" ")
	all_terms = terms + splits
	all_terms.sort()
	new_term_list = list_name + ' '  + ' '.join(all_terms) + concat + '\n'
	return text[:match.start()] + new_term_list + text[match.end():]

text = add_to_pipe_list(text, statements, "stmt", '\n\t|/* EMPTY */\n\t{ $$ = NULL; }')
text = add_to_pipe_list(text, unreserved_keywords, "unreserved_keyword")
text = add_to_space_list(text, statements, "%type <node>")
text = add_to_space_list(text, unreserved_keywords, "%token <keyword>")

# now write the concatenated file
with open(yacc_temp_file, 'w+') as f:
	f.write(text)

# generate the bison
cmd = [bison_location, "-o", compile_location, "-d", yacc_temp_file]
print(' '.join(cmd))
proc = subprocess.Popen(cmd)
res = proc.wait()

if res != 0:
	exit(1)

# replace namespace with base_yy to postgres
def replace_namespace(file_location):
	with open(file_location, 'r') as f:
		text = f.read()
	text = text.replace("namespace base_yy", "namespace postgres")
	text = text.replace("location.hh", "location.hpp")
	with open(file_location, 'w+') as f:
		f.write(text)

replace_namespace(compile_location_location)
replace_namespace(compile_location_hpp)
replace_namespace(compile_location)

# move headers to correct location
os.rename(compile_location_hpp, target_location_header)
os.rename(compile_location_location, target_location_location)
