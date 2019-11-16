

import subprocess, re, os, sys

fpath = 'third_party/libpg_query/gram.y'
temp_path = 'third_party/libpg_query/gram.y.tmp'
statements_list = 'third_party/libpg_query/grammar/statements.list'
statements_dir = 'third_party/libpg_query/grammar/statements'
types_dir = 'third_party/libpg_query/grammar/types'

if len(sys.argv) < 3:
	print("Usage: python3 extract.py [stmt] [target.y] [-f]")
	exit(1)

stmt = sys.argv[1]
target_file = sys.argv[2]
force = False
for args in sys.argv:
	if args == '-f':
		force = True

if not target_file.endswith(".y"):
	print("Usage: python3 extract.py [stmt] [target.y]")
	print("Error: target file needs to end with .y!")
	exit(1)


types_file = os.path.join(types_dir, target_file.replace(".y", ".yh"))
target_file = os.path.join(statements_dir, target_file)

if not force:
	if os.path.isfile(target_file):
		print("Target file %s already exists! Use -f to overwrite" % (target_file,))
		exit(1)

	if os.path.isfile(types_file):
		print("Type file %s already exists! Use -f to overwrite" % (types_file,))
		exit(1)

rules = []
types = []

def extract_rule(text, rulename):
	global rules
	# find the rule
	regex = '^' + rulename + ":"
	print(regex)
	match = re.search(regex, text, re.MULTILINE)
	if match == None:
		print("Could not find rule to remove ", rulename)
		exit(1)
	# now find the next rule or the next comment block
	next_text = text[match.end():]
	match2 = re.search('^([/][*]|[a-zA-Z0-9_]+:)', next_text, re.MULTILINE)
	if match2 == None:
		print("Could not find next match for rulename ", rulename)
		exit(1)
	rule = text[match.start():match.end() + match2.start()]
	rules.append(rule)
	return text[:match.start()] + text[match.end() + match2.start():]

def remove_terminal(text, terminal):
	regex = '[ \t\n]' + terminal + '[ \t\n]'
	return re.sub(regex, ' ', text)

def extract_type(text, terminal):
	global types
	print("^%type <([^>]+)>[a-zA-Z0-9_\n\t ]+" + terminal)
	match = re.search("%type <([^>]+)>[a-zA-Z0-9_\n\t ]+" + terminal, text)
	if match != None:
		types.append("%%type <%s> %s" % (match.groups()[0], terminal))
	return remove_terminal(text, terminal)


with open(fpath, 'r') as f:
	text = f.read()

# extract the comment block above the main statement, if any
match = re.search("(/[*][^/]*[*]/)[\t\n ]*" + stmt + ":", text, re.DOTALL)
if match != None:
	rules.append(match.groups()[0])
# extract the main statement rule
text = extract_rule(text, stmt)
text = text.replace("\n\t\t\t| " + stmt, "")
text = remove_terminal(text, stmt)


with open(temp_path, 'w+') as f:
	f.write(text)

proc_cmd = ['bison', '-o', 'src/parser/grammar/grammar.c', '-d', temp_path]
print(' '.join(proc_cmd))
proc = subprocess.Popen(proc_cmd, stderr=subprocess.PIPE)

error_text = proc.stderr.read().decode('utf8').strip()
if len(error_text) > 0:
	print(error_text)

	useless_terminals = set(re.findall('warning: useless nonterminal: ([a-zA-Z0-9_]+)', error_text))
	useless_rules = set(re.findall('warning: useless rule: ([^:]+):', error_text))

	if len(useless_rules) == 0 and len(useless_terminals) == 0:
		print("No rules or terminals to write?")
		exit(1)
	# extract rules
	print(useless_rules)
	for rule in useless_rules:
		text = extract_rule(text, rule)

	# extract types
	print(useless_terminals)
	for terminal in useless_terminals:
		text = extract_type(text, terminal)

	# remove useless %type statements
	for i in range(10):
		text = re.sub('%type <[^>]+>[ \t\n]+%', '\n%', text)

rule_text = '\n'.join(rules).strip() + '\n'
type_text = '\n'.join(types).strip() + '\n'
write_types = len(type_text.strip()) > 0
if len(rule_text.strip()) == 0:
	print("No rules to write?")
	exit(1)

print("---------------------------------------------------------")
print("---- The following changes will be added to the file ----")
print("%s" % (target_file))
print("---------------------------------------------------------")

print(rule_text)

if write_types:
	print("---------------------------------------------------------")
	print("---- The following changes will be added to the file ----")
	print("%s" % (types_file))
	print("---------------------------------------------------------")
	print(type_text)

# verify that the original will compile after the changes
with open(temp_path, 'w+') as f:
	f.write(text)

proc_cmd = ['bison', '-o', 'src/parser/grammar/grammar.c', '-d', temp_path]
proc = subprocess.Popen(proc_cmd, stderr=subprocess.PIPE)
error_text = proc.stderr.read().decode('utf8').strip()
if len(error_text) > 0:
	print("!!Original will not compile after these changes!!")
	print(error_text)
else:
	print("Original will compile after these changes.")

input_text = input("Accept Changes (Y/N)? ")  # Python 3

if input_text.lower() != 'y':
	exit(1)

# create the target_file
with open(target_file, 'w+') as f:
	f.write(rule_text)

if write_types:
	with open(types_file, 'w+') as f:
		f.write(type_text)

# place the statement in the statements list
statements = []
with open(statements_list, 'r') as f:
	statement_text = f.read().strip()
	if len(statement_text) > 0:
		statements = statement_text.split('\n')
statements = list(set(statements + [stmt]))
statements.sort()
with open(statements_list, 'w') as f:
	f.write('\n'.join(statements))

# finally overwrite the gram.y with the new file
os.system('cp %s %s' % (temp_path, fpath))