# this script was used to clean up a gram.y file and remove useless rules
import subprocess, re, os, sys

fpath = 'third_party/libpg_query/gram.y'
temp_path = 'third_party/libpg_query/gram.y.tmp'

def remove_rule(text, rulename):
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
	return text[:match.start()] + text[match.end() + match2.start():]

def remove_terminal(text, terminal):
	regex = '[ \t\n]' + terminal + '[ \t\n]'
	return re.sub(regex, ' ', text)

if len(sys.argv) == 2:
	stmt = sys.argv[1]

	with open(fpath, 'r') as f:
		text = f.read()

	# remove the statement
	text = text.replace("\n\t\t\t| " + stmt, "")
	text = remove_terminal(text, stmt)

	with open(temp_path, 'w+') as f:
		f.write(text)

	diffcmd = 'diff %s %s' % (fpath, temp_path)
	os.system(diffcmd)

	text = input("Accept Changes (Y/N)? ")  # Python 3

	if text.lower() != 'y':
		exit(1)

	os.system('cp %s %s' % (temp_path, fpath))

proc_cmd = ['bison', '-o', 'src/parser/grammar/grammar.c', '-d', fpath]
print(' '.join(proc_cmd))
proc = subprocess.Popen(proc_cmd, stderr=subprocess.PIPE)

error_text = proc.stderr.read().decode('utf8').strip()
if len(error_text) > 0:
	print(error_text)

	with open(fpath, 'r') as f:
		text = f.read()

	useless_terminals = set(re.findall('warning: useless nonterminal: ([a-zA-Z0-9_]+)', error_text))
	useless_rules = set(re.findall('warning: useless rule: ([^:]+):', error_text))

		# remove useless rules
	print(useless_rules)
	for rule in useless_rules:
		text = remove_rule(text, rule)

	# remove useless terminals
	print(useless_terminals)
	for terminal in useless_terminals:
		text = remove_terminal(text, terminal)

	# remove useless %type statements
	for i in range(10):
		text = re.sub('%type <[^>]+>[ \t\n]+%', '\n%', text)
		text = re.sub('%type <[^>]+>[ \t\n]+/', '\n/', text)

	with open(temp_path, 'w+') as f:
		f.write(text)

	diffcmd = 'diff %s %s' % (fpath, temp_path)
	os.system(diffcmd)

	text = input("Accept Changes (Y/N)? ")  # Python 3

	if text.lower() != 'y':
		exit(1)

os.system('cp %s %s' % (temp_path, fpath))