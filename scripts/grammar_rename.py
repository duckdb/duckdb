pg_dir = 'third_party/libpg_query'
parser_dir = 'src/parser/transform'
import os, re

ignored_files = ['src_backend_parser_gram.cpp', 'gram.hpp']
file_contents = {}

def load_file(fpath):
	global file_contents
	with open(fpath, 'r') as f:
		file_contents[fpath] = f.read()

def load_contents(directory):
	global file_contents
	for fname in os.listdir(directory):
		if fname in ignored_files:
			continue
		fpath = os.path.join(directory, fname)
		if os.path.isdir(fpath):
			load_contents(fpath)
		elif fpath.endswith(".c") or fpath.endswith(".h") or fpath.endswith(".cpp") or fpath.endswith(".hpp") or fpath.endswith(".y"):
			load_file(fpath)


# cleanup_nodes: remove unused nodes from the parsenodes.h file
def cleanup_nodes():
	load_contents(pg_dir)
	with open(os.path.join(pg_dir, 'include/nodes/parsenodes.h'), 'r') as f:
		text = f.read()

	delete_nodes = []
	current_index = 0
	while True:
		match = re.search('typedef struct ([a-zA-Z0-9_]+)[ \t\n]*{[^}]+} ([a-zA-Z0-9_]+);', text[current_index:])
		if match == None:
			break
		current_index += match.end()

		node_name = match.groups()[0]
		# check all the files except parsenodes.h if class is ever used file is ever used
		found_match = False
		node_regex = '[, \t\n(]' + node_name + '[), \t\n;*]'
		for fname in file_contents.keys():
			if fname.endswith('parsenodes.h'):
				continue
			fcontent = file_contents[fname]
			match = re.search(node_regex, fcontent)
			if match != None:
				found_match = True
				break
		if not found_match:
			delete_nodes.append(node_name)

	if len(delete_nodes) == 0:
		return

	print(delete_nodes)
	for delete_node in delete_nodes:
		text = re.sub('typedef struct %s[ \t\n]*{[^}]+} %s;' % (delete_node, delete_node), '', text)
	with open(os.path.join(pg_dir, 'include/nodes/parsenodes.h'), 'w+') as f:
		f.write(text)

def add_rename(name_map, name):
	name = name.strip()
	if 'yy' in name.lower():
		return
	if name.lower().startswith("pg_"):
		return

	if name.startswith("T_"):
		name_map[name] = "T_PG" + name[2:]
	elif name.lower() == name.upper():
		# everything lower case
		name_map[name] = "pg_" + name
	elif '_' in name:
		name_map[name] = "PG_" + name
	else:
		name_map[name] = "PG" + name

def regex_replace(fcontent, rkey, rename):
	rkey_regex = '([, \t\n(:])' + rkey + '([:), \t\n;*])'
	return re.sub(rkey_regex, '\g<1>' + rename + '\g<2>', fcontent)

def rename_structs():
	global file_contents
	load_contents(pg_dir)
	enum_rename_mapping = {}
	rename_mapping = {}
	# gather all struct definitions
	for fname in file_contents.keys():
		fcontent = file_contents[fname]
		# first remove all the comments from the file
		fcontent = re.sub("//[^\n]*", "", fcontent)
		fcontent = re.sub("/[*].*[*]/", "", fcontent, re.DOTALL)

		# structs
		structs = re.findall('typedef struct( [a-zA-Z0-9_]+)?[ \t\n]*[{][^}]+[}] ([a-zA-Z0-9_]+);', fcontent)
		for struct in structs:
			add_rename(rename_mapping, struct[1])

		 # types
		types = re.findall("[, \t\n](T_[a-zA-Z0-9_]+)[, \t\n]", fcontent)
		for tp in types:
			add_rename(enum_rename_mapping, tp)

		# enums
		matches = re.findall("typedef enum [a-zA-Z0-9_ \n\t]*[{]([^}]*)[}] ([a-zA-Z0-9_]*);", fcontent)
		for m in matches:
			enum_names = re.findall("\t([a-zA-Z0-9_]+)[ \t\n]*[,=}]", m[0])
			for enum_name in enum_names:
				if enum_name.startswith("T_"):
					continue
				add_rename(enum_rename_mapping, enum_name)
			condition_name = m[1]
			add_rename(rename_mapping, condition_name)
	rename_mapping['AttrNumber'] = 'PGAttrNumber'
	rename_mapping['parser_state_str'] = 'pg_parser_state_str'
	rename_mapping['varlena'] = 'pg_varlena'
	rename_mapping['ParseState'] = 'PGParseState'
	rename_mapping['ListCell'] = 'PGListCell'
	rename_mapping['MemoryContext'] = 'PGMemoryContext'
	rename_mapping['Datum'] = 'PGDatum'
	rename_mapping['Size'] = 'PGSize'
	rename_mapping['Oid'] = 'PGOid'
	rename_mapping['Index'] = 'PGIndex'
	rename_mapping['bits32'] = 'pg_bits32'
	rename_mapping['LOCKMODE'] = 'PGLOCKMODE'
	rename_mapping['uint32'] = 'uint32_t'
	rename_mapping['int64'] = 'int64_t'
	rename_mapping['uint32'] = 'uint32_t'
	rename_mapping['int32'] = 'int32_t'
	rename_mapping['int16'] = 'int16_t'
	rename_mapping['uint16'] = 'uint16_t'
	rename_mapping['uint8'] = 'uint8_t'
	rename_mapping['StringInfoData'] = 'PGStringInfoData'
	rename_mapping['FunctionCallInfo'] = 'PGFunctionCallInfo'
	rename_mapping['fmNodePtr'] = 'pg_fmNodePtr'
	rename_mapping['fmAggrefPtr'] = 'pg_fmAggrefPtr'
	rename_mapping['fmStringInfo'] = 'pg_fmStringInfo'
	rename_mapping['Value'] = 'PGValue'
	rename_mapping['tzEntry'] = 'pg_tzEntry'
	rename_mapping['fsec_t'] = 'pg_fsec_t'
	rename_mapping['TimestampTz'] = 'PGTimestampTz'
	rename_mapping['Timestamp'] = 'PGTimestamp'
	rename_mapping['TimeOffset'] = 'PGTimeOffset'
	# now use the rename map to rename files
	file_contents = {}
	load_contents(pg_dir)
	# for fname in file_contents.keys():
	# 	print(fname)
	# 	fcontent = file_contents[fname]
	# 	for rkey in rename_mapping.keys():
	# 		fcontent = regex_replace(fcontent, rkey, rename_mapping[rkey])
	# 	for rkey in enum_rename_mapping.keys():
	# 		fcontent = regex_replace(fcontent, rkey, enum_rename_mapping[rkey])
	# 	with open(fname, 'w+') as f:
	# 		f.write(fcontent)
	# perform the same remapping in the parser
	file_contents = {}
	load_contents(parser_dir)
	load_file('src/include/duckdb/parser/transformer.hpp')
	for fname in file_contents.keys():
		print(fname)
		fcontent = file_contents[fname]
		for rkey in rename_mapping.keys():
			fcontent = regex_replace(fcontent, 'postgres::' + rkey, 'postgres::' + rename_mapping[rkey])
		for rkey in enum_rename_mapping:
			fcontent = regex_replace(fcontent, rkey, enum_rename_mapping[rkey])
		with open(fname, 'w+') as f:
			f.write(fcontent)



rename_structs()
