
#include "sqllogic_test_runner.hpp"

#include "catch.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/extension/generated_extension_loader.hpp"
#include "duckdb/main/extension_entries.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "sqllogic_parser.hpp"
#include "test_helpers.hpp"
#include "sqllogic_test_logger.hpp"

#ifdef DUCKDB_OUT_OF_TREE
#include DUCKDB_EXTENSION_HEADER
#endif

namespace duckdb {

SQLLogicTestRunner::SQLLogicTestRunner(string dbpath) : dbpath(std::move(dbpath)), finished_processing_file(false) {
	config = GetTestConfig();
	config->options.allow_unredacted_secrets = true;
	config->options.load_extensions = false;

	auto env_var = std::getenv("LOCAL_EXTENSION_REPO");
	if (!env_var) {
		config->options.autoload_known_extensions = false;
	} else {
		local_extension_repo = env_var;
		config->options.autoload_known_extensions = true;
	}
}

SQLLogicTestRunner::~SQLLogicTestRunner() {
	config.reset();
	con.reset();
	db.reset();
	for (auto &loaded_path : loaded_databases) {
		if (loaded_path.empty()) {
			continue;
		}
		// only delete database files that were created during the tests
		if (!StringUtil::StartsWith(loaded_path, TestDirectoryPath())) {
			continue;
		}
		DeleteDatabase(loaded_path);
	}
}

void SQLLogicTestRunner::ExecuteCommand(duckdb::unique_ptr<Command> command) {
	if (InLoop()) {
		auto &current_loop = *active_loops.back();
		current_loop.loop_commands.push_back(std::move(command));
	} else {
		ExecuteContext context;
		command->Execute(context);
	}
}

void SQLLogicTestRunner::StartLoop(LoopDefinition definition) {
	auto loop = make_uniq<LoopCommand>(*this, std::move(definition));
	auto loop_ptr = loop.get();
	if (InLoop()) {
		auto &current_loop = *active_loops.back();
		current_loop.loop_commands.push_back(std::move(loop));
	} else {
		// not in a loop yet: new top-level loop
		top_level_loop = std::move(loop);
	}
	active_loops.push_back(loop_ptr);
}

void SQLLogicTestRunner::EndLoop() {
	// finish a loop: pop it from the active_loop queue
	if (active_loops.empty()) {
		throw std::runtime_error("endloop without active loop!");
	}
	active_loops.pop_back();
	if (active_loops.empty()) {
		// not in a loop
		ExecuteContext context;
		top_level_loop->Execute(context);
		top_level_loop.reset();
	}
}

void SQLLogicTestRunner::LoadDatabase(string dbpath, bool load_extensions) {
	loaded_databases.push_back(dbpath);

	// restart the database with the specified db path
	db.reset();
	con.reset();
	named_connection_map.clear();
	// now re-open the current database

	try {
		db = make_uniq<DuckDB>(dbpath, config.get());
		// always load core functions
		ExtensionHelper::LoadExtension(*db, "core_functions");
	} catch (std::exception &ex) {
		ErrorData err(ex);
		SQLLogicTestLogger::LoadDatabaseFail(dbpath, err.Message());
		FAIL();
	}
	Reconnect();

	// load any previously loaded extensions again
	if (load_extensions) {
		for (auto &extension : extensions) {
			ExtensionHelper::LoadExtension(*db, extension);
		}
	}
}

void SQLLogicTestRunner::Reconnect() {
	con = make_uniq<Connection>(*db);
	if (original_sqlite_test) {
		con->Query("SET integer_division=true");
	}
	con->Query("SET secret_directory='" + TestCreatePath("test_secret_dir") + "'");
#ifdef DUCKDB_ALTERNATIVE_VERIFY
	con->Query("SET pivot_filter_threshold=0");
#endif
	auto &client_config = ClientConfig::GetConfig(*con->context);
	client_config.enable_progress_bar = true;
	client_config.print_progress_bar = false;
	if (enable_verification) {
		con->EnableQueryVerification();
	}
	// Set the local extension repo for autoinstalling extensions
	if (!local_extension_repo.empty()) {
		auto res1 = con->Query("SET autoinstall_extension_repository='" + local_extension_repo + "'");
	}
}

string SQLLogicTestRunner::ReplaceLoopIterator(string text, string loop_iterator_name, string replacement) {
	replacement = ReplaceKeywords(replacement);
	if (StringUtil::Contains(loop_iterator_name, ",")) {
		auto name_splits = StringUtil::Split(loop_iterator_name, ",");
		auto replacement_splits = StringUtil::Split(replacement, ",");
		if (name_splits.size() != replacement_splits.size()) {
			FAIL("foreach loop: number of commas in loop iterator (" + loop_iterator_name +
			     ") does not match number of commas in replacement (" + replacement + ")");
		}
		for (idx_t i = 0; i < name_splits.size(); i++) {
			text = StringUtil::Replace(text, "${" + name_splits[i] + "}", replacement_splits[i]);
		}
		return text;
	} else {
		return StringUtil::Replace(text, "${" + loop_iterator_name + "}", replacement);
	}
}

string SQLLogicTestRunner::LoopReplacement(string text, const vector<LoopDefinition> &loops) {
	for (auto &active_loop : loops) {
		if (active_loop.tokens.empty()) {
			// regular loop
			text = ReplaceLoopIterator(text, active_loop.loop_iterator_name, to_string(active_loop.loop_idx));
		} else {
			// foreach loop
			text = ReplaceLoopIterator(text, active_loop.loop_iterator_name, active_loop.tokens[active_loop.loop_idx]);
		}
	}
	return text;
}

string SQLLogicTestRunner::ReplaceKeywords(string input) {
	// Replace environment variables in the SQL
	for (auto &it : environment_variables) {
		auto &name = it.first;
		auto &value = it.second;
		input = StringUtil::Replace(input, StringUtil::Format("${%s}", name), value);
	}
	input = StringUtil::Replace(input, "__TEST_DIR__", TestDirectoryPath());
	input = StringUtil::Replace(input, "__WORKING_DIRECTORY__", FileSystem::GetWorkingDirectory());
	input = StringUtil::Replace(input, "__BUILD_DIRECTORY__", DUCKDB_BUILD_DIRECTORY);
	return input;
}

bool SQLLogicTestRunner::ForEachTokenReplace(const string &parameter, vector<string> &result) {
	if (parameter.empty()) {
		return true;
	}
	auto token_name = StringUtil::Lower(parameter);
	StringUtil::Trim(token_name);
	bool collection = false;
	bool is_compression = token_name == "<compression>";
	bool is_all = token_name == "<alltypes>";
	bool is_numeric = is_all || token_name == "<numeric>";
	bool is_integral = is_numeric || token_name == "<integral>";
	bool is_signed = is_integral || token_name == "<signed>";
	bool is_unsigned = is_integral || token_name == "<unsigned>";
	bool is_all_types_column = token_name == "<all_types_columns>";
	if (token_name[0] == '!') {
		// !token tries to remove the token from the list of tokens
		auto entry = std::find(result.begin(), result.end(), parameter.substr(1));
		if (entry == result.end()) {
			// not found - insert as-is
			return false;
		}
		// found - erase the entry
		result.erase(entry);
		collection = true;
	}
	if (is_signed) {
		result.push_back("tinyint");
		result.push_back("smallint");
		result.push_back("integer");
		result.push_back("bigint");
		result.push_back("hugeint");
		collection = true;
	}
	if (is_unsigned) {
		result.push_back("utinyint");
		result.push_back("usmallint");
		result.push_back("uinteger");
		result.push_back("ubigint");
		result.push_back("uhugeint");
		collection = true;
	}
	if (is_numeric) {
		result.push_back("float");
		result.push_back("double");
		collection = true;
	}
	if (is_all) {
		result.push_back("bool");
		result.push_back("interval");
		result.push_back("varchar");
		collection = true;
	}
	if (is_compression) {
		result.push_back("none");
		result.push_back("uncompressed");
		result.push_back("rle");
		result.push_back("bitpacking");
		result.push_back("dictionary");
		result.push_back("fsst");
		result.push_back("alp");
		result.push_back("alprd");
		collection = true;
	}
	if (is_all_types_column) {
		result.push_back("bool");
		result.push_back("tinyint");
		result.push_back("smallint");
		result.push_back("int");
		result.push_back("bigint");
		result.push_back("hugeint");
		result.push_back("uhugeint");
		result.push_back("utinyint");
		result.push_back("usmallint");
		result.push_back("uint");
		result.push_back("ubigint");
		result.push_back("date");
		result.push_back("time");
		result.push_back("timestamp");
		result.push_back("timestamp_s");
		result.push_back("timestamp_ms");
		result.push_back("timestamp_ns");
		result.push_back("time_tz");
		result.push_back("timestamp_tz");
		result.push_back("float");
		result.push_back("double");
		result.push_back("dec_4_1");
		result.push_back("dec_9_4");
		result.push_back("dec_18_6");
		result.push_back("dec38_10");
		result.push_back("uuid");
		result.push_back("interval");
		result.push_back("varchar");
		result.push_back("blob");
		result.push_back("bit");
		result.push_back("small_enum");
		result.push_back("medium_enum");
		result.push_back("large_enum");
		result.push_back("int_array");
		result.push_back("double_array");
		result.push_back("date_array");
		result.push_back("timestamp_array");
		result.push_back("timestamptz_array");
		result.push_back("varchar_array");
		result.push_back("nested_int_array");
		result.push_back("struct");
		result.push_back("struct_of_arrays");
		result.push_back("array_of_structs");
		result.push_back("map");
		result.push_back("union");
		result.push_back("fixed_int_array");
		result.push_back("fixed_varchar_array");
		result.push_back("fixed_nested_int_array");
		result.push_back("fixed_nested_varchar_array");
		result.push_back("fixed_struct_array");
		result.push_back("struct_of_fixed_array");
		result.push_back("fixed_array_of_int_list");
		result.push_back("list_of_fixed_int_array");
		collection = true;
	}
	return collection;
}

static string ParseExplanation(SQLLogicParser &parser, const vector<string> &params, size_t &index) {
	string res;
	if (params[index].empty() || params[index][0] != '"') {
		parser.Fail("Quoted parameter should start with double quotes");
	}

	res += params[index].substr(1);
	index++;

	while (index < params.size()) {
		res += " " + params[index];
		index++;

		if (res.back() == '"') {
			res.pop_back();
			break;
		}
	}

	return res;
}

RequireResult SQLLogicTestRunner::CheckRequire(SQLLogicParser &parser, const vector<string> &params) {
	if (params.size() < 1) {
		parser.Fail("require requires a single parameter");
	}
	// require command
	string param = StringUtil::Lower(params[0]);
	// os specific stuff

	if (param == "notmusl") {
#ifdef __MUSL_ENABLED__
		return RequireResult::MISSING;
#else
		return RequireResult::PRESENT;
#endif
	}
	if (param == "notmingw") {
#ifdef __MINGW32__
		return RequireResult::MISSING;
#else
		return RequireResult::PRESENT;
#endif
	}

	if (param == "mingw") {
#ifndef __MINGW32__
		return RequireResult::MISSING;
#else
		return RequireResult::PRESENT;
#endif
	}

	if (param == "notwindows") {
#ifdef _WIN32
		return RequireResult::MISSING;
#else
		return RequireResult::PRESENT;
#endif
	}

	if (param == "windows") {
#ifndef _WIN32
		return RequireResult::MISSING;
#else
		return RequireResult::PRESENT;
#endif
	}

	if (param == "longdouble") {
#if LDBL_MANT_DIG < 54
		return RequireResult::MISSING;
#else
		return RequireResult::PRESENT;
#endif
	}

	if (param == "64bit") {
		if (sizeof(void *) != 8) {
			return RequireResult::MISSING;
		}
		return RequireResult::PRESENT;
	}

	if (param == "noforcestorage") {
		if (TestForceStorage()) {
			return RequireResult::MISSING;
		}
		return RequireResult::PRESENT;
	}

	if (param == "nothreadsan") {
#ifdef DUCKDB_THREAD_SANITIZER
		return RequireResult::MISSING;
#else
		return RequireResult::PRESENT;
#endif
	}

	if (param == "strinline") {
#ifdef DUCKDB_DEBUG_NO_INLINE
		return RequireResult::MISSING;
#else
		return RequireResult::PRESENT;
#endif
	}

	if (param == "ram") {
		if (params.size() != 2) {
			parser.Fail("require ram requires a parameter");
		}
		// require a minimum amount of ram
		auto required_limit = DBConfig::ParseMemoryLimit(params[1]);
		auto limit = FileSystem::GetAvailableMemory();
		if (!limit.IsValid()) {
			return RequireResult::MISSING;
		}
		if (limit.GetIndex() < required_limit) {
			return RequireResult::MISSING;
		}
		return RequireResult::PRESENT;
	}

	if (param == "disk_space") {
		if (params.size() != 2) {
			parser.Fail("require disk_space requires a parameter");
		}
		// require a minimum amount of disk space
		auto required_limit = DBConfig::ParseMemoryLimit(params[1]);
		auto available_space = FileSystem::GetAvailableDiskSpace(".");
		if (!available_space.IsValid()) {
			return RequireResult::MISSING;
		}
		if (available_space.GetIndex() < required_limit) {
			return RequireResult::MISSING;
		}
		return RequireResult::PRESENT;
	}

	if (param == "vector_size") {
		if (params.size() != 2) {
			parser.Fail("require vector_size requires a parameter");
		}
		// require a specific vector size
		auto required_vector_size = NumericCast<idx_t>(std::stoi(params[1]));
		if (STANDARD_VECTOR_SIZE < required_vector_size) {
			// vector size is too low for this test: skip it
			return RequireResult::MISSING;
		}
		return RequireResult::PRESENT;
	}

	if (param == "exact_vector_size") {
		if (params.size() != 2) {
			parser.Fail("require exact_vector_size requires a parameter");
		}
		// require an exact vector size
		auto required_vector_size = NumericCast<idx_t>(std::stoi(params[1]));
		if (STANDARD_VECTOR_SIZE != required_vector_size) {
			// vector size does not match the required vector size: skip it
			return RequireResult::MISSING;
		}
		return RequireResult::PRESENT;
	}

	if (param == "block_size") {
		if (params.size() != 2) {
			parser.Fail("require block_size requires a parameter");
		}
		// require a specific block size
		auto required_block_size = NumericCast<idx_t>(std::stoi(params[1]));
		if (config->options.default_block_alloc_size != required_block_size) {
			// block size does not match the required block size: skip it
			return RequireResult::MISSING;
		}
		return RequireResult::PRESENT;
	}

	if (param == "skip_reload") {
		skip_reload = true;
		return RequireResult::PRESENT;
	}

	if (param == "no_alternative_verify") {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
		return RequireResult::MISSING;
#else
		return RequireResult::PRESENT;
#endif
	}

	if (param == "no_block_verification") {
#ifdef DUCKDB_BLOCK_VERIFICATION
		return RequireResult::MISSING;
#else
		return RequireResult::PRESENT;
#endif
	}

	if (param == "no_vector_verification") {
#ifdef DUCKDB_VERIFY_VECTOR
		return RequireResult::MISSING;
#else
		return RequireResult::PRESENT;
#endif
	}

	if (param == "no_extension_autoloading") {
		if (params.size() < 2) {
			parser.Fail("require no_extension_autoloading needs an explanation string");
		}
		size_t index = 1;
		string explanation = ParseExplanation(parser, params, index);
		if (explanation.rfind("EXPECTED", 0) == 0 || explanation.rfind("FIXME", 0) == 0) {
			// good, explanation is properly formatted
		} else {
			parser.Fail(
			    "require no_extension_autoloading explanation string should begin with either 'EXPECTED' or FIXME'");
		}
		if (config->options.autoload_known_extensions) {
			// If autoloading is on, we skip this test
			return RequireResult::MISSING;
		}
		return RequireResult::PRESENT;
	}

	bool excluded_from_autoloading = true;
	for (const auto &ext : AUTOLOADABLE_EXTENSIONS) {
		if (ext == param) {
			excluded_from_autoloading = false;
			break;
		}
	}

	if (!config->options.autoload_known_extensions) {
		auto result = ExtensionHelper::LoadExtension(*db, param);
		if (result == ExtensionLoadResult::LOADED_EXTENSION) {
			// add the extension to the list of loaded extensions
			extensions.insert(param);
		} else if (result == ExtensionLoadResult::EXTENSION_UNKNOWN) {
			parser.Fail("unknown extension type: %s", params[0]);
		} else if (result == ExtensionLoadResult::NOT_LOADED) {
			// extension known but not build: skip this test
			return RequireResult::MISSING;
		}
	} else if (excluded_from_autoloading) {
		return RequireResult::MISSING;
	}
	return RequireResult::PRESENT;
}

bool TryParseConditions(SQLLogicParser &parser, const string &condition_text, vector<Condition> &conditions,
                        bool skip_if) {
	bool is_condition = false;
	for (auto &c : condition_text) {
		switch (c) {
		case '=':
		case '>':
		case '<':
			is_condition = true;
			break;
		default:
			break;
		}
	}
	if (!is_condition) {
		// not a condition
		return false;
	}
	// split based on &&
	auto condition_strings = StringUtil::Split(condition_text, "&&");
	for (auto &condition_str : condition_strings) {
		vector<pair<string, ExpressionType>> comparators {
		    {"<>", ExpressionType::COMPARE_NOTEQUAL},   {">=", ExpressionType::COMPARE_GREATERTHANOREQUALTO},
		    {">", ExpressionType::COMPARE_GREATERTHAN}, {"<=", ExpressionType::COMPARE_LESSTHANOREQUALTO},
		    {"<", ExpressionType::COMPARE_LESSTHAN},    {"=", ExpressionType::COMPARE_EQUAL}};
		ExpressionType comparison_type = ExpressionType::INVALID;
		vector<string> splits;
		for (auto &comparator : comparators) {
			if (!StringUtil::Contains(condition_str, comparator.first)) {
				continue;
			}
			splits = StringUtil::Split(condition_str, comparator.first);
			comparison_type = comparator.second;
			break;
		}
		// loop condition, e.g. skipif threadid=0
		if (splits.size() != 2) {
			parser.Fail("skipif/onlyif must be in the form of x=y or x>y, potentially separated by &&");
		}
		// strip white space
		for (auto &split : splits) {
			StringUtil::Trim(split);
		}

		// now create the condition
		Condition condition;
		condition.keyword = splits[0];
		condition.value = splits[1];
		condition.comparison = comparison_type;
		condition.skip_if = skip_if;
		conditions.push_back(condition);
	}
	return true;
}

void SQLLogicTestRunner::ExecuteFile(string script) {
	SQLLogicParser parser;
	idx_t skip_level = 0;

	// for the original SQLite tests we convert floating point numbers to integers
	// for our own tests this is undesirable since it hides certain errors
	if (script.find("test/sqlite/select") != string::npos) {
		original_sqlite_test = true;
	}
	if (script.find("third_party/sqllogictest") != string::npos) {
		original_sqlite_test = true;
	}

	if (!dbpath.empty()) {
		// delete the target database file, if it exists
		DeleteDatabase(dbpath);
	}

	// initialize the database with the default dbpath
	LoadDatabase(dbpath, true);

	// open the file and parse it
	bool success = parser.OpenFile(script);
	if (!success) {
		FAIL("Could not find test script '" + script + "'. Perhaps run `make sqlite`. ");
	}

	/* Loop over all records in the file */
	while (parser.NextStatement()) {
		// tokenize the current line
		auto token = parser.Tokenize();

		// throw explicit error on single line statements that are not separated by a comment or newline
		if (parser.IsSingleLineStatement(token) && !parser.NextLineEmptyOrComment()) {
			parser.Fail("all test statements need to be separated by an empty line");
		}

		vector<Condition> conditions;
		bool skip_statement = false;
		while (token.type == SQLLogicTokenType::SQLLOGIC_SKIP_IF || token.type == SQLLogicTokenType::SQLLOGIC_ONLY_IF) {
			// skipif/onlyif
			bool skip_if = token.type == SQLLogicTokenType::SQLLOGIC_SKIP_IF;
			if (token.parameters.size() < 1) {
				parser.Fail("skipif/onlyif requires a single parameter (e.g. skipif duckdb)");
			}
			auto system_name = StringUtil::Lower(token.parameters[0]);
			// we support two kinds of conditions here
			// (for original sqllogictests) system comparisons, e.g.:
			// (1) skipif duckdb
			// (2) onlyif <other_system>
			// conditions on loop variables, e.g.:
			// (1) skipif i=2
			// (2) onlyif threadid=0
			// the latter is only supported in our own tests (not in original sqllogic tests)
			bool is_system_comparison;
			if (original_sqlite_test) {
				is_system_comparison = true;
			} else {
				is_system_comparison = !TryParseConditions(parser, system_name, conditions, skip_if);
			}
			if (is_system_comparison) {
				bool our_system = system_name == "duckdb";
				if (original_sqlite_test) {
					our_system = our_system || system_name == "postgresql";
				}
				if (our_system == skip_if) {
					// we skip this command in two situations
					// (1) skipif duckdb
					// (2) onlyif <other_system>
					skip_statement = true;
					break;
				}
			}
			parser.NextLine();
			token = parser.Tokenize();
		}
		if (skip_statement) {
			continue;
		}
		if (skip_level > 0 && token.type != SQLLogicTokenType::SQLLOGIC_MODE) {
			continue;
		}
		if (token.type == SQLLogicTokenType::SQLLOGIC_STATEMENT) {
			// statement
			if (token.parameters.size() < 1) {
				parser.Fail("statement requires at least one parameter (statement ok/error)");
			}
			auto command = make_uniq<Statement>(*this);

			// parse the first parameter
			if (token.parameters[0] == "ok") {
				command->expected_result = ExpectedResult::RESULT_SUCCESS;
			} else if (token.parameters[0] == "error") {
				command->expected_result = ExpectedResult::RESULT_ERROR;
			} else if (token.parameters[0] == "maybe") {
				command->expected_result = ExpectedResult::RESULT_UNKNOWN;
			} else {
				parser.Fail("statement argument should be 'ok' or 'error");
			}

			command->file_name = script;
			command->query_line = parser.current_line + 1;

			// extract the SQL statement
			parser.NextLine();
			auto statement_text = parser.ExtractStatement();
			if (statement_text.empty()) {
				parser.Fail("Unexpected empty statement text");
			}
			command->expected_error = parser.ExtractExpectedError(
			    command->expected_result == ExpectedResult::RESULT_SUCCESS, original_sqlite_test);

			// perform any renames in the text
			command->base_sql_query = ReplaceKeywords(std::move(statement_text));

			if (token.parameters.size() >= 2) {
				command->connection_name = token.parameters[1];
			}
			command->conditions = std::move(conditions);
			ExecuteCommand(std::move(command));
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_QUERY) {
			if (token.parameters.size() < 1) {
				parser.Fail("query requires at least one parameter (query III)");
			}
			auto command = make_uniq<Query>(*this);

			// parse the expected column count
			command->expected_column_count = 0;
			auto &column_text = token.parameters[0];
			for (idx_t i = 0; i < column_text.size(); i++) {
				command->expected_column_count++;
				if (column_text[i] != 'T' && column_text[i] != 'I' && column_text[i] != 'R') {
					parser.Fail("unknown type character '%s' in string, expected T, I or R only",
					            string(1, column_text[i]));
				}
			}
			if (command->expected_column_count == 0) {
				parser.Fail("query requires at least a single column in the result");
			}

			command->file_name = script;
			command->query_line = parser.current_line + 1;

			// extract the SQL statement
			parser.NextLine();
			auto statement_text = parser.ExtractStatement();

			// perform any renames in the text
			command->base_sql_query = ReplaceKeywords(std::move(statement_text));

			// extract the expected result
			command->values = parser.ExtractExpectedResult();

			// figure out the sort style/connection style
			command->sort_style = SortStyle::NO_SORT;
			if (token.parameters.size() > 1) {
				auto &sort_style = token.parameters[1];
				if (sort_style == "nosort") {
					/* Do no sorting */
					command->sort_style = SortStyle::NO_SORT;
				} else if (sort_style == "rowsort" || sort_style == "sort") {
					/* Row-oriented sorting */
					command->sort_style = SortStyle::ROW_SORT;
				} else if (sort_style == "valuesort") {
					/* Sort all values independently */
					command->sort_style = SortStyle::VALUE_SORT;
				} else {
					// if this is not a known sort style, we use this as the connection name
					// this is a bit dirty, but well
					command->connection_name = sort_style;
				}
			}

			// check the label of the query
			if (token.parameters.size() > 2) {
				command->query_has_label = true;
				command->query_label = token.parameters[2];
			} else {
				command->query_has_label = false;
			}
			command->conditions = std::move(conditions);
			ExecuteCommand(std::move(command));
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_HASH_THRESHOLD) {
			if (token.parameters.size() != 1) {
				parser.Fail("hash-threshold requires a parameter");
			}
			try {
				hash_threshold = std::stoi(token.parameters[0]);
			} catch (...) {
				parser.Fail("hash-threshold must be a number");
			}
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_HALT) {
			break;
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_MODE) {
			if (token.parameters.size() != 1) {
				parser.Fail("mode requires one parameter");
			}
			string parameter = token.parameters[0];
			if (parameter == "skip") {
				skip_level++;
			} else if (parameter == "unskip") {
				skip_level--;
			} else {
				auto command = make_uniq<ModeCommand>(*this, std::move(parameter));
				ExecuteCommand(std::move(command));
			}
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_SET) {
			if (token.parameters.size() < 1) {
				parser.Fail("set requires at least 1 parameter (e.g. set ignore_error_messages HTTP Error)");
			}
			if (token.parameters[0] == "ignore_error_messages" || token.parameters[0] == "always_fail_error_messages") {
				unordered_set<string> *string_set;
				if (token.parameters[0] == "ignore_error_messages") {
					string_set = &ignore_error_messages;
				} else {
					string_set = &always_fail_error_messages;
				}

				// the set command overrides the default values
				string_set->clear();

				// Parse the parameter list as a comma separated list of strings that can contain spaces
				// e.g. `set ignore_error_messages This is an error message, This_is_another, and   another`
				if (token.parameters.size() > 1) {
					string current_string = "";
					unsigned int token_idx = 1;
					unsigned int substr_idx = 0;
					while (token_idx < token.parameters.size()) {
						auto comma_pos = token.parameters[token_idx].find(',', substr_idx);
						if (comma_pos == string::npos) {
							current_string += token.parameters[token_idx].substr(substr_idx) + " ";
							token_idx++;
							substr_idx = 0;
						} else {
							current_string += token.parameters[token_idx].substr(substr_idx, comma_pos);
							StringUtil::Trim(current_string);
							string_set->insert(current_string);
							current_string = "";
							substr_idx = comma_pos + 1;
						}
					}
					StringUtil::Trim(current_string);
					string_set->insert(current_string);
					string_set->erase("");
				}
			} else {
				parser.Fail("unrecognized set parameter: %s", token.parameters[0]);
			}
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_LOOP ||
		           token.type == SQLLogicTokenType::SQLLOGIC_CONCURRENT_LOOP) {
			if (token.parameters.size() != 3) {
				parser.Fail("Expected loop [iterator_name] [start] [end] (e.g. loop i 1 300)");
			}
			LoopDefinition def;
			def.loop_iterator_name = token.parameters[0];
			try {
				def.loop_start = std::stoi(token.parameters[1].c_str());
				def.loop_end = std::stoi(token.parameters[2].c_str());
			} catch (...) {
				parser.Fail("loop_start and loop_end must be a number");
			}
			def.loop_idx = def.loop_start;
			def.is_parallel = token.type == SQLLogicTokenType::SQLLOGIC_CONCURRENT_LOOP;
			StartLoop(def);
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_FOREACH ||
		           token.type == SQLLogicTokenType::SQLLOGIC_CONCURRENT_FOREACH) {
			if (token.parameters.size() < 2) {
				parser.Fail("expected foreach [iterator_name] [m1] [m2] [etc...] (e.g. foreach type integer "
				            "smallint float)");
			}
			LoopDefinition def;
			def.loop_iterator_name = token.parameters[0];
			for (idx_t i = 1; i < token.parameters.size(); i++) {
				D_ASSERT(!token.parameters[i].empty());
				if (!ForEachTokenReplace(token.parameters[i], def.tokens)) {
					def.tokens.push_back(token.parameters[i]);
				}
			}
			def.loop_idx = 0;
			def.loop_start = 0;
			def.loop_end = def.tokens.size();
			def.is_parallel = token.type == SQLLogicTokenType::SQLLOGIC_CONCURRENT_FOREACH;
			StartLoop(def);
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_ENDLOOP) {
			EndLoop();
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_REQUIRE) {
			auto require_result = CheckRequire(parser, token.parameters);
			if (require_result == RequireResult::MISSING) {
				auto &param = token.parameters[0];
				if (IsRequired(param)) {
					// This extension / setting was explicitly required
					parser.Fail(StringUtil::Format("require %s: FAILED", param));
				}
				SKIP_TEST("require " + token.parameters[0]);
				return;
			}
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_REQUIRE_ENV) {
			if (InLoop()) {
				parser.Fail("require-env cannot be called in a loop");
			}

			if (token.parameters.size() != 1 && token.parameters.size() != 2) {
				parser.Fail("require-env requires 1 argument: <env name> [optional: <expected env val>]");
			}

			auto env_var = token.parameters[0];
			auto env_actual = std::getenv(env_var.c_str());
			if (env_actual == nullptr) {
				// Environment variable was not found, this test should not be run
				SKIP_TEST("require-env " + token.parameters[0]);
				return;
			}

			if (token.parameters.size() == 2) {
				// Check that the value is the same as the expected value
				auto env_value = token.parameters[1];
				if (std::strcmp(env_actual, env_value.c_str()) != 0) {
					// It's not, check the test
					SKIP_TEST("require-env " + token.parameters[0] + " " + token.parameters[1]);
					return;
				}
			}

			if (environment_variables.count(env_var)) {
				parser.Fail(StringUtil::Format("Environment variable '%s' has already been defined", env_var));
			}
			environment_variables[env_var] = env_actual;

		} else if (token.type == SQLLogicTokenType::SQLLOGIC_LOAD) {
			bool readonly = token.parameters.size() > 1 && token.parameters[1] == "readonly";
			string load_db_path;
			if (!token.parameters.empty()) {
				load_db_path = ReplaceKeywords(token.parameters[0]);
			} else {
				load_db_path = string();
			}
			auto command = make_uniq<LoadCommand>(*this, load_db_path, readonly);
			ExecuteCommand(std::move(command));
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_RESTART) {
			bool load_extensions = !(token.parameters.size() == 1 && token.parameters[0] == "no_extension_load");

			// restart the current database
			auto command = make_uniq<RestartCommand>(*this, load_extensions);
			ExecuteCommand(std::move(command));
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_RECONNECT) {
			auto command = make_uniq<ReconnectCommand>(*this);
			ExecuteCommand(std::move(command));
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_SLEEP) {
			if (token.parameters.size() != 2) {
				parser.Fail("sleep requires two parameter (e.g. sleep 1 second)");
			}
			// require a specific block size
			auto sleep_duration = std::stoull(token.parameters[0]);
			auto sleep_unit = SleepCommand::ParseUnit(token.parameters[1]);
			auto command = make_uniq<SleepCommand>(*this, sleep_duration, sleep_unit);
			ExecuteCommand(std::move(command));
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_UNZIP) {
			if (token.parameters.size() != 1 && token.parameters.size() != 2) {
				parser.Fail("unzip requires 1 argument: <path/to/file.db.gz> [optional: "
				            "<path/to/unzipped_file.db>, default: __TEST_DIR__/<file.db>]");
			}

			// set input path
			auto input_path = ReplaceKeywords(token.parameters[0]);

			// file name
			idx_t filename_start_pos = input_path.find_last_of("/") + 1;
			if (!StringUtil::EndsWith(input_path, CompressionExtensionFromType(FileCompressionType::GZIP))) {
				parser.Fail("unzip: input has not a GZIP extension");
			}
			string filename = input_path.substr(filename_start_pos, input_path.size() - filename_start_pos - 3);

			// extraction path
			string default_extraction_path = ReplaceKeywords("__TEST_DIR__/" + filename);
			string extraction_path =
			    (token.parameters.size() == 2) ? ReplaceKeywords(token.parameters[1]) : default_extraction_path;
			if (extraction_path == "NULL") {
				extraction_path = default_extraction_path;
			}

			auto command = make_uniq<UnzipCommand>(*this, input_path, extraction_path);
			ExecuteCommand(std::move(command));
		}
	}
	if (InLoop()) {
		parser.Fail("Missing endloop!");
	}
}

} // namespace duckdb
