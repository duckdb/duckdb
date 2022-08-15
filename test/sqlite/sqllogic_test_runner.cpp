
#include "catch.hpp"

#include "sqllogic_test_runner.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "sqllogic_parser.hpp"
#include "test_helpers.hpp"
#include "test_helper_extension.hpp"

namespace duckdb {

SQLLogicTestRunner::SQLLogicTestRunner(string dbpath) : dbpath(move(dbpath)) {
	config = GetTestConfig();
	config->options.load_extensions = false;
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

void SQLLogicTestRunner::ExecuteCommand(unique_ptr<Command> command) {
	if (InLoop()) {
		active_loops.back()->loop_commands.push_back(move(command));
	} else {
		command->Execute();
	}
}

void SQLLogicTestRunner::StartLoop(LoopDefinition definition) {
	auto loop = make_unique<LoopCommand>(*this, move(definition));
	auto loop_ptr = loop.get();
	if (InLoop()) {
		// already in a loop: add it to the currently active loop
		active_loops.back()->loop_commands.push_back(move(loop));
	} else {
		// not in a loop yet: new top-level loop
		top_level_loop = move(loop);
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
		top_level_loop->Execute();
		top_level_loop.reset();
	}
}

void SQLLogicTestRunner::LoadDatabase(string dbpath) {
	loaded_databases.push_back(dbpath);

	// restart the database with the specified db path
	db.reset();
	con.reset();
	named_connection_map.clear();
	// now re-open the current database

	db = make_unique<DuckDB>(dbpath, config.get());
	con = make_unique<Connection>(*db);
	if (enable_verification) {
		con->EnableQueryVerification();
	}

	// load any previously loaded extensions again
	for (auto &extension : extensions) {
		ExtensionHelper::LoadExtension(*db, extension);
	}
}

string SQLLogicTestRunner::ReplaceLoopIterator(string text, string loop_iterator_name, string replacement) {
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

string SQLLogicTestRunner::LoopReplacement(string text, const vector<LoopDefinition *> &loops) {
	for (auto &active_loop : loops) {
		if (active_loop->tokens.empty()) {
			// regular loop
			text = ReplaceLoopIterator(text, active_loop->loop_iterator_name, to_string(active_loop->loop_idx));
		} else {
			// foreach loop
			text =
			    ReplaceLoopIterator(text, active_loop->loop_iterator_name, active_loop->tokens[active_loop->loop_idx]);
		}
	}
	return text;
}

string SQLLogicTestRunner::ReplaceKeywords(string input) {
	input = StringUtil::Replace(input, "__TEST_DIR__", TestDirectoryPath());
	input = StringUtil::Replace(input, "__WORKING_DIRECTORY__", FileSystem::GetWorkingDirectory());
	input = StringUtil::Replace(input, "__BUILD_DIRECTORY__", DUCKDB_BUILD_DIRECTORY);
	return input;
}

bool SQLLogicTestRunner::ForEachTokenReplace(const string &parameter, vector<string> &result) {
	auto token_name = StringUtil::Lower(parameter);
	StringUtil::Trim(token_name);
	bool collection = false;
	bool is_compression = token_name == "<compression>";
	bool is_all = token_name == "<alltypes>";
	bool is_numeric = is_all || token_name == "<numeric>";
	bool is_integral = is_numeric || token_name == "<integral>";
	bool is_signed = is_integral || token_name == "<signed>";
	bool is_unsigned = is_integral || token_name == "<unsigned>";
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
		result.push_back("json");
		collection = true;
	}
	if (is_compression) {
		result.push_back("none");
		result.push_back("uncompressed");
		result.push_back("rle");
		result.push_back("bitpacking");
		result.push_back("dictionary");
		collection = true;
	}
	return collection;
}

void SQLLogicTestRunner::ExecuteFile(string script) {
	SQLLogicParser parser;
	idx_t skip_level = 0;

	// for the original SQLite tests we convert floating point numbers to integers
	// for our own tests this is undesirable since it hides certain errors
	if (script.find("sqlite") != string::npos || script.find("sqllogictest") != string::npos) {
		original_sqlite_test = true;
	}

	if (!dbpath.empty()) {
		// delete the target database file, if it exists
		DeleteDatabase(dbpath);
	}

	// initialize the database with the default dbpath
	LoadDatabase(dbpath);

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

		bool skip_statement = false;
		while (token.type == SQLLogicTokenType::SQLLOGIC_SKIP_IF || token.type == SQLLogicTokenType::SQLLOGIC_ONLY_IF) {
			// skipif/onlyif
			bool skip_if = token.type == SQLLogicTokenType::SQLLOGIC_SKIP_IF;
			if (token.parameters.size() < 1) {
				parser.Fail("skipif/onlyif requires a single parameter (e.g. skipif duckdb)");
			}
			auto system_name = StringUtil::Lower(token.parameters[0]);
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
			auto command = make_unique<Statement>(*this);

			// parse the first parameter
			if (token.parameters[0] == "ok") {
				command->expect_ok = true;
			} else if (token.parameters[0] == "error") {
				command->expect_ok = false;
			} else {
				parser.Fail("statement argument should be 'ok' or 'error");
			}

			command->file_name = script;
			command->query_line = parser.current_line + 1;

			// extract the SQL statement
			parser.NextLine();
			auto statement_text = parser.ExtractStatement(false);
			if (statement_text.empty()) {
				parser.Fail("Unexpected empty statement text");
			}

			// perform any renames in the text
			command->sql_query = ReplaceKeywords(move(statement_text));

			if (token.parameters.size() >= 2) {
				command->connection_name = token.parameters[1];
			}
			ExecuteCommand(move(command));
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_QUERY) {
			if (token.parameters.size() < 1) {
				parser.Fail("query requires at least one parameter (query III)");
			}
			auto command = make_unique<Query>(*this);

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
			auto statement_text = parser.ExtractStatement(true);

			// perform any renames in the text
			command->sql_query = ReplaceKeywords(move(statement_text));

			// extract the expected result
			command->values = parser.ExtractExpectedResult();

			// figure out the sort style/connection style
			command->sort_style = SortStyle::NO_SORT;
			if (token.parameters.size() > 1) {
				auto &sort_style = token.parameters[1];
				if (sort_style == "nosort") {
					/* Do no sorting */
					command->sort_style = SortStyle::NO_SORT;
				} else if (sort_style == "rowsort") {
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
			ExecuteCommand(move(command));
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
			if (token.parameters[0] == "output_hash") {
				output_hash_mode = true;
			} else if (token.parameters[0] == "output_result") {
				output_result_mode = true;
			} else if (token.parameters[0] == "debug") {
				debug_mode = true;
			} else if (token.parameters[0] == "skip") {
				skip_level++;
			} else if (token.parameters[0] == "unskip") {
				skip_level--;
			} else {
				parser.Fail("unrecognized mode: %s", token.parameters[0]);
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
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_LOOP) {
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
			StartLoop(def);
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_FOREACH) {
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
			StartLoop(def);
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_ENDLOOP) {
			EndLoop();
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_REQUIRE) {
			if (token.parameters.size() < 1) {
				parser.Fail("require requires a single parameter");
			}
			// require command
			string param = StringUtil::Lower(token.parameters[0]);
			// os specific stuff
			if (param == "notmingw") {
#ifdef __MINGW32__
				return;
#endif
			} else if (param == "mingw") {
#ifndef __MINGW32__
				return;
#endif
			} else if (param == "notwindows") {
#ifdef _WIN32
				return;
#endif
			} else if (param == "windows") {
#ifndef _WIN32
				return;
#endif
			} else if (param == "longdouble") {
#if LDBL_MANT_DIG < 54
				return;
#endif
			} else if (param == "64bit") {
				if (sizeof(void *) != 8) {
					return;
				}
			} else if (param == "noforcestorage") {
				if (TestForceStorage()) {
					return;
				}
			} else if (param == "vector_size") {
				if (token.parameters.size() != 2) {
					parser.Fail("require vector_size requires a parameter");
				}
				// require a specific vector size
				auto required_vector_size = std::stoi(token.parameters[1]);
				if (STANDARD_VECTOR_SIZE < required_vector_size) {
					// vector size is too low for this test: skip it
					return;
				}
			} else if (param == "test_helper") {
				db->LoadExtension<TestHelperExtension>();
			} else if (param == "skip_reload") {
				skip_reload = true;
			} else {
				auto result = ExtensionHelper::LoadExtension(*db, param);
				if (result == ExtensionLoadResult::LOADED_EXTENSION) {
					// add the extension to the list of loaded extensions
					extensions.insert(param);
				} else if (result == ExtensionLoadResult::EXTENSION_UNKNOWN) {
					parser.Fail("unknown extension type: %s", token.parameters[0]);
				} else if (result == ExtensionLoadResult::NOT_LOADED) {
					// extension known but not build: skip this test
					return;
				}
			}
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_REQUIRE_ENV) {
			if (token.parameters.size() < 2) {
				parser.Fail("require-env requires 2 arguments: <env name> <env value>");
			}

			auto env_var = token.parameters[0];
			auto env_value = token.parameters[1];

			auto env_actual = std::getenv(env_var.c_str());

			if (env_actual == nullptr || std::strcmp(env_actual, env_value.c_str()) != 0) {
				// Environment variable was not found, this test should not be run
				return;
			}
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_LOAD) {
			if (InLoop()) {
				parser.Fail("load cannot be called in a loop");
			}

			bool readonly = token.parameters.size() > 1 && token.parameters[1] == "readonly";
			if (!token.parameters.empty()) {
				dbpath = ReplaceKeywords(token.parameters[0]);
				if (!readonly) {
					// delete the target database file, if it exists
					DeleteDatabase(dbpath);
				}
			} else {
				dbpath = string();
			}
			// set up the config file
			if (readonly) {
				config->options.use_temporary_directory = false;
				config->options.access_mode = AccessMode::READ_ONLY;
			} else {
				config->options.use_temporary_directory = true;
				config->options.access_mode = AccessMode::AUTOMATIC;
			}
			// now create the database file
			LoadDatabase(dbpath);
		} else if (token.type == SQLLogicTokenType::SQLLOGIC_RESTART) {
			if (dbpath.empty()) {
				parser.Fail("cannot restart an in-memory database, did you forget to call \"load\"?");
			}
			// restart the current database
			// first clear all connections
			auto command = make_unique<RestartCommand>(*this);
			ExecuteCommand(move(command));
		}
	}
	if (InLoop()) {
		parser.Fail("Missing endloop!");
	}
}

} // namespace duckdb
