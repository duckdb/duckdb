#include "sqllogic_command.hpp"
#include "test_helper_extension.hpp"
#include "sqllogic_test_runner.hpp"
#include "result_helper.hpp"

namespace duckdb {

static void query_break(int line) {
	(void)line;
}

static Connection *GetConnection(DuckDB &db, unordered_map<string, unique_ptr<Connection>> &named_connection_map,
                                 string con_name) {
	auto entry = named_connection_map.find(con_name);
	if (entry == named_connection_map.end()) {
		// not found: create a new connection
		auto con = make_unique<Connection>(db);
		auto res = con.get();
		named_connection_map[con_name] = move(con);
		return res;
	}
	return entry->second.get();
}

Command::Command(SQLLogicTestRunner &runner) : runner(runner) {
}

Command::~Command() {
}

Connection *Command::CommandConnection() {
	if (connection_name.empty()) {
		return runner.con.get();
	} else {
		return GetConnection(*runner.db, runner.named_connection_map, connection_name);
	}
}

unique_ptr<MaterializedQueryResult> Command::ExecuteQuery(Connection *connection, string file_name, idx_t query_line,
                                                          string sql_query) {
	query_break(query_line);
	auto result = connection->Query(sql_query);

	if (!result->success) {
		TestHelperExtension::SetLastError(result->error);
	} else {
		TestHelperExtension::ClearLastError();
	}

	return result;
}

void Command::Execute() {
	if (runner.finished_processing_file) {
		return;
	}
	if (runner.running_loops.empty()) {
		ExecuteInternal();
		return;
	}
	auto original_query = sql_query;
	// perform the string replacement
	sql_query = SQLLogicTestRunner::LoopReplacement(sql_query, runner.running_loops);
	// execute the iterated statement
	ExecuteInternal();
	// now restore the original query
	sql_query = original_query;
}

Statement::Statement(SQLLogicTestRunner &runner) : Command(runner) {
}

Query::Query(SQLLogicTestRunner &runner) : Command(runner) {
}

RestartCommand::RestartCommand(SQLLogicTestRunner &runner) : Command(runner) {
}

LoopCommand::LoopCommand(SQLLogicTestRunner &runner, LoopDefinition definition_p)
    : Command(runner), definition(move(definition_p)) {
}

void LoopCommand::ExecuteInternal() {
	definition.loop_idx = definition.loop_start;
	runner.running_loops.push_back(&definition);
	bool finished = false;
	while (!finished && !runner.finished_processing_file) {
		// execute the current iteration of the loop
		for (auto &statement : loop_commands) {
			statement->Execute();
		}
		definition.loop_idx++;
		if (definition.loop_idx >= definition.loop_end) {
			// finished
			break;
		}
	}
	runner.running_loops.pop_back();
}

static void OutputSQLQuery(const string &sql_query) {
	string query = sql_query;
	if (StringUtil::EndsWith(sql_query, "\n")) {
		// ends with a newline: don't add one
		if (!StringUtil::EndsWith(sql_query, ";\n")) {
			// no semicolon though
			query[query.size() - 1] = ';';
			query += "\n";
		}
	} else {
		if (!StringUtil::EndsWith(sql_query, ";")) {
			query += ";";
		}
		query += "\n";
	}
	fprintf(stderr, "%s", query.c_str());
}

void Query::ExecuteInternal() {
	auto connection = CommandConnection();

	if (runner.output_result_mode || runner.debug_mode) {
		TestResultHelper::PrintLineSep();
		TestResultHelper::PrintHeader("File " + file_name + ":" + to_string(query_line) + ")");
		TestResultHelper::PrintSQL(sql_query);
		TestResultHelper::PrintLineSep();
	}

	if (runner.output_sql) {
		OutputSQLQuery(sql_query);
		return;
	}
	auto result = ExecuteQuery(connection, file_name, query_line, sql_query);

	TestResultHelper helper(*this, *result);
	helper.CheckQueryResult(move(result));
}

void RestartCommand::ExecuteInternal() {
	runner.LoadDatabase(runner.dbpath);
}

void Statement::ExecuteInternal() {
	auto connection = CommandConnection();

	if (runner.output_result_mode || runner.debug_mode) {
		TestResultHelper::PrintLineSep();
		TestResultHelper::PrintHeader("File " + file_name + ":" + to_string(query_line) + ")");
		TestResultHelper::PrintSQL(sql_query);
		TestResultHelper::PrintLineSep();
	}

	query_break(query_line);
	if (runner.output_sql) {
		OutputSQLQuery(sql_query);
		return;
	}
	auto result = ExecuteQuery(connection, file_name, query_line, sql_query);

	TestResultHelper helper(*this, *result);
	helper.CheckStatementResult();
}

} // namespace duckdb
