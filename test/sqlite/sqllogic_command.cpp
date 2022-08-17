#include "sqllogic_command.hpp"
#include "test_helper_extension.hpp"
#include "sqllogic_test_runner.hpp"
#include "result_helper.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "test_helpers.hpp"

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

void Command::RestartDatabase(Connection *&connection, string sql_query) {
	vector<unique_ptr<SQLStatement>> statements;
	bool query_fail = false;
	try {
		statements = connection->context->ParseStatements(sql_query);
	} catch (...) {
		query_fail = true;
	}
	bool is_any_transaction_active = false;
	for (auto &conn : connection->context->db->GetConnectionManager().connections) {
		if (conn.first->transaction.HasActiveTransaction()) {
			is_any_transaction_active = true;
		}
	}
	if (!query_fail && !is_any_transaction_active && !runner.skip_reload) {
		// We basically restart the database if no transaction is active and if the query is valid
		auto command = make_unique<RestartCommand>(runner);
		runner.ExecuteCommand(move(command));
		connection = CommandConnection();
	}
}

unique_ptr<MaterializedQueryResult> Command::ExecuteQuery(Connection *connection, string file_name, idx_t query_line,
                                                          string sql_query) {
	query_break(query_line);
	if (TestForceReload() && TestForceStorage()) {
		RestartDatabase(connection, sql_query);
	}

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
	// We save the main connection configurations to pass it to the new connection
	runner.config->options = runner.con->context->db->config.options;
	auto client_config = runner.con->context->config;
	auto catalog_search_paths = runner.con->context->client_data->catalog_search_path->GetSetPaths();
	string low_query_writer_path;
	if (runner.con->context->client_data->log_query_writer) {
		low_query_writer_path = runner.con->context->client_data->log_query_writer->path;
	}

	auto prepared_statements = move(runner.con->context->client_data->prepared_statements);

	runner.LoadDatabase(runner.dbpath);

	runner.con->context->config = client_config;

	runner.con->context->client_data->catalog_search_path->Set(catalog_search_paths);
	if (!low_query_writer_path.empty()) {
		runner.con->context->client_data->log_query_writer =
		    make_unique<BufferedFileWriter>(FileSystem::GetFileSystem(*runner.con->context), low_query_writer_path,
		                                    1 << 1 | 1 << 5, runner.con->context->client_data->file_opener.get());
	}
	runner.con->context->client_data->prepared_statements = move(prepared_statements);
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
