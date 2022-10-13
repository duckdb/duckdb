#include "sqllogic_command.hpp"
#include "test_helper_extension.hpp"
#include "sqllogic_test_runner.hpp"
#include "result_helper.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "test_helpers.hpp"
#include <list>
#include <thread>

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

Connection *Command::CommandConnection(ExecuteContext &context) const {
	if (connection_name.empty()) {
		if (context.is_parallel) {
			D_ASSERT(context.con);
			return context.con;
		}
		D_ASSERT(!context.con);
		return runner.con.get();
	} else {
		if (context.is_parallel) {
			throw std::runtime_error("Named connections not supported in parallel loop");
		}
		return GetConnection(*runner.db, runner.named_connection_map, connection_name);
	}
}

void Command::RestartDatabase(ExecuteContext &context, Connection *&connection, string sql_query) const {
	if (context.is_parallel) {
		// cannot restart in parallel
		return;
	}
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
		connection = CommandConnection(context);
	}
}

unique_ptr<MaterializedQueryResult> Command::ExecuteQuery(ExecuteContext &context, Connection *connection,
                                                          string file_name, idx_t query_line) const {
	query_break(query_line);
	if (TestForceReload() && TestForceStorage()) {
		RestartDatabase(context, connection, context.sql_query);
	}

	auto result = connection->Query(context.sql_query);

	if (result->HasError()) {
		TestHelperExtension::SetLastError(result->GetError());
	} else {
		TestHelperExtension::ClearLastError();
	}

	return result;
}

void Command::Execute(ExecuteContext &context) const {
	if (runner.finished_processing_file) {
		return;
	}
	if (context.running_loops.empty()) {
		context.sql_query = base_sql_query;
		ExecuteInternal(context);
		return;
	}
	// perform the string replacement
	context.sql_query = SQLLogicTestRunner::LoopReplacement(base_sql_query, context.running_loops);
	// execute the iterated statement
	ExecuteInternal(context);
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

struct ParallelExecuteContext {
	ParallelExecuteContext(SQLLogicTestRunner &runner, const vector<unique_ptr<Command>> &loop_commands,
	                       LoopDefinition definition)
	    : runner(runner), loop_commands(loop_commands), definition(move(definition)), success(true) {
	}

	SQLLogicTestRunner &runner;
	const vector<unique_ptr<Command>> &loop_commands;
	LoopDefinition definition;
	atomic<bool> success;
	string error_message;
};

static void ParallelExecuteLoop(ParallelExecuteContext *execute_context) {
	try {
		auto &runner = execute_context->runner;

		// construct a new connection to the database
		Connection con(*runner.db);
		// create a new parallel execute context
		vector<LoopDefinition> running_loops {execute_context->definition};
		ExecuteContext context(&con, move(running_loops));
		for (auto &command : execute_context->loop_commands) {
			command->Execute(context);
		}
	} catch (std::exception &ex) {
		execute_context->error_message = ex.what();
		execute_context->success = false;
	} catch (...) {
		execute_context->error_message = "Unknown error message";
		execute_context->success = false;
	}
}

void LoopCommand::ExecuteInternal(ExecuteContext &context) const {
	LoopDefinition loop_def = definition;
	loop_def.loop_idx = definition.loop_start;
	if (loop_def.is_parallel) {
		if (context.is_parallel || !context.running_loops.empty()) {
			throw std::runtime_error("Nested parallel loop commands not allowed");
		}
		// parallel loop: launch threads
		std::list<ParallelExecuteContext> contexts;
		while (true) {
			contexts.emplace_back(runner, loop_commands, loop_def);
			loop_def.loop_idx++;
			if (loop_def.loop_idx >= loop_def.loop_end) {
				// finished
				break;
			}
		}
		std::list<std::thread> threads;
		for (auto &context : contexts) {
			threads.emplace_back(ParallelExecuteLoop, &context);
		}
		for (auto &thread : threads) {
			thread.join();
		}
		for (auto &context : contexts) {
			if (!context.success) {
				FAIL(context.error_message);
			}
		}
	} else {
		bool finished = false;
		while (!finished && !runner.finished_processing_file) {
			// execute the current iteration of the loop
			context.running_loops.push_back(loop_def);
			for (auto &statement : loop_commands) {
				statement->Execute(context);
			}
			context.running_loops.pop_back();
			loop_def.loop_idx++;
			if (loop_def.loop_idx >= loop_def.loop_end) {
				// finished
				break;
			}
		}
	}
}

void Query::ExecuteInternal(ExecuteContext &context) const {
	auto connection = CommandConnection(context);

	{
		SQLLogicTestLogger logger(context, *this);
		if (runner.output_result_mode || runner.debug_mode) {
			logger.PrintLineSep();
			logger.PrintFileHeader();
			logger.PrintSQLFormatted();
			logger.PrintLineSep();
		}

		if (runner.output_sql) {
			logger.PrintSQL();
			return;
		}
	}
	auto result = ExecuteQuery(context, connection, file_name, query_line);

	TestResultHelper helper(runner);
	helper.CheckQueryResult(*this, context, move(result));
}

void RestartCommand::ExecuteInternal(ExecuteContext &context) const {
	if (context.is_parallel) {
		throw std::runtime_error("Cannot restart database in parallel");
	}
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

void Statement::ExecuteInternal(ExecuteContext &context) const {
	auto connection = CommandConnection(context);

	{
		SQLLogicTestLogger logger(context, *this);
		if (runner.output_result_mode || runner.debug_mode) {
			logger.PrintLineSep();
			logger.PrintFileHeader();
			logger.PrintSQLFormatted();
			logger.PrintLineSep();
		}

		query_break(query_line);
		if (runner.output_sql) {
			logger.PrintSQL();
			return;
		}
	}
	auto result = ExecuteQuery(context, connection, file_name, query_line);

	TestResultHelper helper(runner);
	helper.CheckStatementResult(*this, context, move(result));
}

} // namespace duckdb
