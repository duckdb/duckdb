#include "sqllogic_command.hpp"
#include "sqllogic_test_runner.hpp"
#include "result_helper.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "test_helpers.hpp"
#include "sqllogic_test_logger.hpp"
#include "catch.hpp"
#include <list>
#include <thread>

namespace duckdb {

static void query_break(int line) {
	(void)line;
}

static Connection *GetConnection(DuckDB &db,
                                 unordered_map<string, duckdb::unique_ptr<Connection>> &named_connection_map,
                                 string con_name) {
	auto entry = named_connection_map.find(con_name);
	if (entry == named_connection_map.end()) {
		// not found: create a new connection
		auto con = make_uniq<Connection>(db);
		auto res = con.get();
		named_connection_map[con_name] = std::move(con);
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
	vector<duckdb::unique_ptr<SQLStatement>> statements;
	bool query_fail = false;
	try {
		statements = connection->context->ParseStatements(sql_query);
	} catch (...) {
		query_fail = true;
	}
	bool can_restart = true;
	for (auto &conn : connection->context->db->GetConnectionManager().connections) {
		if (!conn.first->client_data->prepared_statements.empty()) {
			can_restart = false;
		}
		if (conn.first->transaction.HasActiveTransaction()) {
			can_restart = false;
		}
	}
	if (!query_fail && can_restart && !runner.skip_reload) {
		// We basically restart the database if no transaction is active and if the query is valid
		auto command = make_uniq<RestartCommand>(runner);
		runner.ExecuteCommand(std::move(command));
		connection = CommandConnection(context);
	}
}

unique_ptr<MaterializedQueryResult> Command::ExecuteQuery(ExecuteContext &context, Connection *connection,
                                                          string file_name, idx_t query_line) const {
	query_break(query_line);
	if (TestForceReload() && TestForceStorage()) {
		RestartDatabase(context, connection, context.sql_query);
	}

	return connection->Query(context.sql_query);
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

ReconnectCommand::ReconnectCommand(SQLLogicTestRunner &runner) : Command(runner) {
}

LoopCommand::LoopCommand(SQLLogicTestRunner &runner, LoopDefinition definition_p)
    : Command(runner), definition(std::move(definition_p)) {
}

struct ParallelExecuteContext {
	ParallelExecuteContext(SQLLogicTestRunner &runner, const vector<duckdb::unique_ptr<Command>> &loop_commands,
	                       LoopDefinition definition)
	    : runner(runner), loop_commands(loop_commands), definition(std::move(definition)), success(true) {
	}

	SQLLogicTestRunner &runner;
	const vector<duckdb::unique_ptr<Command>> &loop_commands;
	LoopDefinition definition;
	atomic<bool> success;
	string error_message;
	string error_file;
	int error_line;
};

static void ParallelExecuteLoop(ParallelExecuteContext *execute_context) {
	try {
		auto &runner = execute_context->runner;

		// construct a new connection to the database
		Connection con(*runner.db);
		// create a new parallel execute context
		vector<LoopDefinition> running_loops {execute_context->definition};
		ExecuteContext context(&con, std::move(running_loops));
		for (auto &command : execute_context->loop_commands) {
			execute_context->error_file = command->file_name;
			execute_context->error_line = command->query_line;
			command->Execute(context);
		}
		if (!context.error_file.empty()) {
			execute_context->error_message = string();
			execute_context->success = false;
			execute_context->error_file = context.error_file;
			execute_context->error_line = context.error_line;
		}
	} catch (std::exception &ex) {
		execute_context->error_message = StringUtil::Format("Failure at %s:%d: %s", execute_context->error_file,
		                                                    execute_context->error_line, ex.what());
		execute_context->success = false;
	} catch (...) {
		execute_context->error_message = StringUtil::Format("Failure at %s:%d: Unknown error message",
		                                                    execute_context->error_file, execute_context->error_line);
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
				if (!context.error_message.empty()) {
					FAIL(context.error_message);
				} else {
					FAIL_LINE(context.error_file, context.error_line, 0);
				}
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
	if (!helper.CheckQueryResult(*this, context, std::move(result))) {
		if (context.is_parallel) {
			runner.finished_processing_file = true;
			context.error_file = file_name;
			context.error_line = query_line;
		} else {
			FAIL_LINE(file_name, query_line, 0);
		}
	}
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

	runner.LoadDatabase(runner.dbpath);

	runner.con->context->config = client_config;

	runner.con->BeginTransaction();
	runner.con->context->client_data->catalog_search_path->Set(catalog_search_paths, CatalogSetPathType::SET_SCHEMAS);
	runner.con->Commit();
	if (!low_query_writer_path.empty()) {
		runner.con->context->client_data->log_query_writer = make_uniq<BufferedFileWriter>(
		    FileSystem::GetFileSystem(*runner.con->context), low_query_writer_path, 1 << 1 | 1 << 5);
	}
}

void ReconnectCommand::ExecuteInternal(ExecuteContext &context) const {
	if (context.is_parallel) {
		throw std::runtime_error("Cannot reconnect in parallel");
	}
	runner.Reconnect();
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
	if (!helper.CheckStatementResult(*this, context, std::move(result))) {
		if (context.is_parallel) {
			runner.finished_processing_file = true;
			context.error_file = file_name;
			context.error_line = query_line;
		} else {
			FAIL_LINE(file_name, query_line, 0);
		}
	}
}

} // namespace duckdb
