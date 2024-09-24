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
#include "duckdb/main/stream_query_result.hpp"
#include <chrono>

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
	auto &connection_manager = connection->context->db->GetConnectionManager();
	auto &connection_list = connection_manager.GetConnectionListReference();
	for (auto &conn_ref : connection_list) {
		auto &conn = conn_ref.first.get();
		if (!conn.client_data->prepared_statements.empty()) {
			can_restart = false;
		}
		if (conn.transaction.HasActiveTransaction()) {
			can_restart = false;
		}
	}
	if (!query_fail && can_restart && !runner.skip_reload) {
		// We basically restart the database if no transaction is active and if the query is valid
		auto command = make_uniq<RestartCommand>(runner, true);
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
#ifdef DUCKDB_ALTERNATIVE_VERIFY
	auto ccontext = connection->context;
	auto result = ccontext->Query(context.sql_query, true);
	if (result->type == QueryResultType::STREAM_RESULT) {
		auto &stream_result = result->Cast<StreamQueryResult>();
		return stream_result.Materialize();
	} else {
		D_ASSERT(result->type == QueryResultType::MATERIALIZED_RESULT);
		return unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
	}
#else
	return connection->Query(context.sql_query);
#endif
}

bool CheckLoopCondition(ExecuteContext &context, const vector<Condition> &conditions) {
	if (conditions.empty()) {
		// no conditions
		return true;
	}
	if (context.running_loops.empty()) {
		throw BinderException("Conditions (onlyif/skipif) on loop parameters can only occur within a loop");
	}
	for (auto &condition : conditions) {
		bool condition_holds = false;
		bool found_loop = false;
		for (auto &loop : context.running_loops) {
			if (loop.loop_iterator_name != condition.keyword) {
				continue;
			}
			found_loop = true;

			string loop_value;
			if (loop.tokens.empty()) {
				loop_value = to_string(loop.loop_idx);
			} else {
				loop_value = loop.tokens[loop.loop_idx];
			}
			if (condition.comparison == ExpressionType::COMPARE_EQUAL ||
			    condition.comparison == ExpressionType::COMPARE_NOTEQUAL) {
				// equality/non-equality is done on the string value
				if (condition.comparison == ExpressionType::COMPARE_EQUAL) {
					condition_holds = loop_value == condition.value;
				} else {
					condition_holds = loop_value != condition.value;
				}
			} else {
				// > >= < <= are done on numeric values
				int64_t loop_val = std::stoll(loop_value);
				int64_t condition_val = std::stoll(condition.value);
				switch (condition.comparison) {
				case ExpressionType::COMPARE_GREATERTHAN:
					condition_holds = loop_val > condition_val;
					break;
				case ExpressionType::COMPARE_LESSTHAN:
					condition_holds = loop_val < condition_val;
					break;
				case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
					condition_holds = loop_val >= condition_val;
					break;
				case ExpressionType::COMPARE_LESSTHANOREQUALTO:
					condition_holds = loop_val <= condition_val;
					break;
				default:
					throw BinderException("Unrecognized comparison for loop condition");
				}
			}
		}
		if (!found_loop) {
			throw BinderException("Condition in onlyif/skipif not found: %s must be a loop iterator name",
			                      condition.keyword);
		}
		if (condition_holds) {
			// the condition holds
			if (condition.skip_if) {
				// skip on condition holding
				return false;
			}
		} else {
			// the condition does not hold
			if (!condition.skip_if) {
				// skip on condition not holding
				return false;
			}
		}
	}
	// all conditions pass - execute
	return true;
}

void Command::Execute(ExecuteContext &context) const {
	if (runner.finished_processing_file) {
		return;
	}
	if (!CheckLoopCondition(context, conditions)) {
		// condition excludes this file
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

RestartCommand::RestartCommand(SQLLogicTestRunner &runner, bool load_extensions_p)
    : Command(runner), load_extensions(load_extensions_p) {
}

ReconnectCommand::ReconnectCommand(SQLLogicTestRunner &runner) : Command(runner) {
}

LoopCommand::LoopCommand(SQLLogicTestRunner &runner, LoopDefinition definition_p)
    : Command(runner), definition(std::move(definition_p)) {
}

ModeCommand::ModeCommand(SQLLogicTestRunner &runner, string parameter_p)
    : Command(runner), parameter(std::move(parameter_p)) {
}

SleepCommand::SleepCommand(SQLLogicTestRunner &runner, idx_t duration, SleepUnit unit)
    : Command(runner), duration(duration), unit(unit) {
}

UnzipCommand::UnzipCommand(SQLLogicTestRunner &runner, string &input, string &output)
    : Command(runner), input_path(input), extraction_path(output) {
}

LoadCommand::LoadCommand(SQLLogicTestRunner &runner, string dbpath_p, bool readonly)
    : Command(runner), dbpath(std::move(dbpath_p)), readonly(readonly) {
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
		for (auto &running_loop : context.running_loops) {
			if (running_loop.is_parallel) {
				throw std::runtime_error("Nested parallel loop commands not allowed");
			}
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
	if (runner.dbpath.empty()) {
		throw std::runtime_error("cannot restart an in-memory database, did you forget to call \"load\"?");
	}
	// We save the main connection configurations to pass it to the new connection
	runner.config->options = runner.con->context->db->config.options;
	auto client_config = runner.con->context->config;
	auto catalog_search_paths = runner.con->context->client_data->catalog_search_path->GetSetPaths();
	string low_query_writer_path;
	if (runner.con->context->client_data->log_query_writer) {
		low_query_writer_path = runner.con->context->client_data->log_query_writer->path;
	}

	runner.LoadDatabase(runner.dbpath, load_extensions);

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

void ModeCommand::ExecuteInternal(ExecuteContext &context) const {
	if (parameter == "output_hash") {
		runner.output_hash_mode = true;
	} else if (parameter == "output_result") {
		runner.output_result_mode = true;
	} else if (parameter == "no_output") {
		runner.output_hash_mode = false;
		runner.output_result_mode = false;
	} else if (parameter == "debug") {
		runner.debug_mode = true;
	} else {
		throw std::runtime_error("unrecognized mode: " + parameter);
	}
}

void SleepCommand::ExecuteInternal(ExecuteContext &context) const {
	switch (unit) {
	case SleepUnit::NANOSECOND:
		std::this_thread::sleep_for(std::chrono::duration<double, std::nano>(duration));
		break;
	case SleepUnit::MICROSECOND:
		std::this_thread::sleep_for(std::chrono::duration<double, std::micro>(duration));
		break;
	case SleepUnit::MILLISECOND:
		std::this_thread::sleep_for(std::chrono::duration<double, std::milli>(duration));
		break;
	case SleepUnit::SECOND:
		std::this_thread::sleep_for(std::chrono::duration<double, std::milli>(duration * 1000));
		break;
	default:
		throw std::runtime_error("Unrecognized sleep unit");
	}
}

SleepUnit SleepCommand::ParseUnit(const string &unit) {
	if (unit == "second" || unit == "seconds" || unit == "sec") {
		return SleepUnit::SECOND;
	} else if (unit == "millisecond" || unit == "milliseconds" || unit == "milli") {
		return SleepUnit::MILLISECOND;
	} else if (unit == "microsecond" || unit == "microseconds" || unit == "micro") {
		return SleepUnit::MICROSECOND;
	} else if (unit == "nanosecond" || unit == "nanoseconds" || unit == "nano") {
		return SleepUnit::NANOSECOND;
	} else {
		throw std::runtime_error("Unrecognized sleep mode - expected second/millisecond/microescond/nanosecond");
	}
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

void UnzipCommand::ExecuteInternal(ExecuteContext &context) const {
	VirtualFileSystem vfs;

	// input
	FileOpenFlags in_flags(FileFlags::FILE_FLAGS_READ);
	in_flags.SetCompression(FileCompressionType::GZIP);
	auto compressed_file_handle = vfs.OpenFile(input_path, in_flags);
	if (compressed_file_handle == nullptr) {
		throw CatalogException("Cannot open the file \"%s\"", input_path);
	}

	// output
	FileOpenFlags out_flags(FileOpenFlags::FILE_FLAGS_FILE_CREATE | FileOpenFlags::FILE_FLAGS_WRITE);
	auto output_file = vfs.OpenFile(extraction_path, out_flags);
	if (!output_file) {
		throw CatalogException("Cannot open the file \"%s\"", extraction_path);
	}

	// read the compressed data from the file
	while (true) {
		std::unique_ptr<char[]> compressed_buffer(new char[BUFFER_SIZE]);
		int64_t bytes_read = vfs.Read(*compressed_file_handle, compressed_buffer.get(), BUFFER_SIZE);
		if (bytes_read == 0) {
			break;
		}

		vfs.Write(*output_file, compressed_buffer.get(), bytes_read);
	}
}

void LoadCommand::ExecuteInternal(ExecuteContext &context) const {
	auto resolved_path = SQLLogicTestRunner::LoopReplacement(dbpath, context.running_loops);
	if (!readonly) {
		// delete the target database file, if it exists
		DeleteDatabase(resolved_path);
	}
	runner.dbpath = resolved_path;

	// set up the config file
	if (readonly) {
		runner.config->options.use_temporary_directory = false;
		runner.config->options.access_mode = AccessMode::READ_ONLY;
	} else {
		runner.config->options.use_temporary_directory = true;
		runner.config->options.access_mode = AccessMode::AUTOMATIC;
	}
	// now create the database file
	runner.LoadDatabase(resolved_path, true);
}

} // namespace duckdb
