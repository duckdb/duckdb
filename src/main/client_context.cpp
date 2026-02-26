#include "duckdb/main/client_context.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/statement/relation_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/column_data_ref.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/pragma_handler.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/result_set_manager.hpp"

#ifdef __APPLE__
#include <sys/sysctl.h>

// code adapted from Apple's example
// https://developer.apple.com/documentation/apple-silicon/about-the-rosetta-translation-environment#Determine-Whether-Your-App-Is-Running-as-a-Translated-Binary
static bool OsxRosettaIsActive() {
	int ret = 0;
	size_t size = sizeof(ret);
	if (sysctlbyname("sysctl.proc_translated", &ret, &size, NULL, 0)) {
		return false;
	}
	return ret == 1;
}

#endif

namespace duckdb {

struct ActiveQueryContext {
public:
	//! The query that is currently being executed
	string query;
	//! Prepared statement data
	shared_ptr<PreparedStatementData> prepared;
	//! The query executor
	unique_ptr<Executor> executor;
	//! The progress bar
	unique_ptr<ProgressBar> progress_bar;

public:
	void SetOpenResult(BaseQueryResult &result) {
		open_result = &result;
	}
	bool IsOpenResult(BaseQueryResult &result) {
		return open_result == &result;
	}
	bool HasOpenResult() const {
		return open_result != nullptr;
	}

private:
	//! The currently open result
	BaseQueryResult *open_result = nullptr;
};

#ifdef DEBUG
struct DebugClientContextState : public ClientContextState {
	~DebugClientContextState() override {
		if (Exception::UncaughtException()) {
			return;
		}
		D_ASSERT(!active_transaction);
		D_ASSERT(!active_query);
	}

	bool active_transaction = false;
	bool active_query = false;

	void QueryBegin(ClientContext &context) override {
		if (active_query) {
			throw InternalException("DebugClientContextState::QueryBegin called when a query is already active");
		}
		active_query = true;
	}
	void QueryEnd(ClientContext &context) override {
		if (!active_query) {
			throw InternalException("DebugClientContextState::QueryEnd called when no query is active");
		}
		active_query = false;
	}
	void TransactionBegin(MetaTransaction &transaction, ClientContext &context) override {
		if (active_transaction) {
			throw InternalException(
			    "DebugClientContextState::TransactionBegin called when a transaction is already active");
		}
		active_transaction = true;
	}
	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override {
		if (!active_transaction) {
			throw InternalException("DebugClientContextState::TransactionCommit called when no transaction is active");
		}
		active_transaction = false;
	}
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		if (!active_transaction) {
			throw InternalException(
			    "DebugClientContextState::TransactionRollback called when no transaction is active");
		}
		active_transaction = false;
	}
#ifdef DUCKDB_DEBUG_REBIND
	RebindQueryInfo OnPlanningError(ClientContext &context, SQLStatement &statement, ErrorData &error) override {
		return RebindQueryInfo::ATTEMPT_TO_REBIND;
	}
	RebindQueryInfo OnFinalizePrepare(ClientContext &context, PreparedStatementData &prepared,
	                                  PreparedStatementMode mode) override {
		if (mode == PreparedStatementMode::PREPARE_AND_EXECUTE) {
			return RebindQueryInfo::ATTEMPT_TO_REBIND;
		}
		return RebindQueryInfo::DO_NOT_REBIND;
	}
	RebindQueryInfo OnExecutePrepared(ClientContext &context, PreparedStatementCallbackInfo &info,
	                                  RebindQueryInfo current_rebind) override {
		return RebindQueryInfo::ATTEMPT_TO_REBIND;
	}
#endif
};
#endif

ClientContext::ClientContext(shared_ptr<DatabaseInstance> database)
    : db(std::move(database)), interrupted(false), transaction(*this), connection_id(DConstants::INVALID_INDEX) {
	registered_state = make_uniq<RegisteredStateManager>();
#ifdef DEBUG
	registered_state->GetOrCreate<DebugClientContextState>("debug_client_context_state");
#endif
	LoggingContext context(LogContextScope::CONNECTION);
	logger = db->GetLogManager().CreateLogger(context, true);
	client_data = make_uniq<ClientData>(*this);

#ifdef __APPLE__
	if (OsxRosettaIsActive()) {
		DUCKDB_LOG_WARNING(*this, "OSX binary translation ('Rosetta') detected. Running DuckDB through Rosetta will "
		                          "cause a significant performance degradation. DuckDB is available natively on Apple "
		                          "silicon, please download an appropriate binary here: https://duckdb.org/install/");
	}
#endif
}

ClientContext::~ClientContext() {
	if (Exception::UncaughtException()) {
		return;
	}
	// destroy the client context and rollback if there is an active transaction
	// but only if we are not destroying this client context as part of an exception stack unwind
	Destroy();
}

unique_ptr<ClientContextLock> ClientContext::LockContext() {
	return make_uniq<ClientContextLock>(context_lock);
}

void ClientContext::Destroy() {
	auto lock = LockContext();
	if (transaction.HasActiveTransaction()) {
		transaction.ResetActiveQuery();
		if (!transaction.IsAutoCommit()) {
			transaction.Rollback(nullptr);
		}
	}
	CleanupInternal(*lock);
}

void ClientContext::ProcessError(ErrorData &error, const string &query) const {
	error.FinalizeError();
	if (Settings::Get<ErrorsAsJSONSetting>(*this)) {
		error.ConvertErrorToJSON();
	} else {
		error.AddErrorLocation(query);
	}
}

template <class T>
unique_ptr<T> ClientContext::ErrorResult(ErrorData error, const string &query) {
	ProcessError(error, query);
	return make_uniq<T>(std::move(error));
}

void ClientContext::BeginQueryInternal(ClientContextLock &lock, const string &query) {
	// check if we are on AutoCommit. In this case we should start a transaction
	D_ASSERT(!active_query);
	auto &db_inst = DatabaseInstance::GetDatabase(*this);
	if (ValidChecker::IsInvalidated(db_inst)) {
		throw ErrorManager::InvalidatedDatabase(*this, ValidChecker::InvalidatedMessage(db_inst));
	}
	active_query = make_uniq<ActiveQueryContext>();
	if (transaction.IsAutoCommit()) {
		transaction.BeginTransaction();
	}

	transaction.SetActiveQuery(db->GetDatabaseManager().GetNewQueryNumber());
	LogQueryInternal(lock, query);
	active_query->query = query;

	query_progress.Initialize();
	// Set query deadline if max_execution_time is configured
	auto max_execution_time = Settings::Get<MaxExecutionTimeSetting>(*this);
	if (max_execution_time > 0) {
		auto now = steady_clock::now();
		auto deadline_tp = now + milliseconds(max_execution_time);
		query_deadline = NumericCast<idx_t>(duration_cast<milliseconds>(deadline_tp.time_since_epoch()).count());
	} else {
		query_deadline.SetInvalid();
	}
	// Notify any registered state of query begin
	for (auto &state : registered_state->States()) {
		state->QueryBegin(*this);
	}

	// Flush the old logger.
	logger->Flush();

	// Refresh the logger to ensure we are in sync with the global log settings.
	LoggingContext logging_context(LogContextScope::CONNECTION);
	logging_context.connection_id = connection_id;
	logging_context.transaction_id = transaction.ActiveTransaction().global_transaction_id;
	logging_context.query_id = transaction.GetActiveQuery();
	logger = db->GetLogManager().CreateLogger(logging_context, true);
	DUCKDB_LOG(*this, QueryLogType, query);
}

ErrorData ClientContext::EndQueryInternal(ClientContextLock &lock, bool success, bool invalidate_transaction,
                                          optional_ptr<ErrorData> previous_error) {
	if (active_query->executor) {
		active_query->executor->CancelTasks();
	}
	active_query->progress_bar.reset();
	D_ASSERT(active_query.get());
	active_query.reset();
	query_deadline.SetInvalid();
	query_progress.Initialize();
	ErrorData error;
	try {
		if (transaction.HasActiveTransaction()) {
			transaction.ResetActiveQuery();
			if (transaction.IsAutoCommit()) {
				if (success) {
					transaction.Commit();
				} else {
					transaction.Rollback(previous_error);
				}
			} else if (invalidate_transaction) {
				D_ASSERT(!success);
				ValidChecker::Invalidate(ActiveTransaction(), "Failed to commit");
			}
		}
	} catch (std::exception &ex) {
		error = ErrorData(ex);
		if (Exception::InvalidatesDatabase(error.Type()) || error.Type() == ExceptionType::INTERNAL) {
			auto &db_inst = DatabaseInstance::GetDatabase(*this);
			ValidChecker::Invalidate(db_inst, error.RawMessage());
		}
	} catch (...) { // LCOV_EXCL_START
		error = ErrorData("Unhandled exception!");
	} // LCOV_EXCL_STOP

	client_data->profiler->EndQuery();

	// Refresh the logger
	logger->Flush();
	LoggingContext context(LogContextScope::CONNECTION);
	context.connection_id = reinterpret_cast<idx_t>(this);
	logger = db->GetLogManager().CreateLogger(context, true);

	// Notify any registered state of query end
	for (auto const &s : registered_state->States()) {
		if (error.HasError()) {
			s->QueryEnd(*this, &error);
		} else {
			s->QueryEnd(*this, previous_error);
		}
	}
	return error;
}

void ClientContext::CleanupInternal(ClientContextLock &lock, BaseQueryResult *result, bool invalidate_transaction) {
	if (!active_query) {
		// no query currently active
		return;
	}
	if (active_query->executor) {
		active_query->executor->CancelTasks();
	}
	active_query->progress_bar.reset();

	// Relaunch the threads if a SET THREADS command was issued
	auto &scheduler = TaskScheduler::GetScheduler(*this);
	scheduler.RelaunchThreads();

	optional_ptr<ErrorData> passed_error = nullptr;
	if (result && result->HasError()) {
		passed_error = result->GetErrorObject();
	}
	auto error = EndQueryInternal(lock, result ? !result->HasError() : false, invalidate_transaction, passed_error);
	if (result && !result->HasError()) {
		// if an error occurred while committing report it in the result
		result->SetError(error);
	}
	D_ASSERT(!active_query);
}

Executor &ClientContext::GetExecutor() {
	D_ASSERT(active_query);
	D_ASSERT(active_query->executor);
	return *active_query->executor;
}

Logger &ClientContext::GetLogger() const {
	return *logger;
}

const string &ClientContext::GetCurrentQuery() {
	D_ASSERT(active_query);
	return active_query->query;
}

connection_t ClientContext::GetConnectionId() const {
	return connection_id;
}

unique_ptr<QueryResult> ClientContext::FetchResultInternal(ClientContextLock &lock, PendingQueryResult &pending) {
	D_ASSERT(active_query);
	D_ASSERT(active_query->IsOpenResult(pending));
	D_ASSERT(active_query->prepared);
	auto &executor = GetExecutor();
	auto &prepared = *active_query->prepared;
	bool create_stream_result =
	    prepared.properties.output_type == QueryResultOutputType::ALLOW_STREAMING && pending.allow_stream_result;
	unique_ptr<QueryResult> result;
	D_ASSERT(executor.HasResultCollector());
	// we have a result collector - fetch the result directly from the result collector
	result = executor.GetResult();
	if (!create_stream_result) {
		CleanupInternal(lock, result.get(), false);
	} else {
		active_query->SetOpenResult(*result);
	}
	return result;
}

static bool IsExplainAnalyze(SQLStatement *statement) {
	if (!statement) {
		return false;
	}
	if (statement->type != StatementType::EXPLAIN_STATEMENT) {
		return false;
	}
	auto &explain = statement->Cast<ExplainStatement>();
	return explain.explain_type == ExplainType::EXPLAIN_ANALYZE;
}

shared_ptr<PreparedStatementData> ClientContext::CreatePreparedStatementInternal(ClientContextLock &lock,
                                                                                 const string &query,
                                                                                 unique_ptr<SQLStatement> statement,
                                                                                 PendingQueryParameters parameters) {
	StatementType statement_type = statement->type;
	auto result = make_shared_ptr<PreparedStatementData>(statement_type);

	auto &profiler = QueryProfiler::Get(*this);
	profiler.StartQuery(query, IsExplainAnalyze(statement.get()), true);
	profiler.StartPhase(MetricType::PLANNER);
	Planner logical_planner(*this);
	if (parameters.parameters) {
		auto &parameter_values = *parameters.parameters;
		for (auto &value : parameter_values) {
			logical_planner.parameter_data.emplace(value.first, BoundParameterData(value.second));
		}
	}

	logical_planner.CreatePlan(std::move(statement));
	D_ASSERT(logical_planner.plan || !logical_planner.properties.bound_all_parameters);
	profiler.EndPhase();

	auto logical_plan = std::move(logical_planner.plan);
	// extract the result column names from the plan
	result->properties = logical_planner.properties;
	result->names = logical_planner.names;
	result->types = logical_planner.types;
	result->value_map = std::move(logical_planner.value_map);
	if (!logical_planner.properties.bound_all_parameters) {
		// not all parameters were bound - return
		return result;
	}
#ifdef DEBUG
	logical_plan->Verify(*this);
#endif
	if (result->properties.parameter_count > 0 && !parameters.parameters) {
		// if this is a prepared statement we can choose not to fully plan
		// if we have parameters, we might want to re-bind when they are available as we can then do more optimizations
		// in this situation we check if we want to cache the plan at all
		if (!PreparedStatement::CanCachePlan(*logical_plan)) {
			// we don't - early-out
			result->properties.always_require_rebind = true;
			return result;
		}
	}

	if (config.enable_optimizer && logical_plan->RequireOptimizer()) {
		profiler.StartPhase(MetricType::ALL_OPTIMIZERS);
		Optimizer optimizer(*logical_planner.binder, *this);
		logical_plan = optimizer.Optimize(std::move(logical_plan));
		D_ASSERT(logical_plan);
		profiler.EndPhase();

#ifdef DEBUG
		logical_plan->Verify(*this);
#endif
	}

	// Convert the logical query plan into a physical query plan.
	profiler.StartPhase(MetricType::PHYSICAL_PLANNER);
	PhysicalPlanGenerator physical_planner(*this);
	result->physical_plan = physical_planner.Plan(std::move(logical_plan));
	profiler.EndPhase();
	D_ASSERT(result->physical_plan);
	return result;
}

shared_ptr<PreparedStatementData> ClientContext::CreatePreparedStatement(ClientContextLock &lock, const string &query,
                                                                         unique_ptr<SQLStatement> statement,
                                                                         PendingQueryParameters parameters,
                                                                         PreparedStatementMode mode) {
	// check if any client context state could request a rebind
	bool can_request_rebind = false;
	for (auto &state : registered_state->States()) {
		if (state->CanRequestRebind()) {
			can_request_rebind = true;
		}
	}
	if (can_request_rebind) {
		bool rebind = false;
		// if any registered state can request a rebind we do the binding on a copy first
		shared_ptr<PreparedStatementData> result;
		try {
			result = CreatePreparedStatementInternal(lock, query, statement->Copy(), parameters);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			// check if any registered client context state wants to try a rebind
			for (auto &state : registered_state->States()) {
				auto info = state->OnPlanningError(*this, *statement, error);
				if (info == RebindQueryInfo::ATTEMPT_TO_REBIND) {
					rebind = true;
				}
			}
			if (!rebind) {
				throw;
			}
		}
		if (result) {
			D_ASSERT(!rebind);
			for (auto &state : registered_state->States()) {
				auto info = state->OnFinalizePrepare(*this, *result, mode);
				if (info == RebindQueryInfo::ATTEMPT_TO_REBIND) {
					rebind = true;
				}
			}
		}
		if (!rebind) {
			return result;
		}
		// an extension wants to do a rebind - do it once
	}

	return CreatePreparedStatementInternal(lock, query, std::move(statement), parameters);
}

QueryProgress ClientContext::GetQueryProgress() {
	return query_progress;
}

void BindPreparedStatementParameters(PreparedStatementData &statement, const PendingQueryParameters &parameters) {
	case_insensitive_map_t<BoundParameterData> owned_values;
	if (parameters.parameters) {
		auto &params = *parameters.parameters;
		for (auto &val : params) {
			owned_values.emplace(val);
		}
	}
	statement.Bind(std::move(owned_values));
}

void ClientContext::RebindPreparedStatement(ClientContextLock &lock, const string &query,
                                            shared_ptr<PreparedStatementData> &prepared,
                                            const PendingQueryParameters &parameters) {
	if (!prepared->unbound_statement) {
		throw InternalException("ClientContext::RebindPreparedStatement called but PreparedStatementData did not have "
		                        "an unbound statement so rebinding cannot be done");
	}
	// catalog was modified: rebind the statement before execution
	auto new_prepared = CreatePreparedStatement(lock, query, prepared->unbound_statement->Copy(), parameters);
	D_ASSERT(new_prepared->properties.bound_all_parameters);
	new_prepared->properties.parameter_count = prepared->properties.parameter_count;
	prepared = std::move(new_prepared);
	prepared->properties.bound_all_parameters = false;
}

void ClientContext::CheckIfPreparedStatementIsExecutable(PreparedStatementData &statement) {
	if (ValidChecker::IsInvalidated(ActiveTransaction()) && statement.properties.requires_valid_transaction) {
		throw ErrorManager::InvalidatedTransaction(*this);
	}

	auto &meta_transaction = MetaTransaction::Get(*this);
	auto &manager = DatabaseManager::Get(*this);
	for (auto &it : statement.properties.modified_databases) {
		auto &modified_database = it.first;
		auto entry = manager.GetDatabase(*this, modified_database);
		if (!entry) {
			// database has been detached
			throw InvalidInputException("Database \"%s\" not found", modified_database);
		}
		if (entry->IsReadOnly()) {
			throw InvalidInputException(StringUtil::Format(
			    "Cannot execute statement of type \"%s\" on database \"%s\" which is attached in read-only mode!",
			    StatementTypeToString(statement.statement_type), modified_database));
		}
		meta_transaction.ModifyDatabase(*entry, it.second.modifications);
	}
}

unique_ptr<PendingQueryResult>
ClientContext::PendingPreparedStatementInternal(ClientContextLock &lock,
                                                shared_ptr<PreparedStatementData> statement_data_p,
                                                const PendingQueryParameters &parameters) {
	D_ASSERT(active_query);
	auto &statement_data = *statement_data_p;
	BindPreparedStatementParameters(statement_data, parameters);

	// Create the query executor.
	active_query->executor = make_uniq<Executor>(*this);
	auto &executor = *active_query->executor;

	if (config.enable_progress_bar) {
		progress_bar_display_create_func_t display_create_func = nullptr;
		if (config.print_progress_bar) {
			// Use either a custom display function, or the default.
			display_create_func =
			    config.display_create_func ? config.display_create_func : ProgressBar::DefaultProgressBarDisplay;
		}
		active_query->progress_bar =
		    make_uniq<ProgressBar>(executor, NumericCast<idx_t>(config.wait_time), display_create_func);
		active_query->progress_bar->Start();
		query_progress.Restart();
	}

	const auto stream_result = parameters.query_parameters.output_type == QueryResultOutputType::ALLOW_STREAMING &&
	                           statement_data.properties.output_type == QueryResultOutputType::ALLOW_STREAMING;

	// Decide how to get the result collector.
	get_result_collector_t get_collector = PhysicalResultCollector::GetResultCollector;
	auto &client_config = ClientConfig::GetConfig(*this);
	if (!stream_result && client_config.get_result_collector) {
		get_collector = client_config.get_result_collector;
	}
	statement_data.output_type =
	    stream_result ? QueryResultOutputType::ALLOW_STREAMING : QueryResultOutputType::FORCE_MATERIALIZED;
	statement_data.memory_type = parameters.query_parameters.memory_type;

	// Get the result collector and initialize the executor.
	auto &collector = get_collector(*this, statement_data);
	D_ASSERT(collector.type == PhysicalOperatorType::RESULT_COLLECTOR);
	executor.Initialize(collector);

	auto types = executor.GetTypes();
	D_ASSERT(types == statement_data.types);
	D_ASSERT(!active_query->HasOpenResult());

	auto pending_result =
	    make_uniq<PendingQueryResult>(shared_from_this(), *statement_data_p, std::move(types), stream_result);
	active_query->prepared = std::move(statement_data_p);
	active_query->SetOpenResult(*pending_result);
	return pending_result;
}

unique_ptr<PendingQueryResult> ClientContext::PendingPreparedStatement(ClientContextLock &lock, const string &query,
                                                                       shared_ptr<PreparedStatementData> prepared,
                                                                       const PendingQueryParameters &parameters) {
	CheckIfPreparedStatementIsExecutable(*prepared);

	RebindQueryInfo rebind = RebindQueryInfo::DO_NOT_REBIND;
	if (prepared->RequireRebind(*this, parameters.parameters)) {
		rebind = RebindQueryInfo::ATTEMPT_TO_REBIND;
	}

	for (auto &state : registered_state->States()) {
		PreparedStatementCallbackInfo info(*prepared, parameters);
		auto new_rebind = state->OnExecutePrepared(*this, info, rebind);
		if (new_rebind == RebindQueryInfo::ATTEMPT_TO_REBIND) {
			rebind = RebindQueryInfo::ATTEMPT_TO_REBIND;
		}
	}
	if (rebind == RebindQueryInfo::ATTEMPT_TO_REBIND) {
		RebindPreparedStatement(lock, query, prepared, parameters);
		CheckIfPreparedStatementIsExecutable(*prepared); // rerun this too as modified_databases might have changed
	}
	return PendingPreparedStatementInternal(lock, prepared, parameters);
}

void ClientContext::WaitForTask(ClientContextLock &lock, BaseQueryResult &result) {
	active_query->executor->WaitForTask();
}

PendingExecutionResult ClientContext::ExecuteTaskInternal(ClientContextLock &lock, BaseQueryResult &result,
                                                          bool dry_run) {
	D_ASSERT(active_query);
	D_ASSERT(active_query->IsOpenResult(result));
	bool invalidate_transaction = true;
	try {
		auto query_result = active_query->executor->ExecuteTask(dry_run);
		if (active_query->progress_bar) {
			auto is_finished = PendingQueryResult::IsResultReady(query_result);
			active_query->progress_bar->Update(is_finished);
			query_progress = active_query->progress_bar->GetDetailedQueryProgress();
		}
		return query_result;
	} catch (std::exception &ex) {
		auto error = ErrorData(ex);
		if (error.Type() == ExceptionType::INTERRUPT) {
			auto &executor = *active_query->executor;
			if (!executor.HasError()) {
				// Interrupted by the user
				result.SetError(ex);
				invalidate_transaction = true;
			} else {
				// Interrupted by an exception caused in a worker thread
				error = executor.GetError();
				invalidate_transaction = Exception::InvalidatesTransaction(error.Type());
				result.SetError(error);
			}
		} else if (!Exception::InvalidatesTransaction(error.Type())) {
			invalidate_transaction = false;
		} else if (Exception::InvalidatesDatabase(error.Type()) || error.Type() == ExceptionType::INTERNAL) {
			// fatal exceptions invalidate the entire database
			auto &db_instance = DatabaseInstance::GetDatabase(*this);
			ValidChecker::Invalidate(db_instance, error.RawMessage());
		}
		ProcessError(error, active_query->query);
		result.SetError(std::move(error));
	} catch (...) { // LCOV_EXCL_START
		result.SetError(ErrorData("Unhandled exception in ExecuteTaskInternal"));
	} // LCOV_EXCL_STOP
	EndQueryInternal(lock, false, invalidate_transaction, result.GetErrorObject());
	return PendingExecutionResult::EXECUTION_ERROR;
}

void ClientContext::InitialCleanup(ClientContextLock &lock) {
	//! Cleanup any open results and reset the interrupted flag
	CleanupInternal(lock);
	interrupted = false;
}

vector<unique_ptr<SQLStatement>> ClientContext::ParseStatements(const string &query) {
	auto lock = LockContext();
	return ParseStatementsInternal(*lock, query);
}

vector<unique_ptr<SQLStatement>> ClientContext::ParseStatementsInternal(ClientContextLock &lock, const string &query) {
	try {
		Parser parser(GetParserOptions());
		parser.ParseQuery(query);

		PragmaHandler handler(*this);
		handler.HandlePragmaStatements(lock, parser.statements);

		return std::move(parser.statements);
	} catch (std::exception &ex) {
		auto error = ErrorData(ex);
		ProcessError(error, query);
		error.Throw();
	}
}

void ClientContext::HandlePragmaStatements(vector<unique_ptr<SQLStatement>> &statements) {
	auto lock = LockContext();

	PragmaHandler handler(*this);
	handler.HandlePragmaStatements(*lock, statements);
}

unique_ptr<LogicalOperator> ClientContext::ExtractPlan(const string &query) {
	auto lock = LockContext();

	auto statements = ParseStatementsInternal(*lock, query);
	if (statements.size() != 1) {
		throw InvalidInputException("ExtractPlan can only prepare a single statement");
	}

	unique_ptr<LogicalOperator> plan;
	RunFunctionInTransactionInternal(*lock, [&]() {
		Planner planner(*this);
		planner.CreatePlan(std::move(statements[0]));
		D_ASSERT(planner.plan);

		plan = std::move(planner.plan);

		if (config.enable_optimizer) {
			Optimizer optimizer(*planner.binder, *this);
			plan = optimizer.Optimize(std::move(plan));
		}

		ColumnBindingResolver resolver;
		resolver.Verify(*plan);
		resolver.VisitOperator(*plan);

		plan->ResolveOperatorTypes();
	});
	return plan;
}

unique_ptr<PreparedStatement> ClientContext::PrepareInternal(ClientContextLock &lock,
                                                             unique_ptr<SQLStatement> statement) {
	auto named_param_map = statement->named_param_map;
	auto statement_query = statement->query;
	shared_ptr<PreparedStatementData> prepared_data;
	auto unbound_statement = statement->Copy();
	PendingQueryParameters parameters;
	RunFunctionInTransactionInternal(
	    lock,
	    [&]() { prepared_data = CreatePreparedStatement(lock, statement_query, std::move(statement), parameters); },
	    false);
	prepared_data->unbound_statement = std::move(unbound_statement);
	return make_uniq<PreparedStatement>(shared_from_this(), std::move(prepared_data), std::move(statement_query),
	                                    std::move(named_param_map));
}

unique_ptr<PreparedStatement> ClientContext::Prepare(unique_ptr<SQLStatement> statement) {
	auto lock = LockContext();
	// Store the query in case of an error.
	auto query = statement->query;

	// Try to prepare.
	try {
		InitialCleanup(*lock);
		return PrepareInternal(*lock, std::move(statement));
	} catch (std::exception &ex) {
		return ErrorResult<PreparedStatement>(ErrorData(ex), query);
	}
}

unique_ptr<PreparedStatement> ClientContext::Prepare(const string &query) {
	auto lock = LockContext();
	// prepare the query
	try {
		InitialCleanup(*lock);

		// first parse the query
		auto statements = ParseStatementsInternal(*lock, query);
		if (statements.empty()) {
			throw InvalidInputException("No statement to prepare!");
		}
		if (statements.size() > 1) {
			throw InvalidInputException("Cannot prepare multiple statements at once!");
		}
		return PrepareInternal(*lock, std::move(statements[0]));
	} catch (std::exception &ex) {
		return ErrorResult<PreparedStatement>(ErrorData(ex), query);
	}
}

unique_ptr<PendingQueryResult> ClientContext::PendingQueryPreparedInternal(ClientContextLock &lock, const string &query,
                                                                           shared_ptr<PreparedStatementData> &prepared,
                                                                           const PendingQueryParameters &parameters) {
	try {
		InitialCleanup(lock);
	} catch (std::exception &ex) {
		return ErrorResult<PendingQueryResult>(ErrorData(ex), query);
	}
	return PendingStatementOrPreparedStatementInternal(lock, query, nullptr, prepared, parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(const string &query,
                                                           shared_ptr<PreparedStatementData> &prepared,
                                                           const PendingQueryParameters &parameters) {
	auto lock = LockContext();
	return PendingQueryPreparedInternal(*lock, query, prepared, parameters);
}

unique_ptr<QueryResult> ClientContext::Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
                                               const PendingQueryParameters &parameters) {
	auto lock = LockContext();
	auto pending = PendingQueryPreparedInternal(*lock, query, prepared, parameters);
	if (pending->HasError()) {
		return ErrorResult<MaterializedQueryResult>(pending->GetErrorObject());
	}
	return pending->ExecuteInternal(*lock);
}

unique_ptr<QueryResult> ClientContext::Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
                                               case_insensitive_map_t<BoundParameterData> &values,
                                               QueryParameters query_parameters) {
	PendingQueryParameters parameters;
	parameters.parameters = &values;
	parameters.query_parameters = query_parameters;
	return Execute(query, prepared, parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingStatementInternal(ClientContextLock &lock, const string &query,
                                                                       unique_ptr<SQLStatement> statement,
                                                                       const PendingQueryParameters &parameters) {
	// prepare the query for execution
	if (parameters.parameters) {
		PreparedStatement::VerifyParameters(*parameters.parameters, statement->named_param_map);
	}

	auto prepared = CreatePreparedStatement(lock, query, std::move(statement), parameters,
	                                        PreparedStatementMode::PREPARE_AND_EXECUTE);

	idx_t parameter_count = !parameters.parameters ? 0 : parameters.parameters->size();
	if (prepared->properties.parameter_count > 0 && parameter_count == 0) {
		string error_message = StringUtil::Format("Expected %lld parameters, but none were supplied",
		                                          prepared->properties.parameter_count);
		return ErrorResult<PendingQueryResult>(InvalidInputException(error_message), query);
	}
	if (!prepared->properties.bound_all_parameters) {
		return ErrorResult<PendingQueryResult>(InvalidInputException("Not all parameters were bound"), query);
	}
	// execute the prepared statement
	CheckIfPreparedStatementIsExecutable(*prepared);
	return PendingPreparedStatementInternal(lock, std::move(prepared), parameters);
}

unique_ptr<QueryResult> ClientContext::RunStatementInternal(ClientContextLock &lock, const string &query,
                                                            unique_ptr<SQLStatement> statement,
                                                            const PendingQueryParameters &parameters, bool verify) {
	auto pending = PendingQueryInternal(lock, std::move(statement), parameters, verify);
	if (pending->HasError()) {
		return ErrorResult<MaterializedQueryResult>(pending->GetErrorObject());
	}
	return ExecutePendingQueryInternal(lock, *pending);
}

bool ClientContext::IsActiveResult(ClientContextLock &lock, BaseQueryResult &result) {
	if (!active_query) {
		return false;
	}
	return active_query->IsOpenResult(result);
}

unique_ptr<PendingQueryResult> ClientContext::PendingStatementOrPreparedStatementInternal(
    ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
    shared_ptr<PreparedStatementData> &prepared, const PendingQueryParameters &parameters) {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
	if (statement && statement->type != StatementType::LOGICAL_PLAN_STATEMENT) {
		statement = statement->Copy();
	}
#endif
	if (statement && config.query_verification_enabled) {
		// query verification is enabled
		// create a copy of the statement, and use the copy
		// this way we verify that the copy correctly copies all properties
		auto copied_statement = statement->Copy();
		switch (statement->type) {
		case StatementType::SELECT_STATEMENT: {
			// in case this is a select query, we verify the original statement
			ErrorData error;
			try {
				error = VerifyQuery(lock, query, std::move(statement), parameters);
			} catch (std::exception &ex) {
				error = ErrorData(ex);
			}
			if (error.HasError()) {
				// error in verifying query
				return ErrorResult<PendingQueryResult>(std::move(error), query);
			}
			statement = std::move(copied_statement);
			break;
		}
		default: {
#ifndef DUCKDB_ALTERNATIVE_VERIFY
			bool reparse_statement = true;
#else
			bool reparse_statement = false;
#endif
			statement = std::move(copied_statement);
			if (statement->type == StatementType::RELATION_STATEMENT) {
				reparse_statement = false;
			}
			if (reparse_statement) {
				try {
					Parser parser(GetParserOptions());
					ErrorData error;
					parser.ParseQuery(statement->ToString());
					if (statement->type == StatementType::UPDATE_STATEMENT) {
						// re-apply `prioritize_table_when_binding` (which is normally set during transform)
						parser.statements[0]->Cast<UpdateStatement>().prioritize_table_when_binding =
						    statement->Cast<UpdateStatement>().prioritize_table_when_binding;
					}
					statement = std::move(parser.statements[0]);
				} catch (const NotImplementedException &) {
					// ToString was not implemented, just use the copied statement
				}
			}
			break;
		}
		}
	}
	return PendingStatementOrPreparedStatement(lock, query, std::move(statement), prepared, parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingStatementOrPreparedStatement(
    ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
    shared_ptr<PreparedStatementData> &prepared, const PendingQueryParameters &parameters) {
	unique_ptr<PendingQueryResult> pending;

	// Start the profiler.
	auto &profiler = QueryProfiler::Get(*this);
	profiler.StartQuery(query, IsExplainAnalyze(statement ? statement.get() : prepared->unbound_statement.get()));

	try {
		BeginQueryInternal(lock, query);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (Exception::InvalidatesDatabase(error.Type())) {
			// fatal exceptions invalidate the entire database
			auto &db_instance = DatabaseInstance::GetDatabase(*this);
			ValidChecker::Invalidate(db_instance, error.RawMessage());
		}
		return ErrorResult<PendingQueryResult>(std::move(error), query);
	}

	bool invalidate_query = true;
	try {
		if (statement) {
			pending = PendingStatementInternal(lock, query, std::move(statement), parameters);
		} else {
			pending = PendingPreparedStatement(lock, query, prepared, parameters);
		}
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (!Exception::InvalidatesTransaction(error.Type())) {
			// standard exceptions do not invalidate the current transaction
			invalidate_query = false;
		} else if (Exception::InvalidatesDatabase(error.Type())) {
			// fatal exceptions invalidate the entire database
			if (!config.query_verification_enabled) {
				auto &db_instance = DatabaseInstance::GetDatabase(*this);
				ValidChecker::Invalidate(db_instance, error.RawMessage());
			}
		}
		// other types of exceptions do invalidate the current transaction
		pending = ErrorResult<PendingQueryResult>(std::move(error), query);
	}
	if (pending->HasError()) {
		// query failed: abort now
		EndQueryInternal(lock, false, invalidate_query, pending->GetErrorObject());
		return pending;
	}
	D_ASSERT(active_query->IsOpenResult(*pending));
	return pending;
}

void ClientContext::LogQueryInternal(ClientContextLock &, const string &query) {
	if (!client_data->log_query_writer) {
#ifdef DUCKDB_FORCE_QUERY_LOG
		try {
			string log_path(DUCKDB_FORCE_QUERY_LOG);
			client_data->log_query_writer = make_uniq<BufferedFileWriter>(FileSystem::GetFileSystem(*this), log_path,
			                                                              BufferedFileWriter::DEFAULT_OPEN_FLAGS);
		} catch (...) {
			return;
		}
#else
		return;
#endif
	}
	// log query path is set: log the query
	client_data->log_query_writer->WriteData(const_data_ptr_cast(query.c_str()), query.size());
	client_data->log_query_writer->WriteData(const_data_ptr_cast("\n"), 1);
	client_data->log_query_writer->Flush();
	client_data->log_query_writer->Sync();
}

unique_ptr<QueryResult> ClientContext::Query(unique_ptr<SQLStatement> statement, QueryParameters parameters) {
	auto pending_query = PendingQuery(std::move(statement), parameters);
	if (pending_query->HasError()) {
		return ErrorResult<MaterializedQueryResult>(pending_query->GetErrorObject());
	}
	return pending_query->Execute();
}

unique_ptr<QueryResult> ClientContext::Query(const string &query, QueryParameters query_parameters) {
	auto lock = LockContext();

	vector<unique_ptr<SQLStatement>> statements;
	try {
		statements = ParseStatements(*lock, query);
	} catch (const std::exception &ex) {
		return ErrorResult<MaterializedQueryResult>(ErrorData(ex), query);
	}
	if (statements.empty()) {
		// no statements, return empty successful result
		StatementProperties properties;
		vector<string> names;
		auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator());
		return make_uniq<MaterializedQueryResult>(StatementType::INVALID_STATEMENT, properties, std::move(names),
		                                          std::move(collection), GetClientProperties());
	}

	unique_ptr<QueryResult> result;
	optional_ptr<QueryResult> last_result;
	bool last_had_result = false;
	for (idx_t i = 0; i < statements.size(); i++) {
		auto &statement = statements[i];
		bool is_last_statement = i + 1 == statements.size();
		PendingQueryParameters parameters;
		parameters.query_parameters = query_parameters;
		if (!is_last_statement) {
			parameters.query_parameters.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
		}
		auto pending_query = PendingQueryInternal(*lock, std::move(statement), parameters);
		auto has_result = pending_query->properties.return_type == StatementReturnType::QUERY_RESULT;
		unique_ptr<QueryResult> current_result;
		if (pending_query->HasError()) {
			current_result = ErrorResult<MaterializedQueryResult>(pending_query->GetErrorObject());
		} else {
			current_result = ExecutePendingQueryInternal(*lock, *pending_query);
		}
		if (current_result->HasError()) {
			// Reset the interrupted flag, this was set by the task that found the error
			// Next statements should not be bothered by that interruption
			interrupted = false;
			return current_result;
		}
		// now append the result to the list of results
		if (!last_result || !last_had_result) {
			// first result of the query
			result = std::move(current_result);
			last_result = result.get();
			last_had_result = has_result;
		} else {
			// later results; attach to the result chain
			// but only if there is a result
			if (!has_result) {
				continue;
			}
			last_result->next = std::move(current_result);
			last_result = last_result->next.get();
		}
		D_ASSERT(last_result);
	}
	return result;
}

vector<unique_ptr<SQLStatement>> ClientContext::ParseStatements(ClientContextLock &lock, const string &query) {
	InitialCleanup(lock);
	// parse the query and transform it into a set of statements
	return ParseStatementsInternal(lock, query);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(const string &query, QueryParameters parameters) {
	case_insensitive_map_t<BoundParameterData> empty_param_list;
	return PendingQuery(query, empty_param_list, parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(unique_ptr<SQLStatement> statement,
                                                           QueryParameters parameters) {
	case_insensitive_map_t<BoundParameterData> empty_param_list;
	return PendingQuery(std::move(statement), empty_param_list, parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(const string &query,
                                                           case_insensitive_map_t<BoundParameterData> &values,
                                                           QueryParameters parameters) {
	PendingQueryParameters params;
	params.parameters = values;
	params.query_parameters = parameters;
	return PendingQuery(query, params);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(const string &query, PendingQueryParameters parameters) {
	auto lock = LockContext();
	try {
		InitialCleanup(*lock);

		auto statements = ParseStatementsInternal(*lock, query);
		if (statements.empty()) {
			throw InvalidInputException("No statement to prepare!");
		}
		if (statements.size() > 1) {
			throw InvalidInputException("Cannot prepare multiple statements at once!");
		}

		return PendingQueryInternal(*lock, std::move(statements[0]), parameters, true);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		ProcessError(error, query);
		return make_uniq<PendingQueryResult>(std::move(error));
	}
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(unique_ptr<SQLStatement> statement,
                                                           case_insensitive_map_t<BoundParameterData> &values,
                                                           QueryParameters parameters) {
	auto lock = LockContext();
	auto query = statement->query;
	try {
		InitialCleanup(*lock);

		PendingQueryParameters params;
		params.query_parameters = parameters;
		params.parameters = values;

		return PendingQueryInternal(*lock, std::move(statement), params, true);
	} catch (std::exception &ex) {
		return make_uniq<PendingQueryResult>(ErrorData(ex));
	}
}

unique_ptr<PendingQueryResult> ClientContext::PendingQueryInternal(ClientContextLock &lock,
                                                                   unique_ptr<SQLStatement> statement,
                                                                   const PendingQueryParameters &parameters,
                                                                   bool verify) {
	auto query = statement->query;
	shared_ptr<PreparedStatementData> prepared;
	if (verify) {
		return PendingStatementOrPreparedStatementInternal(lock, query, std::move(statement), prepared, parameters);
	} else {
		return PendingStatementOrPreparedStatement(lock, query, std::move(statement), prepared, parameters);
	}
}

unique_ptr<QueryResult> ClientContext::ExecutePendingQueryInternal(ClientContextLock &lock, PendingQueryResult &query) {
	return query.ExecuteInternal(lock);
}

void ClientContext::Interrupt() {
	interrupted = true;
}

bool ClientContext::IsInterrupted() const {
	return interrupted;
}

void ClientContext::ClearInterrupt() {
	interrupted = false;
}

void ClientContext::InterruptCheck() const {
	// Counter for throttling timeout checks - only check every N iterations
	static constexpr uint32_t TIMEOUT_CHECK_INTERVAL = 256;
	thread_local uint32_t timeout_check_counter = 0;

	if (interrupted.load(std::memory_order_relaxed)) {
		throw InterruptException();
	}
	// Only check timeout every N calls to avoid expensive steady_clock::now() syscall
	if (query_deadline.IsValid() && ++timeout_check_counter % TIMEOUT_CHECK_INTERVAL == 0) {
		auto now = NumericCast<idx_t>(duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count());
		if (now >= query_deadline.GetIndex()) {
			throw InterruptException();
		}
	}
}

void ClientContext::CancelTransaction() {
	auto lock = LockContext();
	InitialCleanup(*lock);
}

void ClientContext::EnableProfiling() {
	auto lock = LockContext();
	auto &client_config = ClientConfig::GetConfig(*this);
	client_config.enable_profiler = true;
	client_config.emit_profiler_output = true;
}

void ClientContext::DisableProfiling() {
	auto lock = LockContext();
	auto &client_config = ClientConfig::GetConfig(*this);
	client_config.enable_profiler = false;
}

void ClientContext::RegisterFunction(CreateFunctionInfo &info) {
	RunFunctionInTransaction([&]() {
		auto existing_function = Catalog::GetEntry<ScalarFunctionCatalogEntry>(*this, INVALID_CATALOG, info.schema,
		                                                                       info.name, OnEntryNotFound::RETURN_NULL);
		if (existing_function) {
			auto &new_info = info.Cast<CreateScalarFunctionInfo>();
			if (new_info.functions.MergeFunctionSet(existing_function->functions)) {
				// function info was updated from catalog entry, rewrite is needed
				info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
			}
		}
		// create function
		auto &catalog = Catalog::GetSystemCatalog(*this);
		catalog.CreateFunction(*this, info);
	});
}

void ClientContext::RunFunctionInTransactionInternal(ClientContextLock &lock, const std::function<void(void)> &fun,
                                                     bool requires_valid_transaction) {
	if (requires_valid_transaction && transaction.HasActiveTransaction() &&
	    ValidChecker::IsInvalidated(ActiveTransaction())) {
		throw TransactionException(ErrorManager::FormatException(*this, ErrorType::INVALIDATED_TRANSACTION));
	}

	// check if we are on AutoCommit. In this case we should start a transaction
	bool require_new_transaction = transaction.IsAutoCommit() && !transaction.HasActiveTransaction();
	if (require_new_transaction) {
		D_ASSERT(!active_query);
		transaction.BeginTransaction();
		interrupted = false;
	}
	try {
		fun();
	} catch (std::exception &ex) {
		ErrorData error(ex);
		bool invalidates_transaction = true;
		if (!Exception::InvalidatesTransaction(error.Type())) {
			// standard exceptions don't invalidate the transaction
			invalidates_transaction = false;
		} else if (Exception::InvalidatesDatabase(error.Type())) {
			auto &db_instance = DatabaseInstance::GetDatabase(*this);
			ValidChecker::Invalidate(db_instance, error.RawMessage());
		}
		if (require_new_transaction) {
			transaction.Rollback(error);
		} else if (invalidates_transaction) {
			ValidChecker::Invalidate(ActiveTransaction(), error.RawMessage());
		}
		throw;
	}
	if (require_new_transaction) {
		transaction.Commit();
	}
}

void ClientContext::RunFunctionInTransaction(const std::function<void(void)> &fun, bool requires_valid_transaction) {
	auto lock = LockContext();
	RunFunctionInTransactionInternal(*lock, fun, requires_valid_transaction);
}

unique_ptr<TableDescription> ClientContext::TableInfo(const string &database_name, const string &schema_name,
                                                      const string &table_name) {
	unique_ptr<TableDescription> result;
	RunFunctionInTransaction([&]() {
		// Obtain the table from the catalog.
		auto table = Catalog::GetEntry<TableCatalogEntry>(*this, database_name, schema_name, table_name,
		                                                  OnEntryNotFound::RETURN_NULL);
		if (!table) {
			return;
		}
		// Create the table description.
		result = make_uniq<TableDescription>(database_name, schema_name, table_name);
		auto &catalog = Catalog::GetCatalog(*this, database_name);
		result->readonly = catalog.GetAttached().IsReadOnly();
		for (auto &column : table->GetColumns().Logical()) {
			result->columns.emplace_back(column.Copy());
		}
	});
	return result;
}

unique_ptr<TableDescription> ClientContext::TableInfo(const string &schema_name, const string &table_name) {
	return TableInfo(INVALID_CATALOG, schema_name, table_name);
}

void ClientContext::Append(unique_ptr<SQLStatement> stmt) {
	auto result = Query(std::move(stmt), false);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to append: ");
	}
}

void ClientContext::Append(TableDescription &description, ColumnDataCollection &collection) {
	string table_name = "__duckdb_internal_appended_data";
	vector<string> expected_names;
	auto query = Appender::ConstructQuery(description, table_name, expected_names);
	auto table_ref = BaseAppender::GetColumnDataTableRef(collection, table_name, expected_names);
	auto stmt = BaseAppender::ParseStatement(std::move(table_ref), query, table_name);
	Append(std::move(stmt));
}

void ClientContext::InternalTryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns) {
	// bind the expressions
	auto binder = Binder::CreateBinder(*this);
	auto result = relation.Bind(*binder);
	D_ASSERT(result.names.size() == result.types.size());

	result_columns.reserve(result_columns.size() + result.names.size());
	for (idx_t i = 0; i < result.names.size(); i++) {
		result_columns.emplace_back(result.names[i], result.types[i]);
	}
}

void ClientContext::TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns) {
#ifdef DEBUG
	D_ASSERT(!relation.GetAlias().empty());
	D_ASSERT(!relation.ToString().empty());
#endif
	RunFunctionInTransaction([&]() { InternalTryBindRelation(relation, result_columns); });
}

unordered_set<string> ClientContext::GetTableNames(const string &query, const bool qualified) {
	auto lock = LockContext();

	auto statements = ParseStatementsInternal(*lock, query);
	if (statements.size() != 1) {
		throw InvalidInputException("Expected a single statement");
	}

	unordered_set<string> result;
	RunFunctionInTransactionInternal(*lock, [&]() {
		// bind the expressions
		auto binder = Binder::CreateBinder(*this);
		auto mode = qualified ? BindingMode::EXTRACT_QUALIFIED_NAMES : BindingMode::EXTRACT_NAMES;
		binder->SetBindingMode(mode);
		binder->Bind(*statements[0]);
		result = binder->GetTableNames();
	});
	return result;
}

unique_ptr<PendingQueryResult> ClientContext::PendingQueryInternal(ClientContextLock &lock,
                                                                   const shared_ptr<Relation> &relation,
                                                                   QueryParameters query_parameters) {
	InitialCleanup(lock);

	string query;
	if (config.query_verification_enabled) {
		// run the ToString method of any relation we run, mostly to ensure it doesn't crash
		relation->ToString();
		relation->GetAlias();
		if (relation->IsReadOnly()) {
			// verify read only statements by running a select statement
			auto select = make_uniq<SelectStatement>();
			select->node = relation->GetQueryNode();
			PendingQueryParameters parameters;
			parameters.query_parameters = query_parameters;
			parameters.query_parameters.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
			RunStatementInternal(lock, query, std::move(select), parameters);
		}
	}

	auto relation_stmt = make_uniq<RelationStatement>(relation);
	PendingQueryParameters parameters;
	parameters.query_parameters = query_parameters;
	return PendingQueryInternal(lock, std::move(relation_stmt), parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(const shared_ptr<Relation> &relation,
                                                           QueryParameters query_parameters) {
	auto lock = LockContext();
	return PendingQueryInternal(*lock, relation, query_parameters);
}

unique_ptr<QueryResult> ClientContext::Execute(const shared_ptr<Relation> &relation) {
	auto lock = LockContext();
	auto &expected_columns = relation->Columns();
	auto pending = PendingQueryInternal(*lock, relation, false);
	if (!pending->success) {
		return ErrorResult<MaterializedQueryResult>(pending->GetErrorObject());
	}

	unique_ptr<QueryResult> result;
	result = ExecutePendingQueryInternal(*lock, *pending);
	if (result->HasError()) {
		return result;
	}
	// verify that the result types and result names of the query match the expected result types/names
	if (result->types.size() == expected_columns.size()) {
		bool mismatch = false;
		for (idx_t i = 0; i < result->types.size(); i++) {
			if (result->types[i] != expected_columns[i].Type() || result->names[i] != expected_columns[i].Name()) {
				mismatch = true;
				break;
			}
		}
		if (!mismatch) {
			// all is as expected: return the result
			return result;
		}
	}
	// result mismatch
	string err_str = "Result mismatch in query!\nExpected the following columns: [";
	for (idx_t i = 0; i < expected_columns.size(); i++) {
		if (i > 0) {
			err_str += ", ";
		}
		err_str += expected_columns[i].Name() + " " + expected_columns[i].Type().ToString();
	}
	err_str += "]\nBut result contained the following: ";
	for (idx_t i = 0; i < result->types.size(); i++) {
		err_str += i == 0 ? "[" : ", ";
		err_str += result->names[i] + " " + result->types[i].ToString();
	}
	err_str += "]";
	return ErrorResult<MaterializedQueryResult>(ErrorData(err_str));
}

SettingLookupResult ClientContext::TryGetCurrentSetting(const string &key, Value &result) const {
	optional_ptr<const ConfigurationOption> option;
	// try to get the setting index
	auto &db_config = DBConfig::GetConfig(*this);
	auto setting_index = db_config.TryGetSettingIndex(key, option);
	if (setting_index.IsValid()) {
		// generic setting - try to fetch it
		auto lookup_result =
		    config.user_settings.TryGetSetting(db_config.user_settings, setting_index.GetIndex(), result);
		if (lookup_result) {
			return lookup_result;
		}
	}
	if (option && option->get_setting) {
		// legacy callback
		result = option->get_setting(*this);
		return SettingLookupResult(SettingScope::LOCAL);
	}
	// setting is not set - get the default value
	return DBConfig::TryGetDefaultValue(option, result);
}

SettingLookupResult ClientContext::TryGetCurrentUserSetting(idx_t setting_index, Value &result) const {
	auto &db_config = DBConfig::GetConfig(*this);
	return config.user_settings.TryGetSetting(db_config.user_settings, setting_index, result);
}

ParserOptions ClientContext::GetParserOptions() const {
	ParserOptions options;
	options.preserve_identifier_case = Settings::Get<PreserveIdentifierCaseSetting>(*this);
	options.integer_division = Settings::Get<IntegerDivisionSetting>(*this);
	options.max_expression_depth = Settings::Get<MaxExpressionDepthSetting>(*this);
	options.extensions = DBConfig::GetConfig(*this).GetCallbackManager();
	options.parser_override_setting = Settings::Get<AllowParserOverrideExtensionSetting>(*this);
	return options;
}

ClientProperties ClientContext::GetClientProperties() {
	string timezone = "UTC";
	Value result;

	if (TryGetCurrentSetting("TimeZone", result)) {
		timezone = result.ToString();
	}
	ArrowOffsetSize arrow_offset_size = ArrowOffsetSize::REGULAR;
	if (Settings::Get<ArrowLargeBufferSizeSetting>(*this)) {
		arrow_offset_size = ArrowOffsetSize::LARGE;
	}
	bool arrow_use_list_view = Settings::Get<ArrowOutputListViewSetting>(*this);
	bool arrow_lossless_conversion = Settings::Get<ArrowLosslessConversionSetting>(*this);
	bool arrow_use_string_view = Settings::Get<ProduceArrowStringViewSetting>(*this);
	auto arrow_format_version = Settings::Get<ArrowOutputVersionSetting>(*this);
	return {timezone,
	        arrow_offset_size,
	        arrow_use_list_view,
	        arrow_use_string_view,
	        arrow_lossless_conversion,
	        arrow_format_version,
	        this};
}

bool ClientContext::ExecutionIsFinished() {
	if (!active_query || !active_query->executor) {
		return false;
	}
	return active_query->executor->ExecutionIsFinished();
}

LogicalType ClientContext::ParseLogicalType(const string &type) {
	auto lock = LockContext();
	LogicalType logical_type;
	RunFunctionInTransactionInternal(*lock,
	                                 [&]() { logical_type = TypeManager::Get(*db).ParseLogicalType(type, *this); });
	return logical_type;
}

} // namespace duckdb
