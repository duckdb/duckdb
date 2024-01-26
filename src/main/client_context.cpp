#include "duckdb/main/client_context.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/http_state.hpp"
#include "duckdb/common/preserved_error.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
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
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/statement/relation_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/pragma_handler.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

struct ActiveQueryContext {
	//! The query that is currently being executed
	string query;
	//! The currently open result
	BaseQueryResult *open_result = nullptr;
	//! Prepared statement data
	shared_ptr<PreparedStatementData> prepared;
	//! The query executor
	unique_ptr<Executor> executor;
	//! The progress bar
	unique_ptr<ProgressBar> progress_bar;
};

ClientContext::ClientContext(shared_ptr<DatabaseInstance> database)
    : db(std::move(database)), interrupted(false), client_data(make_uniq<ClientData>(*this)), transaction(*this) {
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
			transaction.Rollback();
		}
	}
	CleanupInternal(*lock);
}

unique_ptr<DataChunk> ClientContext::Fetch(ClientContextLock &lock, StreamQueryResult &result) {
	D_ASSERT(IsActiveResult(lock, &result));
	D_ASSERT(active_query->executor);
	return FetchInternal(lock, *active_query->executor, result);
}

unique_ptr<DataChunk> ClientContext::FetchInternal(ClientContextLock &lock, Executor &executor,
                                                   BaseQueryResult &result) {
	bool invalidate_query = true;
	try {
		// fetch the chunk and return it
		auto chunk = executor.FetchChunk();
		if (!chunk || chunk->size() == 0) {
			CleanupInternal(lock, &result);
		}
		return chunk;
	} catch (StandardException &ex) {
		// standard exceptions do not invalidate the current transaction
		result.SetError(PreservedError(ex));
		invalidate_query = false;
	} catch (FatalException &ex) {
		// fatal exceptions invalidate the entire database
		result.SetError(PreservedError(ex));
		auto &db_inst = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db_inst, ex.what());
	} catch (const Exception &ex) {
		result.SetError(PreservedError(ex));
	} catch (std::exception &ex) {
		result.SetError(PreservedError(ex));
	} catch (...) { // LCOV_EXCL_START
		result.SetError(PreservedError("Unhandled exception in FetchInternal"));
	} // LCOV_EXCL_STOP
	CleanupInternal(lock, &result, invalidate_query);
	return nullptr;
}

void ClientContext::BeginTransactionInternal(ClientContextLock &lock, bool requires_valid_transaction) {
	// check if we are on AutoCommit. In this case we should start a transaction
	D_ASSERT(!active_query);
	auto &db_inst = DatabaseInstance::GetDatabase(*this);
	if (ValidChecker::IsInvalidated(db_inst)) {
		throw FatalException(ErrorManager::FormatException(*this, ErrorType::INVALIDATED_DATABASE,
		                                                   ValidChecker::InvalidatedMessage(db_inst)));
	}
	if (requires_valid_transaction && transaction.HasActiveTransaction() &&
	    ValidChecker::IsInvalidated(transaction.ActiveTransaction())) {
		throw Exception(ErrorManager::FormatException(*this, ErrorType::INVALIDATED_TRANSACTION));
	}
	active_query = make_uniq<ActiveQueryContext>();
	if (transaction.IsAutoCommit()) {
		transaction.BeginTransaction();
	}
}

void ClientContext::BeginQueryInternal(ClientContextLock &lock, const string &query) {
	BeginTransactionInternal(lock, false);
	LogQueryInternal(lock, query);
	active_query->query = query;

	query_progress.Initialize();
	transaction.SetActiveQuery(db->GetDatabaseManager().GetNewQueryNumber());
}

PreservedError ClientContext::EndQueryInternal(ClientContextLock &lock, bool success, bool invalidate_transaction) {
	client_data->profiler->EndQuery();

	if (client_data->http_state) {
		client_data->http_state->Reset();
	}

	// Notify any registered state of query end
	for (auto const &s : registered_state) {
		s.second->QueryEnd();
	}

	D_ASSERT(active_query.get());
	active_query.reset();
	query_progress.Initialize();
	PreservedError error;
	try {
		if (transaction.HasActiveTransaction()) {
			// Move the query profiler into the history
			auto &prev_profilers = client_data->query_profiler_history->GetPrevProfilers();
			prev_profilers.emplace_back(transaction.GetActiveQuery(), std::move(client_data->profiler));
			// Reinitialize the query profiler
			client_data->profiler = make_shared<QueryProfiler>(*this);
			// Propagate settings of the saved query into the new profiler.
			client_data->profiler->Propagate(*prev_profilers.back().second);
			if (prev_profilers.size() >= client_data->query_profiler_history->GetPrevProfilersSize()) {
				prev_profilers.pop_front();
			}

			transaction.ResetActiveQuery();
			if (transaction.IsAutoCommit()) {
				if (success) {
					transaction.Commit();
				} else {
					transaction.Rollback();
				}
			} else if (invalidate_transaction) {
				D_ASSERT(!success);
				ValidChecker::Invalidate(ActiveTransaction(), "Failed to commit");
			}
		}
	} catch (FatalException &ex) {
		auto &db_inst = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db_inst, ex.what());
		error = PreservedError(ex);
	} catch (const Exception &ex) {
		error = PreservedError(ex);
	} catch (std::exception &ex) {
		error = PreservedError(ex);
	} catch (...) { // LCOV_EXCL_START
		error = PreservedError("Unhandled exception!");
	} // LCOV_EXCL_STOP
	return error;
}

void ClientContext::CleanupInternal(ClientContextLock &lock, BaseQueryResult *result, bool invalidate_transaction) {
	client_data->http_state = make_shared<HTTPState>();
	if (!active_query) {
		// no query currently active
		return;
	}
	if (active_query->executor) {
		active_query->executor->CancelTasks();
	}
	active_query->progress_bar.reset();

	auto error = EndQueryInternal(lock, result ? !result->HasError() : false, invalidate_transaction);
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

const string &ClientContext::GetCurrentQuery() {
	D_ASSERT(active_query);
	return active_query->query;
}

unique_ptr<QueryResult> ClientContext::FetchResultInternal(ClientContextLock &lock, PendingQueryResult &pending) {
	D_ASSERT(active_query);
	D_ASSERT(active_query->open_result == &pending);
	D_ASSERT(active_query->prepared);
	auto &executor = GetExecutor();
	auto &prepared = *active_query->prepared;
	bool create_stream_result = prepared.properties.allow_stream_result && pending.allow_stream_result;
	if (create_stream_result) {
		D_ASSERT(!executor.HasResultCollector());
		active_query->progress_bar.reset();
		query_progress.Initialize();
		// successfully compiled SELECT clause, and it is the last statement
		// return a StreamQueryResult so the client can call Fetch() on it and stream the result
		auto stream_result = make_uniq<StreamQueryResult>(pending.statement_type, pending.properties,
		                                                  shared_from_this(), pending.types, pending.names);
		active_query->open_result = stream_result.get();
		return std::move(stream_result);
	}
	unique_ptr<QueryResult> result;
	if (executor.HasResultCollector()) {
		// we have a result collector - fetch the result directly from the result collector
		result = executor.GetResult();
		CleanupInternal(lock, result.get(), false);
	} else {
		// no result collector - create a materialized result by continuously fetching
		auto result_collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), pending.types);
		D_ASSERT(!result_collection->Types().empty());
		auto materialized_result =
		    make_uniq<MaterializedQueryResult>(pending.statement_type, pending.properties, pending.names,
		                                       std::move(result_collection), GetClientProperties());

		auto &collection = materialized_result->Collection();
		D_ASSERT(!collection.Types().empty());
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		while (true) {
			auto chunk = FetchInternal(lock, GetExecutor(), *materialized_result);
			if (!chunk || chunk->size() == 0) {
				break;
			}
#ifdef DEBUG
			for (idx_t i = 0; i < chunk->ColumnCount(); i++) {
				if (pending.types[i].id() == LogicalTypeId::VARCHAR) {
					chunk->data[i].UTFVerify(chunk->size());
				}
			}
#endif
			collection.Append(append_state, *chunk);
		}
		result = std::move(materialized_result);
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

shared_ptr<PreparedStatementData>
ClientContext::CreatePreparedStatement(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
                                       optional_ptr<case_insensitive_map_t<Value>> values) {
	StatementType statement_type = statement->type;
	auto result = make_shared<PreparedStatementData>(statement_type);

	auto &profiler = QueryProfiler::Get(*this);
	profiler.StartQuery(query, IsExplainAnalyze(statement.get()), true);
	profiler.StartPhase("planner");
	Planner planner(*this);
	if (values) {
		auto &parameter_values = *values;
		for (auto &value : parameter_values) {
			planner.parameter_data.emplace(value.first, BoundParameterData(value.second));
		}
	}

	client_data->http_state = make_shared<HTTPState>();
	planner.CreatePlan(std::move(statement));
	D_ASSERT(planner.plan || !planner.properties.bound_all_parameters);
	profiler.EndPhase();

	auto plan = std::move(planner.plan);
	// extract the result column names from the plan
	result->properties = planner.properties;
	result->names = planner.names;
	result->types = planner.types;
	result->value_map = std::move(planner.value_map);
	result->catalog_version = MetaTransaction::Get(*this).catalog_version;

	if (!planner.properties.bound_all_parameters) {
		return result;
	}
#ifdef DEBUG
	plan->Verify(*this);
#endif
	if (config.enable_optimizer && plan->RequireOptimizer()) {
		profiler.StartPhase("optimizer");
		Optimizer optimizer(*planner.binder, *this);
		plan = optimizer.Optimize(std::move(plan));
		D_ASSERT(plan);
		profiler.EndPhase();

#ifdef DEBUG
		plan->Verify(*this);
#endif
	}

	profiler.StartPhase("physical_planner");
	// now convert logical query plan into a physical query plan
	PhysicalPlanGenerator physical_planner(*this);
	auto physical_plan = physical_planner.CreatePlan(std::move(plan));
	profiler.EndPhase();

#ifdef DEBUG
	D_ASSERT(!physical_plan->ToString().empty());
#endif
	result->plan = std::move(physical_plan);
	return result;
}

QueryProgress ClientContext::GetQueryProgress() {
	return query_progress;
}

unique_ptr<PendingQueryResult> ClientContext::PendingPreparedStatement(ClientContextLock &lock,
                                                                       shared_ptr<PreparedStatementData> statement_p,
                                                                       const PendingQueryParameters &parameters) {
	D_ASSERT(active_query);
	auto &statement = *statement_p;
	if (ValidChecker::IsInvalidated(ActiveTransaction()) && statement.properties.requires_valid_transaction) {
		throw Exception(ErrorManager::FormatException(*this, ErrorType::INVALIDATED_TRANSACTION));
	}
	auto &meta_transaction = MetaTransaction::Get(*this);
	auto &manager = DatabaseManager::Get(*this);
	for (auto &modified_database : statement.properties.modified_databases) {
		auto entry = manager.GetDatabase(*this, modified_database);
		if (!entry) {
			throw InternalException("Database \"%s\" not found", modified_database);
		}
		if (entry->IsReadOnly()) {
			throw Exception(StringUtil::Format(
			    "Cannot execute statement of type \"%s\" on database \"%s\" which is attached in read-only mode!",
			    StatementTypeToString(statement.statement_type), modified_database));
		}
		meta_transaction.ModifyDatabase(*entry);
	}

	// bind the bound values before execution
	case_insensitive_map_t<Value> owned_values;
	if (parameters.parameters) {
		auto &params = *parameters.parameters;
		for (auto &val : params) {
			owned_values.emplace(val);
		}
	}
	statement.Bind(std::move(owned_values));

	active_query->executor = make_uniq<Executor>(*this);
	auto &executor = *active_query->executor;
	if (config.enable_progress_bar) {
		progress_bar_display_create_func_t display_create_func = nullptr;
		if (config.print_progress_bar) {
			// If a custom display is set, use that, otherwise just use the default
			display_create_func =
			    config.display_create_func ? config.display_create_func : ProgressBar::DefaultProgressBarDisplay;
		}
		active_query->progress_bar = make_uniq<ProgressBar>(executor, config.wait_time, display_create_func);
		active_query->progress_bar->Start();
		query_progress.Restart();
	}
	auto stream_result = parameters.allow_stream_result && statement.properties.allow_stream_result;
	if (!stream_result && statement.properties.return_type == StatementReturnType::QUERY_RESULT) {
		unique_ptr<PhysicalResultCollector> collector;
		auto &client_config = ClientConfig::GetConfig(*this);
		auto get_method = client_config.result_collector ? client_config.result_collector
		                                                 : PhysicalResultCollector::GetResultCollector;
		collector = get_method(*this, statement);
		D_ASSERT(collector->type == PhysicalOperatorType::RESULT_COLLECTOR);
		executor.Initialize(std::move(collector));
	} else {
		executor.Initialize(*statement.plan);
	}
	auto types = executor.GetTypes();
	D_ASSERT(types == statement.types);
	D_ASSERT(!active_query->open_result);

	auto pending_result =
	    make_uniq<PendingQueryResult>(shared_from_this(), *statement_p, std::move(types), stream_result);
	active_query->prepared = std::move(statement_p);
	active_query->open_result = pending_result.get();
	return pending_result;
}

PendingExecutionResult ClientContext::ExecuteTaskInternal(ClientContextLock &lock, PendingQueryResult &result) {
	D_ASSERT(active_query);
	D_ASSERT(active_query->open_result == &result);
	try {
		auto query_result = active_query->executor->ExecuteTask();
		if (active_query->progress_bar) {
			active_query->progress_bar->Update(query_result == PendingExecutionResult::RESULT_READY);
			query_progress = active_query->progress_bar->GetDetailedQueryProgress();
		}
		return query_result;
	} catch (FatalException &ex) {
		// fatal exceptions invalidate the entire database
		result.SetError(PreservedError(ex));
		auto &db_instance = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db_instance, ex.what());
	} catch (const Exception &ex) {
		result.SetError(PreservedError(ex));
	} catch (std::exception &ex) {
		result.SetError(PreservedError(ex));
	} catch (...) { // LCOV_EXCL_START
		result.SetError(PreservedError("Unhandled exception in ExecuteTaskInternal"));
	} // LCOV_EXCL_STOP
	EndQueryInternal(lock, false, true);
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
	Parser parser(GetParserOptions());
	parser.ParseQuery(query);

	PragmaHandler handler(*this);
	handler.HandlePragmaStatements(lock, parser.statements);

	return std::move(parser.statements);
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
		throw Exception("ExtractPlan can only prepare a single statement");
	}

	unique_ptr<LogicalOperator> plan;
	client_data->http_state = make_shared<HTTPState>();
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
	auto n_param = statement->n_param;
	auto named_param_map = std::move(statement->named_param_map);
	auto statement_query = statement->query;
	shared_ptr<PreparedStatementData> prepared_data;
	auto unbound_statement = statement->Copy();
	RunFunctionInTransactionInternal(
	    lock, [&]() { prepared_data = CreatePreparedStatement(lock, statement_query, std::move(statement)); }, false);
	prepared_data->unbound_statement = std::move(unbound_statement);
	return make_uniq<PreparedStatement>(shared_from_this(), std::move(prepared_data), std::move(statement_query),
	                                    n_param, std::move(named_param_map));
}

unique_ptr<PreparedStatement> ClientContext::Prepare(unique_ptr<SQLStatement> statement) {
	auto lock = LockContext();
	// prepare the query
	try {
		InitialCleanup(*lock);
		return PrepareInternal(*lock, std::move(statement));
	} catch (const Exception &ex) {
		return make_uniq<PreparedStatement>(PreservedError(ex));
	} catch (std::exception &ex) {
		return make_uniq<PreparedStatement>(PreservedError(ex));
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
			throw Exception("No statement to prepare!");
		}
		if (statements.size() > 1) {
			throw Exception("Cannot prepare multiple statements at once!");
		}
		return PrepareInternal(*lock, std::move(statements[0]));
	} catch (const Exception &ex) {
		return make_uniq<PreparedStatement>(PreservedError(ex));
	} catch (std::exception &ex) {
		return make_uniq<PreparedStatement>(PreservedError(ex));
	}
}

unique_ptr<PendingQueryResult> ClientContext::PendingQueryPreparedInternal(ClientContextLock &lock, const string &query,
                                                                           shared_ptr<PreparedStatementData> &prepared,
                                                                           const PendingQueryParameters &parameters) {
	try {
		InitialCleanup(lock);
	} catch (const Exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
	} catch (std::exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
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
		return make_uniq<MaterializedQueryResult>(pending->GetErrorObject());
	}
	return pending->ExecuteInternal(*lock);
}

unique_ptr<QueryResult> ClientContext::Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
                                               case_insensitive_map_t<Value> &values, bool allow_stream_result) {
	PendingQueryParameters parameters;
	parameters.parameters = &values;
	parameters.allow_stream_result = allow_stream_result;
	return Execute(query, prepared, parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingStatementInternal(ClientContextLock &lock, const string &query,
                                                                       unique_ptr<SQLStatement> statement,
                                                                       const PendingQueryParameters &parameters) {
	// prepare the query for execution
	auto prepared = CreatePreparedStatement(lock, query, std::move(statement), parameters.parameters);
	idx_t parameter_count = !parameters.parameters ? 0 : parameters.parameters->size();
	if (prepared->properties.parameter_count > 0 && parameter_count == 0) {
		string error_message = StringUtil::Format("Expected %lld parameters, but none were supplied",
		                                          prepared->properties.parameter_count);
		return make_uniq<PendingQueryResult>(PreservedError(error_message));
	}
	if (!prepared->properties.bound_all_parameters) {
		return make_uniq<PendingQueryResult>(PreservedError("Not all parameters were bound"));
	}
	// execute the prepared statement
	return PendingPreparedStatement(lock, std::move(prepared), parameters);
}

unique_ptr<QueryResult> ClientContext::RunStatementInternal(ClientContextLock &lock, const string &query,
                                                            unique_ptr<SQLStatement> statement,
                                                            bool allow_stream_result, bool verify) {
	PendingQueryParameters parameters;
	parameters.allow_stream_result = allow_stream_result;
	auto pending = PendingQueryInternal(lock, std::move(statement), parameters, verify);
	if (pending->HasError()) {
		return make_uniq<MaterializedQueryResult>(pending->GetErrorObject());
	}
	return ExecutePendingQueryInternal(lock, *pending);
}

bool ClientContext::IsActiveResult(ClientContextLock &lock, BaseQueryResult *result) {
	if (!active_query) {
		return false;
	}
	return active_query->open_result == result;
}

unique_ptr<PendingQueryResult> ClientContext::PendingStatementOrPreparedStatementInternal(
    ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
    shared_ptr<PreparedStatementData> &prepared, const PendingQueryParameters &parameters) {
	// check if we are on AutoCommit. In this case we should start a transaction.
	if (statement && config.AnyVerification()) {
		// query verification is enabled
		// create a copy of the statement, and use the copy
		// this way we verify that the copy correctly copies all properties
		auto copied_statement = statement->Copy();
		switch (statement->type) {
		case StatementType::SELECT_STATEMENT: {
			// in case this is a select query, we verify the original statement
			PreservedError error;
			try {
				error = VerifyQuery(lock, query, std::move(statement));
			} catch (const Exception &ex) {
				error = PreservedError(ex);
			} catch (std::exception &ex) {
				error = PreservedError(ex);
			}
			if (error) {
				// error in verifying query
				return make_uniq<PendingQueryResult>(error);
			}
			statement = std::move(copied_statement);
			break;
		}
#ifndef DUCKDB_ALTERNATIVE_VERIFY
		case StatementType::COPY_STATEMENT:
		case StatementType::INSERT_STATEMENT:
		case StatementType::DELETE_STATEMENT:
		case StatementType::UPDATE_STATEMENT: {
			Parser parser;
			PreservedError error;
			try {
				parser.ParseQuery(statement->ToString());
			} catch (const Exception &ex) {
				error = PreservedError(ex);
			} catch (std::exception &ex) {
				error = PreservedError(ex);
			}
			if (error) {
				// error in verifying query
				return make_uniq<PendingQueryResult>(error);
			}
			statement = std::move(parser.statements[0]);
			break;
		}
#endif
		default:
			statement = std::move(copied_statement);
			break;
		}
	}
	return PendingStatementOrPreparedStatement(lock, query, std::move(statement), prepared, parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingStatementOrPreparedStatement(
    ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
    shared_ptr<PreparedStatementData> &prepared, const PendingQueryParameters &parameters) {
	unique_ptr<PendingQueryResult> result;

	try {
		BeginQueryInternal(lock, query);
	} catch (FatalException &ex) {
		// fatal exceptions invalidate the entire database
		auto &db_instance = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db_instance, ex.what());
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
		return result;
	} catch (const Exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
	} catch (std::exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
	}
	// start the profiler
	auto &profiler = QueryProfiler::Get(*this);
	profiler.StartQuery(query, IsExplainAnalyze(statement ? statement.get() : prepared->unbound_statement.get()));

	bool invalidate_query = true;
	try {
		if (statement) {
			result = PendingStatementInternal(lock, query, std::move(statement), parameters);
		} else {
			if (prepared->RequireRebind(*this, parameters.parameters)) {
				// catalog was modified: rebind the statement before execution
				auto new_prepared =
				    CreatePreparedStatement(lock, query, prepared->unbound_statement->Copy(), parameters.parameters);
				D_ASSERT(new_prepared->properties.bound_all_parameters);
				new_prepared->unbound_statement = std::move(prepared->unbound_statement);
				prepared = std::move(new_prepared);
				prepared->properties.bound_all_parameters = false;
			}
			result = PendingPreparedStatement(lock, prepared, parameters);
		}
	} catch (StandardException &ex) {
		// standard exceptions do not invalidate the current transaction
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
		invalidate_query = false;
	} catch (FatalException &ex) {
		// fatal exceptions invalidate the entire database
		if (!config.query_verification_enabled) {
			auto &db_instance = DatabaseInstance::GetDatabase(*this);
			ValidChecker::Invalidate(db_instance, ex.what());
		}
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
	} catch (const Exception &ex) {
		// other types of exceptions do invalidate the current transaction
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
	} catch (std::exception &ex) {
		// other types of exceptions do invalidate the current transaction
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
	}
	if (result->HasError()) {
		// query failed: abort now
		EndQueryInternal(lock, false, invalidate_query);
		return result;
	}
	D_ASSERT(active_query->open_result == result.get());
	return result;
}

void ClientContext::LogQueryInternal(ClientContextLock &, const string &query) {
	if (!client_data->log_query_writer) {
#ifdef DUCKDB_FORCE_QUERY_LOG
		try {
			string log_path(DUCKDB_FORCE_QUERY_LOG);
			client_data->log_query_writer =
			    make_uniq<BufferedFileWriter>(FileSystem::GetFileSystem(*this), log_path,
			                                  BufferedFileWriter::DEFAULT_OPEN_FLAGS, client_data->file_opener.get());
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

unique_ptr<QueryResult> ClientContext::Query(unique_ptr<SQLStatement> statement, bool allow_stream_result) {
	auto pending_query = PendingQuery(std::move(statement), allow_stream_result);
	if (pending_query->HasError()) {
		return make_uniq<MaterializedQueryResult>(pending_query->GetErrorObject());
	}
	return pending_query->Execute();
}

unique_ptr<QueryResult> ClientContext::Query(const string &query, bool allow_stream_result) {
	auto lock = LockContext();

	PreservedError error;
	vector<unique_ptr<SQLStatement>> statements;
	if (!ParseStatements(*lock, query, statements, error)) {
		return make_uniq<MaterializedQueryResult>(std::move(error));
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
	QueryResult *last_result = nullptr;
	bool last_had_result = false;
	for (idx_t i = 0; i < statements.size(); i++) {
		auto &statement = statements[i];
		bool is_last_statement = i + 1 == statements.size();
		PendingQueryParameters parameters;
		parameters.allow_stream_result = allow_stream_result && is_last_statement;
		auto pending_query = PendingQueryInternal(*lock, std::move(statement), parameters);
		auto has_result = pending_query->properties.return_type == StatementReturnType::QUERY_RESULT;
		unique_ptr<QueryResult> current_result;
		if (pending_query->HasError()) {
			current_result = make_uniq<MaterializedQueryResult>(pending_query->GetErrorObject());
		} else {
			current_result = ExecutePendingQueryInternal(*lock, *pending_query);
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
	}
	return result;
}

bool ClientContext::ParseStatements(ClientContextLock &lock, const string &query,
                                    vector<unique_ptr<SQLStatement>> &result, PreservedError &error) {
	try {
		InitialCleanup(lock);
		// parse the query and transform it into a set of statements
		result = ParseStatementsInternal(lock, query);
		return true;
	} catch (const Exception &ex) {
		error = PreservedError(ex);
		return false;
	} catch (std::exception &ex) {
		error = PreservedError(ex);
		return false;
	}
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(const string &query, bool allow_stream_result) {
	auto lock = LockContext();

	PreservedError error;
	vector<unique_ptr<SQLStatement>> statements;
	if (!ParseStatements(*lock, query, statements, error)) {
		return make_uniq<PendingQueryResult>(std::move(error));
	}
	if (statements.size() != 1) {
		return make_uniq<PendingQueryResult>(PreservedError("PendingQuery can only take a single statement"));
	}
	PendingQueryParameters parameters;
	parameters.allow_stream_result = allow_stream_result;
	return PendingQueryInternal(*lock, std::move(statements[0]), parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(unique_ptr<SQLStatement> statement,
                                                           bool allow_stream_result) {
	auto lock = LockContext();

	try {
		InitialCleanup(*lock);
	} catch (const Exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
	} catch (std::exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
	}

	PendingQueryParameters parameters;
	parameters.allow_stream_result = allow_stream_result;
	return PendingQueryInternal(*lock, std::move(statement), parameters);
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
	}
	try {
		fun();
	} catch (StandardException &ex) {
		if (require_new_transaction) {
			transaction.Rollback();
		}
		throw;
	} catch (FatalException &ex) {
		auto &db_instance = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db_instance, ex.what());
		throw;
	} catch (std::exception &ex) {
		if (require_new_transaction) {
			transaction.Rollback();
		} else {
			ValidChecker::Invalidate(ActiveTransaction(), ex.what());
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

unique_ptr<TableDescription> ClientContext::TableInfo(const string &schema_name, const string &table_name) {
	unique_ptr<TableDescription> result;
	RunFunctionInTransaction([&]() {
		// obtain the table info
		auto table = Catalog::GetEntry<TableCatalogEntry>(*this, INVALID_CATALOG, schema_name, table_name,
		                                                  OnEntryNotFound::RETURN_NULL);
		if (!table) {
			return;
		}
		// write the table info to the result
		result = make_uniq<TableDescription>();
		result->schema = schema_name;
		result->table = table_name;
		for (auto &column : table->GetColumns().Logical()) {
			result->columns.emplace_back(column.Name(), column.Type());
		}
	});
	return result;
}

void ClientContext::Append(TableDescription &description, ColumnDataCollection &collection) {
	RunFunctionInTransaction([&]() {
		auto &table_entry =
		    Catalog::GetEntry<TableCatalogEntry>(*this, INVALID_CATALOG, description.schema, description.table);
		// verify that the table columns and types match up
		if (description.columns.size() != table_entry.GetColumns().PhysicalColumnCount()) {
			throw Exception("Failed to append: table entry has different number of columns!");
		}
		for (idx_t i = 0; i < description.columns.size(); i++) {
			if (description.columns[i].Type() != table_entry.GetColumns().GetColumn(PhysicalIndex(i)).Type()) {
				throw Exception("Failed to append: table entry has different number of columns!");
			}
		}
		table_entry.GetStorage().LocalAppend(table_entry, *this, collection);
	});
}

void ClientContext::TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns) {
#ifdef DEBUG
	D_ASSERT(!relation.GetAlias().empty());
	D_ASSERT(!relation.ToString().empty());
#endif
	client_data->http_state = make_shared<HTTPState>();
	RunFunctionInTransaction([&]() {
		// bind the expressions
		auto binder = Binder::CreateBinder(*this);
		auto result = relation.Bind(*binder);
		D_ASSERT(result.names.size() == result.types.size());

		result_columns.reserve(result_columns.size() + result.names.size());
		for (idx_t i = 0; i < result.names.size(); i++) {
			result_columns.emplace_back(result.names[i], result.types[i]);
		}
	});
}

unordered_set<string> ClientContext::GetTableNames(const string &query) {
	auto lock = LockContext();

	auto statements = ParseStatementsInternal(*lock, query);
	if (statements.size() != 1) {
		throw InvalidInputException("Expected a single statement");
	}

	unordered_set<string> result;
	RunFunctionInTransactionInternal(*lock, [&]() {
		// bind the expressions
		auto binder = Binder::CreateBinder(*this);
		binder->SetBindingMode(BindingMode::EXTRACT_NAMES);
		binder->Bind(*statements[0]);
		result = binder->GetTableNames();
	});
	return result;
}

unique_ptr<PendingQueryResult> ClientContext::PendingQueryInternal(ClientContextLock &lock,
                                                                   const shared_ptr<Relation> &relation,
                                                                   bool allow_stream_result) {
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
			RunStatementInternal(lock, query, std::move(select), false);
		}
	}

	auto relation_stmt = make_uniq<RelationStatement>(relation);
	PendingQueryParameters parameters;
	parameters.allow_stream_result = allow_stream_result;
	return PendingQueryInternal(lock, std::move(relation_stmt), parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(const shared_ptr<Relation> &relation,
                                                           bool allow_stream_result) {
	auto lock = LockContext();
	return PendingQueryInternal(*lock, relation, allow_stream_result);
}

unique_ptr<QueryResult> ClientContext::Execute(const shared_ptr<Relation> &relation) {
	auto lock = LockContext();
	auto &expected_columns = relation->Columns();
	auto pending = PendingQueryInternal(*lock, relation, false);
	if (!pending->success) {
		return make_uniq<MaterializedQueryResult>(pending->GetErrorObject());
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
	return make_uniq<MaterializedQueryResult>(PreservedError(err_str));
}

bool ClientContext::TryGetCurrentSetting(const std::string &key, Value &result) {
	// first check the built-in settings
	auto &db_config = DBConfig::GetConfig(*this);
	auto option = db_config.GetOptionByName(key);
	if (option) {
		result = option->get_setting(*this);
		return true;
	}

	// check the client session values
	const auto &session_config_map = config.set_variables;

	auto session_value = session_config_map.find(key);
	bool found_session_value = session_value != session_config_map.end();
	if (found_session_value) {
		result = session_value->second;
		return true;
	}
	// finally check the global session values
	return db->TryGetCurrentSetting(key, result);
}

ParserOptions ClientContext::GetParserOptions() const {
	auto &client_config = ClientConfig::GetConfig(*this);
	ParserOptions options;
	options.preserve_identifier_case = client_config.preserve_identifier_case;
	options.integer_division = client_config.integer_division;
	options.max_expression_depth = client_config.max_expression_depth;
	options.extensions = &DBConfig::GetConfig(*this).parser_extensions;
	return options;
}

ClientProperties ClientContext::GetClientProperties() const {
	string timezone = "UTC";
	Value result;
	// 1) Check Set Variable
	auto &client_config = ClientConfig::GetConfig(*this);
	auto tz_config = client_config.set_variables.find("timezone");
	if (tz_config == client_config.set_variables.end()) {
		// 2) Check for Default Value
		auto default_value = db->config.extension_parameters.find("timezone");
		if (default_value != db->config.extension_parameters.end()) {
			timezone = default_value->second.default_value.GetValue<string>();
		}
	} else {
		timezone = tz_config->second.GetValue<string>();
	}
	return {timezone, db->config.options.arrow_offset_size};
}

bool ClientContext::ExecutionIsFinished() {
	if (!active_query || !active_query->executor) {
		return false;
	}
	return active_query->executor->ExecutionIsFinished();
}

} // namespace duckdb
