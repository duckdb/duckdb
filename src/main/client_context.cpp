#include "main/client_context.hpp"

#include "execution/physical_plan_generator.hpp"
#include "main/database.hpp"
#include "optimizer/optimizer.hpp"
#include "parser/parser.hpp"
#include "planner/operator/logical_execute.hpp"
#include "planner/planner.hpp"

using namespace duckdb;
using namespace std;

ClientContext::ClientContext(DuckDB &database)
    : db(database), transaction(database.transaction_manager), interrupted(false),
      prepared_statements(make_unique<CatalogSet>()), open_result(nullptr) {
}

void ClientContext::Cleanup() {
	lock_guard<mutex> client_guard(context_lock);
	if (is_invalidated) {
		return;
	}
	return CleanupInternal();
}

unique_ptr<DataChunk> ClientContext::Fetch() {
	lock_guard<mutex> client_guard(context_lock);
	if (!open_result) {
		// no result to fetch from
		return nullptr;
	}
	if (is_invalidated) {
		// ClientContext is invalidated: database has been closed
		open_result->error = "Database that this connection belongs to has been closed!";
		return nullptr;
	}
	try {
		// fetch the chunk and return it
		auto chunk = FetchInternal();
		return chunk;
	} catch (Exception &ex) {
		open_result->error = ex.GetMessage();
	} catch (...) {
		open_result->error = "Unhandled exception in Fetch";
	}
	open_result->success = false;
	CleanupInternal();
	return nullptr;
}

string ClientContext::FinalizeQuery(bool success) {
	profiler.EndQuery();
	if (profiler.IsEnabled() && profiler.automatic_printing) {
		cout << profiler.ToString() << "\n";
	}

	execution_context.physical_plan = nullptr;

	string error;
	if (transaction.HasActiveTransaction()) {
		ActiveTransaction().active_query = MAXIMUM_QUERY_ID;
		try {
			if (transaction.IsAutoCommit()) {
				if (success) {
					// query was successful: commit
					transaction.Commit();
				} else {
					// query was unsuccessful: rollback
					transaction.Rollback();
				}
			}
		} catch (Exception &ex) {
			error = ex.GetMessage();
		} catch (...) {
			error = "Unhandled exception!";
		}
	}
	return error;
}

void ClientContext::CleanupInternal() {
	if (!open_result) {
		// no result currently open
		return;
	}

	auto error = FinalizeQuery(open_result->success);
	if (open_result->success) {
		// if an error occurred while committing report it in the result
		open_result->error = error;
		open_result->success = error.empty();
	}

	open_result->is_open = false;
	open_result = nullptr;
}

unique_ptr<DataChunk> ClientContext::FetchInternal() {
	assert(execution_context.physical_plan);
	auto chunk = make_unique<DataChunk>();
	// run the plan to get the next chunks
	execution_context.physical_plan->InitializeChunk(*chunk);
	execution_context.physical_plan->GetChunk(*this, *chunk, execution_context.physical_state.get());
	return chunk;
}

unique_ptr<QueryResult> ClientContext::ExecuteStatementInternal(string query, unique_ptr<SQLStatement> statement,
                                                                bool allow_stream_result) {
	bool create_stream_result = statement->type == StatementType::SELECT && allow_stream_result;
	// for many statements, we log the literal query string in the WAL
	// also note the exception for EXECUTE below
	bool log_query_string = false;
	switch (statement->type) {
	case StatementType::UPDATE:
	case StatementType::DELETE:
	case StatementType::ALTER:
	case StatementType::CREATE_INDEX:
	case StatementType::PREPARE:
	case StatementType::DEALLOCATE:
		log_query_string = true;
		break;
	default:
		break;
	}

	Planner planner(*this);
	planner.CreatePlan(move(statement));
	if (!planner.plan) {
		// we have to log here because some queries are executed in the planner
		if (log_query_string) {
			ActiveTransaction().PushQuery(query);
		}
		// return an empty result
		return make_unique<MaterializedQueryResult>();
	}

	auto plan = move(planner.plan);
	// extract the result column names from the plan
	auto names = plan->GetNames();
#ifdef DEBUG
	if (enable_optimizer) {
#endif
		Optimizer optimizer(planner.binder, *this);
		plan = optimizer.Optimize(move(plan));
		assert(plan);
#ifdef DEBUG
	}
#endif

	// special case with logging EXECUTE with prepared statements that do not scan the table
	if (plan->type == LogicalOperatorType::EXECUTE) {
		auto exec = (LogicalExecute *)plan.get();
		if (exec->prep->statement_type == StatementType::UPDATE ||
		    exec->prep->statement_type == StatementType::DELETE) {
			log_query_string = true;
		}
	}

	// now convert logical query plan into a physical query plan
	PhysicalPlanGenerator physical_planner(*this);
	physical_planner.CreatePlan(move(plan));
	assert(physical_planner.plan);

	// store the physical plan in the context for calls to Fetch()
	execution_context.physical_plan = move(physical_planner.plan);
	execution_context.physical_state = execution_context.physical_plan->GetOperatorState();

	auto types = execution_context.physical_plan->GetTypes();

	if (create_stream_result) {
		// successfully compiled SELECT clause and it is the last statement
		// return a StreamQueryResult so the client can call Fetch() on it and stream the result
		assert(!log_query_string);
		return make_unique<StreamQueryResult>(*this, types, names);
	}
	// check if we need to log the query string
	if (log_query_string) {
		ActiveTransaction().PushQuery(query);
	}
	// create a materialized result by continuously fetching
	auto result = make_unique<MaterializedQueryResult>(types, names);
	while (true) {
		auto chunk = FetchInternal();
		if (chunk->size() == 0) {
			break;
		}
		result->collection.Append(*chunk);
	}
	return move(result);
}

unique_ptr<QueryResult> ClientContext::Query(string query, bool allow_stream_result) {
	lock_guard<mutex> client_guard(context_lock);
	if (is_invalidated) {
		return make_unique<MaterializedQueryResult>("Database that this connection belongs to has been closed!");
	}

	//! Cleanup any open results and reset the interrupted flag
	CleanupInternal();
	interrupted = false;

	// now start by parsing the query
	Parser parser(*this);
	try {
		// parse the query and transform it into a set of statements
		parser.ParseQuery(query.c_str());
	} catch (Exception &ex) {
		return make_unique<MaterializedQueryResult>(ex.GetMessage());
	} catch (...) {
		return make_unique<MaterializedQueryResult>("Unhandled exception thrown in parser");
	}

	if (parser.statements.size() == 0) {
		// no statements, return empty successful result
		return make_unique<MaterializedQueryResult>();
	}

	// now we have a list of statements
	// iterate over them and execute them one by one
	unique_ptr<QueryResult> result, current_result;
	QueryResult *last_result = nullptr;
	for (size_t i = 0; i < parser.statements.size(); i++) {
		auto &statement = parser.statements[i];
		bool is_last_statement = i + 1 == parser.statements.size();
		// check if we are on AutoCommit. In this case we should start a transaction.
		if (transaction.IsAutoCommit()) {
			transaction.BeginTransaction();
		}
		ActiveTransaction().active_query = db.transaction_manager.GetQueryNumber();
		// start the profiler
		profiler.StartQuery(query);
		try {
			// run the actual query
			current_result = ExecuteStatementInternal(query, move(statement), allow_stream_result && is_last_statement);
			// only the last result can be STREAM_RESULT
			assert(is_last_statement || current_result->type != QueryResultType::STREAM_RESULT);
		} catch (Exception &ex) {
			current_result = make_unique<MaterializedQueryResult>(ex.GetMessage());
		} catch (...) {
			current_result = make_unique<MaterializedQueryResult>("Unhandled exception thrown in query execution");
		}
		if (!current_result->success) {
			// initial failures should always be reported as MaterializedResult
			assert(current_result->type != QueryResultType::STREAM_RESULT);
			// query failed: abort now
			FinalizeQuery(false);
			return current_result;
		}
		// query succeeded, append to list of results
		if (current_result->type == QueryResultType::STREAM_RESULT) {
			// store as currently open result if it is a stream result
			open_result = (StreamQueryResult *)current_result.get();
		} else {
			// finalize the query if it is not a stream result
			FinalizeQuery(true);
		}
		// now append the result to the list of results
		if (!last_result) {
			// first result of the query
			result = move(current_result);
			last_result = result.get();
		} else {
			// later results; attach to the result chain
			last_result->next = move(current_result);
			last_result = last_result->next.get();
		}
	}
	return result;
}

void ClientContext::Interrupt() {
	interrupted = true;
}

void ClientContext::EnableProfiling() {
	lock_guard<mutex> client_guard(context_lock);
	profiler.Enable();
}

void ClientContext::DisableProfiling() {
	lock_guard<mutex> client_guard(context_lock);
	profiler.Disable();
}

void ClientContext::Invalidate() {
	Interrupt();
	lock_guard<mutex> client_guard(context_lock);
	is_invalidated = true;
	transaction.Invalidate();
	if (open_result) {
		open_result->is_open = false;
	}
}
