#include "main/client_context.hpp"

#include "common/serializer/buffered_deserializer.hpp"
#include "common/serializer/buffered_serializer.hpp"
#include "execution/physical_plan_generator.hpp"
#include "main/database.hpp"
#include "main/materialized_query_result.hpp"
#include "main/query_result.hpp"
#include "main/stream_query_result.hpp"
#include "optimizer/optimizer.hpp"
#include "parser/parser.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/statement/execute_statement.hpp"
#include "parser/statement/explain_statement.hpp"
#include "parser/statement/prepare_statement.hpp"
#include "planner/operator/logical_execute.hpp"
#include "planner/planner.hpp"
#include "transaction/transaction_manager.hpp"
#include "parser/statement/deallocate_statement.hpp"

using namespace duckdb;
using namespace std;

ClientContext::ClientContext(DuckDB &database)
    : db(database), transaction(*database.transaction_manager), interrupted(false), catalog(*database.catalog),
      prepared_statements(make_unique<CatalogSet>(*db.catalog)), open_result(nullptr) {
}

void ClientContext::Cleanup() {
	lock_guard<mutex> client_guard(context_lock);
	if (is_invalidated || !prepared_statements) {
		return;
	}
	if (transaction.HasActiveTransaction()) {
		ActiveTransaction().active_query = MAXIMUM_QUERY_ID;
		if (!transaction.IsAutoCommit()) {
			transaction.Rollback();
		}
	}
	assert(prepared_statements);
	db.transaction_manager->AddCatalogSet(*this, move(prepared_statements));
	// invalidate any prepared statements
	for (auto &statement : prepared_statement_objects) {
		statement->is_invalidated = true;
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
		open_result->error = ex.what();
	} catch (...) {
		open_result->error = "Unhandled exception in Fetch";
	}
	open_result->success = false;
	CleanupInternal();
	return nullptr;
}

string ClientContext::FinalizeQuery(bool success) {
	profiler.EndQuery();

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
			error = ex.what();
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
	StatementType statement_type = statement->type;
	bool create_stream_result = statement_type == StatementType::SELECT && allow_stream_result;
	// for many statements, we log the literal query string in the WAL
	// also note the exception for EXECUTE below
	bool log_query_string = false;
	switch (statement_type) {
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

	profiler.StartPhase("planner");
	Planner planner(*this);
	planner.CreatePlan(move(statement));
	if (!planner.plan) {
		// we have to log here because some queries are executed in the planner
		if (log_query_string) {
			ActiveTransaction().PushQuery(query);
		}
		// return an empty result
		return make_unique<MaterializedQueryResult>(statement_type);
	}
	profiler.EndPhase();

	auto plan = move(planner.plan);
	// extract the result column names from the plan
	auto names = planner.names;
	auto sql_types = planner.sql_types;

#ifdef DEBUG
	if (enable_optimizer) {
#endif
		profiler.StartPhase("optimizer");
		Optimizer optimizer(planner.binder, *this);
		plan = optimizer.Optimize(move(plan));
		assert(plan);
		profiler.EndPhase();
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
		statement_type = exec->prep->statement_type;
	}

	profiler.StartPhase("physical_planner");
	// now convert logical query plan into a physical query plan
	PhysicalPlanGenerator physical_planner(*this);
	auto physical_plan = physical_planner.CreatePlan(move(plan));
	profiler.EndPhase();

	// store the physical plan in the context for calls to Fetch()
	execution_context.physical_plan = move(physical_plan);
	execution_context.physical_state = execution_context.physical_plan->GetOperatorState();

	auto types = execution_context.physical_plan->GetTypes();
	assert(types.size() == sql_types.size());

	if (create_stream_result) {
		// successfully compiled SELECT clause and it is the last statement
		// return a StreamQueryResult so the client can call Fetch() on it and stream the result
		assert(!log_query_string);
		return make_unique<StreamQueryResult>(statement_type, *this, sql_types, types, names);
	}
	// check if we need to log the query string
	if (log_query_string) {
		ActiveTransaction().PushQuery(query);
	}
	// create a materialized result by continuously fetching
	auto result = make_unique<MaterializedQueryResult>(statement_type, sql_types, types, names);
	while (true) {
		auto chunk = FetchInternal();
		if (chunk->size() == 0) {
			break;
		}
		result->collection.Append(*chunk);
	}
	return move(result);
}

static string CanExecuteStatementInReadOnlyMode(SQLStatement &stmt) {
	switch (stmt.type) {
	case StatementType::INSERT:
	case StatementType::COPY:
	case StatementType::DELETE:
	case StatementType::UPDATE:
	case StatementType::CREATE_INDEX:
	case StatementType::CREATE_TABLE:
	case StatementType::CREATE_VIEW:
	case StatementType::CREATE_SCHEMA:
	case StatementType::CREATE_SEQUENCE:
	case StatementType::DROP:
	case StatementType::ALTER:
	case StatementType::TRANSACTION:
		return StringUtil::Format("Cannot execute statement of type \"%s\" in read-only mode!",
		                          StatementTypeToString(stmt.type).c_str());
	case StatementType::PREPARE: {
		// prepare statement: check the underlying statement type
		auto &prepare = (PrepareStatement &)stmt;
		return CanExecuteStatementInReadOnlyMode(*prepare.statement);
	}
	default:
		return string();
	}
}

unique_ptr<PreparedStatement> ClientContext::Prepare(string query) {
	lock_guard<mutex> client_guard(context_lock);
	// prepare the query
	try {
		if (is_invalidated) {
			throw Exception("Database that this connection belongs to has been closed!");
		}
		//! Cleanup any open results and reset the interrupted flag
		CleanupInternal();
		interrupted = false;

		// first parse the query
		Parser parser(*this);
		parser.ParseQuery(query.c_str());
		if (parser.statements.size() != 1) {
			throw Exception("Cannot prepare multiple statements at once!");
		}
		string prepare_name = "duckdb_internal_prepare_" + to_string(prepare_count);
		prepare_count++;
		// create a prepare statement out of the underlying statement
		auto prepare = make_unique<PrepareStatement>();
		prepare->name = prepare_name;
		prepare->statement = move(parser.statements[0]);
		parser.statements[0] = move(prepare);

		// now perform the actual PREPARE query
		auto result = ExecuteStatementsInternal(query, parser.statements, false);
		if (!result->success) {
			throw Exception(result->error);
		}
		auto prepared_object = make_unique<PreparedStatement>(this, prepare_name);
		prepared_statement_objects.insert(prepared_object.get());
		return prepared_object;
	} catch (Exception &ex) {
		return make_unique<PreparedStatement>(ex.what());
	}
}

unique_ptr<QueryResult> ClientContext::Execute(string name, vector<Value> &values, bool allow_stream_result) {
	lock_guard<mutex> client_guard(context_lock);
	if (is_invalidated) {
		return make_unique<MaterializedQueryResult>("Database that this connection belongs to has been closed!");
	}

	//! Cleanup any open results and reset the interrupted flag
	CleanupInternal();
	interrupted = false;

	// create the execute statement
	auto execute = make_unique<ExecuteStatement>();
	execute->name = name;
	for (auto &val : values) {
		execute->values.push_back(make_unique<ConstantExpression>(SQLTypeFromInternalType(val.type), val));
	}

	vector<unique_ptr<SQLStatement>> statements;
	statements.push_back(move(execute));

	return ExecuteStatementsInternal("", statements, allow_stream_result);
}

void ClientContext::RemovePreparedStatement(PreparedStatement *statement) {
	lock_guard<mutex> client_guard(context_lock);
	if (!statement->success || statement->is_invalidated || is_invalidated) {
		return;
	}
	// erase the object from the list of prepared statements
	prepared_statement_objects.erase(statement);
	// drop it from the catalog
	auto deallocate_statement = make_unique<DeallocateStatement>(statement->name);
	vector<unique_ptr<SQLStatement>> statements;
	statements.push_back(move(deallocate_statement));
	ExecuteStatementsInternal("", statements, false);
}

unique_ptr<QueryResult> ClientContext::ExecuteStatementsInternal(string query,
                                                                 vector<unique_ptr<SQLStatement>> &statements,
                                                                 bool allow_stream_result) {
	// now we have a list of statements
	// iterate over them and execute them one by one
	unique_ptr<QueryResult> result, current_result;
	QueryResult *last_result = nullptr;
	for (index_t i = 0; i < statements.size(); i++) {
		auto &statement = statements[i];
		if (db.access_mode == AccessMode::READ_ONLY) {
			// if the database is opened in read-only mode, check if we can execute this statement
			string error = CanExecuteStatementInReadOnlyMode(*statement);
			if (!error.empty()) {
				return make_unique<MaterializedQueryResult>(error);
			}
		}
		bool is_last_statement = i + 1 == statements.size();
		// check if we are on AutoCommit. In this case we should start a transaction.
		if (transaction.IsAutoCommit()) {
			transaction.BeginTransaction();
		}
		ActiveTransaction().active_query = db.transaction_manager->GetQueryNumber();
		if (statement->type == StatementType::SELECT && query_verification_enabled) {
			// query verification is enabled:
			// create a copy of the statement and verify the original statement
			auto copied_statement = ((SelectStatement &)*statement).Copy();
			string error = VerifyQuery(query, move(statement));
			if (!error.empty()) {
				// error in verifying query
				return make_unique<MaterializedQueryResult>(error);
			}
			statement = move(copied_statement);
		}
		// start the profiler
		profiler.StartQuery(query);
		try {
			// run the actual query
			current_result = ExecuteStatementInternal(query, move(statement), allow_stream_result && is_last_statement);
			// only the last result can be STREAM_RESULT
			assert(is_last_statement || current_result->type != QueryResultType::STREAM_RESULT);
		} catch (Exception &ex) {
			current_result = make_unique<MaterializedQueryResult>(ex.what());
		} catch (std::bad_alloc &ex) {
			current_result = make_unique<MaterializedQueryResult>("Out of memory!");
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
			string error = FinalizeQuery(true);
			if (!error.empty()) {
				// failure in committing transaction
				return make_unique<MaterializedQueryResult>(error);
			}
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
		return make_unique<MaterializedQueryResult>(ex.what());
	} catch (std::bad_alloc &ex) {
		return make_unique<MaterializedQueryResult>("Out of memory!");
	} catch (...) {
		return make_unique<MaterializedQueryResult>("Unhandled exception thrown in parser");
	}

	if (parser.statements.size() == 0) {
		// no statements, return empty successful result
		return make_unique<MaterializedQueryResult>(StatementType::INVALID);
	}

	return ExecuteStatementsInternal(query, parser.statements, allow_stream_result);
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
	// interrupt any running query before attempting to obtain the lock
	// this way we don't have to wait for the entire query to finish
	Interrupt();
	// now obtain the context lock
	lock_guard<mutex> client_guard(context_lock);
	// invalidate this context and the TransactionManager
	is_invalidated = true;
	transaction.Invalidate();
	// also close any open result
	if (open_result) {
		open_result->is_open = false;
	}
}

string ClientContext::VerifyQuery(string query, unique_ptr<SQLStatement> statement) {
	assert(statement->type == StatementType::SELECT);
	// aggressive query verification

	// the purpose of this function is to test correctness of otherwise hard to test features:
	// Copy() of statements and expressions
	// Serialize()/Deserialize() of expressions
	// Hash() of expressions
	// Equality() of statements and expressions
	// Correctness of plans both with and without optimizers

	// copy the statement
	auto select_stmt = (SelectStatement *)statement.get();
	auto copied_stmt = select_stmt->Copy();
	auto unoptimized_stmt = select_stmt->Copy();

	BufferedSerializer serializer;
	select_stmt->Serialize(serializer);
	BufferedDeserializer source(serializer);
	auto deserialized_stmt = SelectStatement::Deserialize(source);
	// all the statements should be equal
	assert(copied_stmt->Equals(statement.get()));
	assert(deserialized_stmt->Equals(statement.get()));
	assert(copied_stmt->Equals(deserialized_stmt.get()));

	// now perform checking on the expressions
	auto &orig_expr_list = select_stmt->node->GetSelectList();
	auto &de_expr_list = deserialized_stmt->node->GetSelectList();
	auto &cp_expr_list = copied_stmt->node->GetSelectList();
	assert(orig_expr_list.size() == de_expr_list.size() && cp_expr_list.size() == de_expr_list.size());
	for (index_t i = 0; i < orig_expr_list.size(); i++) {
		// check that the expressions are equivalent
		assert(orig_expr_list[i]->Equals(de_expr_list[i].get()));
		assert(orig_expr_list[i]->Equals(cp_expr_list[i].get()));
		assert(de_expr_list[i]->Equals(cp_expr_list[i].get()));
		// check that the hashes are equivalent too
		assert(orig_expr_list[i]->Hash() == de_expr_list[i]->Hash());
		assert(orig_expr_list[i]->Hash() == cp_expr_list[i]->Hash());
	}
	// now perform additional checking within the expressions
	for (index_t outer_idx = 0; outer_idx < orig_expr_list.size(); outer_idx++) {
		auto hash = orig_expr_list[outer_idx]->Hash();
		for (index_t inner_idx = 0; inner_idx < orig_expr_list.size(); inner_idx++) {
			auto hash2 = orig_expr_list[inner_idx]->Hash();
			if (hash != hash2) {
				// if the hashes are not equivalent, the expressions should not be equivalent
				assert(!orig_expr_list[outer_idx]->Equals(orig_expr_list[inner_idx].get()));
			}
		}
	}

	// disable profiling if it is enabled
	bool profiling_is_enabled = profiler.IsEnabled();
	if (profiling_is_enabled) {
		profiler.Disable();
	}

	// see below
	auto statement_copy_for_explain = select_stmt->Copy();

	auto original_result = make_unique<MaterializedQueryResult>(StatementType::SELECT),
	     copied_result = make_unique<MaterializedQueryResult>(StatementType::SELECT),
	     deserialized_result = make_unique<MaterializedQueryResult>(StatementType::SELECT),
	     unoptimized_result = make_unique<MaterializedQueryResult>(StatementType::SELECT);
	// execute the original statement
	try {
		auto result = ExecuteStatementInternal(query, move(statement), false);
		original_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
	} catch (Exception &ex) {
		original_result->error = ex.what();
		original_result->success = false;
	}

	// check explain, only if q does not already contain EXPLAIN
	if (original_result->success) {
		auto explain_q = "EXPLAIN " + query;
		auto explain_stmt = make_unique<ExplainStatement>(move(statement_copy_for_explain));
		try {
			ExecuteStatementInternal(explain_q, move(explain_stmt), false);
		} catch (Exception &ex) {
			return "EXPLAIN failed but query did not";
		}
	}

	// now execute the copied statement
	try {
		auto result = ExecuteStatementInternal(query, move(copied_stmt), false);
		copied_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
	} catch (Exception &ex) {
		copied_result->error = ex.what();
	}
	// now execute the deserialized statement
	try {
		auto result = ExecuteStatementInternal(query, move(deserialized_stmt), false);
		deserialized_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
	} catch (Exception &ex) {
		deserialized_result->error = ex.what();
	}
	// now execute the unoptimized statement
	enable_optimizer = false;
	try {
		auto result = ExecuteStatementInternal(query, move(unoptimized_stmt), false);
		unoptimized_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
	} catch (Exception &ex) {
		unoptimized_result->error = ex.what();
	}

	enable_optimizer = true;
	if (profiling_is_enabled) {
		profiler.Enable();
	}

	// now compare the results
	// the results of all four runs should be identical
	if (!original_result->collection.Equals(copied_result->collection)) {
		string result = "Copied result differs from original result!\n";
		result += "Original Result:\n" + original_result->ToString();
		result += "Copied Result\n" + copied_result->ToString();
		return result;
	}
	if (!original_result->collection.Equals(deserialized_result->collection)) {
		string result = "Deserialized result differs from original result!\n";
		result += "Original Result:\n" + original_result->ToString();
		result += "Deserialized Result\n" + deserialized_result->ToString();
		return result;
	}
	if (!original_result->collection.Equals(unoptimized_result->collection)) {
		string result = "Unoptimized result differs from original result!\n";
		result += "Original Result:\n" + original_result->ToString();
		result += "Unoptimized Result\n" + unoptimized_result->ToString();
		return result;
	}

	return "";
}
