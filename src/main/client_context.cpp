#include "duckdb/main/client_context.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/parser/statement/relation_statement.hpp"

using namespace duckdb;
using namespace std;

ClientContext::ClientContext(DuckDB &database)
    : db(database), transaction(*database.transaction_manager), interrupted(false), catalog(*database.catalog),
      temporary_objects(make_unique<SchemaCatalogEntry>(db.catalog.get(), TEMP_SCHEMA)),
      prepared_statements(make_unique<CatalogSet>(*db.catalog)), open_result(nullptr) {
	random_device rd;
	random_engine.seed(rd());
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
	for (auto &appender : appenders) {
		appender->Invalidate("Connection has been closed!", false);
	}
	CleanupInternal();
}

void ClientContext::RegisterAppender(Appender *appender) {
	lock_guard<mutex> client_guard(context_lock);
	if (is_invalidated) {
		throw Exception("Database that this connection belongs to has been closed!");
	}
	appenders.insert(appender);
}

void ClientContext::RemoveAppender(Appender *appender) {
	lock_guard<mutex> client_guard(context_lock);
	if (is_invalidated) {
		return;
	}
	appenders.erase(appender);
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
		open_result->success = false;
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

	execution_context.Reset();

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

unique_ptr<PreparedStatementData> ClientContext::CreatePreparedStatement(const string &query,
                                                                         unique_ptr<SQLStatement> statement) {
	StatementType statement_type = statement->type;
	auto result = make_unique<PreparedStatementData>(statement_type);

	profiler.StartPhase("planner");
	Planner planner(*this);
	planner.CreatePlan(move(statement));
	assert(planner.plan);
	profiler.EndPhase();

	auto plan = move(planner.plan);
	// extract the result column names from the plan
	result->read_only = planner.read_only;
	result->requires_valid_transaction = planner.requires_valid_transaction;
	result->names = planner.names;
	result->sql_types = planner.sql_types;
	result->value_map = move(planner.value_map);

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

	profiler.StartPhase("physical_planner");
	// now convert logical query plan into a physical query plan
	PhysicalPlanGenerator physical_planner(*this);
	auto physical_plan = physical_planner.CreatePlan(move(plan));
	profiler.EndPhase();

	result->dependencies = move(physical_planner.dependencies);
	result->types = physical_plan->types;
	result->plan = move(physical_plan);
	return result;
}

unique_ptr<QueryResult> ClientContext::ExecutePreparedStatement(const string &query, PreparedStatementData &statement,
                                                                vector<Value> bound_values, bool allow_stream_result) {
	if (ActiveTransaction().is_invalidated && statement.requires_valid_transaction) {
		throw Exception("Current transaction is aborted (please ROLLBACK)");
	}
	if (db.access_mode == AccessMode::READ_ONLY && !statement.read_only) {
		throw Exception(StringUtil::Format("Cannot execute statement of type \"%s\" in read-only mode!",
		                                   StatementTypeToString(statement.statement_type).c_str()));
	}

	// bind the bound values before execution
	statement.Bind(move(bound_values));

	bool create_stream_result = statement.statement_type == StatementType::SELECT_STATEMENT && allow_stream_result;

	// store the physical plan in the context for calls to Fetch()
	execution_context.physical_plan = move(statement.plan);
	execution_context.physical_state = execution_context.physical_plan->GetOperatorState();

	auto types = execution_context.physical_plan->GetTypes();
	assert(types.size() == statement.sql_types.size());

	if (create_stream_result) {
		// successfully compiled SELECT clause and it is the last statement
		// return a StreamQueryResult so the client can call Fetch() on it and stream the result
		return make_unique<StreamQueryResult>(statement.statement_type, *this, statement.sql_types, types,
		                                      statement.names);
	}
	// create a materialized result by continuously fetching
	auto result =
	    make_unique<MaterializedQueryResult>(statement.statement_type, statement.sql_types, types, statement.names);
	while (true) {
		auto chunk = FetchInternal();
		if (chunk->size() == 0) {
			break;
		}
#ifdef DEBUG
		for (idx_t i = 0; i < chunk->column_count(); i++) {
			if (statement.sql_types[i].id == SQLTypeId::VARCHAR) {
				chunk->data[i].UTFVerify(chunk->size());
			}
		}
#endif
		result->collection.Append(*chunk);
	}
	return move(result);
}

void ClientContext::InitialCleanup() {
	if (is_invalidated) {
		throw Exception("Database that this connection belongs to has been closed!");
	}
	//! Cleanup any open results and reset the interrupted flag
	CleanupInternal();
	interrupted = false;
}

unique_ptr<PreparedStatement> ClientContext::Prepare(string query) {
	lock_guard<mutex> client_guard(context_lock);
	// prepare the query
	try {
		InitialCleanup();

		// first parse the query
		Parser parser;
		parser.ParseQuery(query.c_str());
		if (parser.statements.size() == 0) {
			throw Exception("No statement to prepare!");
		}
		if (parser.statements.size() > 1) {
			throw Exception("Cannot prepare multiple statements at once!");
		}
		// now write the prepared statement data into the catalog
		string prepare_name = "____duckdb_internal_prepare_" + to_string(prepare_count);
		prepare_count++;
		// create a prepare statement out of the underlying statement
		auto prepare = make_unique<PrepareStatement>();
		prepare->name = prepare_name;
		prepare->statement = move(parser.statements[0]);

		// now perform the actual PREPARE query
		auto result = RunStatement(query, move(prepare), false);
		if (!result->success) {
			throw Exception(result->error);
		}
		auto prepared_catalog = (PreparedStatementCatalogEntry *)prepared_statements->GetRootEntry(prepare_name);
		auto prepared_object = make_unique<PreparedStatement>(this, prepare_name, query, *prepared_catalog->prepared,
		                                                      parser.n_prepared_parameters);
		prepared_statement_objects.insert(prepared_object.get());
		return prepared_object;
	} catch (Exception &ex) {
		return make_unique<PreparedStatement>(ex.what());
	}
}

unique_ptr<QueryResult> ClientContext::Execute(string name, vector<Value> &values, bool allow_stream_result,
                                               string query) {
	lock_guard<mutex> client_guard(context_lock);
	try {
		InitialCleanup();
	} catch (std::exception &ex) {
		return make_unique<MaterializedQueryResult>(ex.what());
	}

	// create the execute statement
	auto execute = make_unique<ExecuteStatement>();
	execute->name = name;
	for (auto &val : values) {
		execute->values.push_back(make_unique<ConstantExpression>(val.GetSQLType(), val));
	}

	return RunStatement(query, move(execute), allow_stream_result);
}
void ClientContext::RemovePreparedStatement(PreparedStatement *statement) {
	lock_guard<mutex> client_guard(context_lock);
	if (!statement->success || statement->is_invalidated || is_invalidated) {
		return;
	}
	try {
		InitialCleanup();
	} catch (...) {
		return;
	}
	// erase the object from the list of prepared statements
	prepared_statement_objects.erase(statement);
	// drop it from the catalog
	auto deallocate_statement = make_unique<DropStatement>();
	deallocate_statement->info->type = CatalogType::PREPARED_STATEMENT;
	deallocate_statement->info->name = statement->name;
	string query = "DEALLOCATE " + statement->name;
	RunStatement(query, move(deallocate_statement), false);
}

unique_ptr<QueryResult> ClientContext::RunStatementInternal(const string &query, unique_ptr<SQLStatement> statement,
                                                            bool allow_stream_result) {
	// prepare the query for execution
	auto prepared = CreatePreparedStatement(query, move(statement));
	// by default, no values are bound
	vector<Value> bound_values;
	// execute the prepared statement
	return ExecutePreparedStatement(query, *prepared, move(bound_values), allow_stream_result);
}

unique_ptr<QueryResult> ClientContext::RunStatement(const string &query, unique_ptr<SQLStatement> statement,
                                                    bool allow_stream_result) {
	unique_ptr<QueryResult> result;
	// check if we are on AutoCommit. In this case we should start a transaction.
	if (transaction.IsAutoCommit()) {
		transaction.BeginTransaction();
	}
	ActiveTransaction().active_query = db.transaction_manager->GetQueryNumber();
	if (statement->type == StatementType::SELECT_STATEMENT && query_verification_enabled) {
		// query verification is enabled:
		// create a copy of the statement and verify the original statement
		auto copied_statement = ((SelectStatement &)*statement).Copy();
		string error = VerifyQuery(query, move(statement));
		if (!error.empty()) {
			// query failed: abort now
			FinalizeQuery(false);
			// error in verifying query
			return make_unique<MaterializedQueryResult>(error);
		}
		statement = move(copied_statement);
	}
	// start the profiler
	profiler.StartQuery(query, *statement);
	try {
		result = RunStatementInternal(query, move(statement), allow_stream_result);
	} catch (StandardException &ex) {
		// standard exceptions do not invalidate the current transaction
		result = make_unique<MaterializedQueryResult>(ex.what());
	} catch (std::exception &ex) {
		// other types of exceptions do invalidate the current transaction
		if (transaction.HasActiveTransaction()) {
			ActiveTransaction().is_invalidated = true;
		}
		result = make_unique<MaterializedQueryResult>(ex.what());
	}
	if (!result->success) {
		// initial failures should always be reported as MaterializedResult
		assert(result->type != QueryResultType::STREAM_RESULT);
		// query failed: abort now
		FinalizeQuery(false);
		return result;
	}
	// query succeeded, append to list of results
	if (result->type == QueryResultType::STREAM_RESULT) {
		// store as currently open result if it is a stream result
		this->open_result = (StreamQueryResult *)result.get();
	} else {
		// finalize the query if it is not a stream result
		string error = FinalizeQuery(true);
		if (!error.empty()) {
			// failure in committing transaction
			return make_unique<MaterializedQueryResult>(error);
		}
	}
	return result;
}

unique_ptr<QueryResult> ClientContext::RunStatements(const string &query, vector<unique_ptr<SQLStatement>> &statements,
                                                     bool allow_stream_result) {
	// now we have a list of statements
	// iterate over them and execute them one by one
	unique_ptr<QueryResult> result;
	QueryResult *last_result = nullptr;
	for (idx_t i = 0; i < statements.size(); i++) {
		auto &statement = statements[i];
		bool is_last_statement = i + 1 == statements.size();
		auto current_result = RunStatement(query, move(statement), allow_stream_result && is_last_statement);
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

	Parser parser;
	try {
		InitialCleanup();
		// parse the query and transform it into a set of statements
		parser.ParseQuery(query.c_str());
	} catch (std::exception &ex) {
		return make_unique<MaterializedQueryResult>(ex.what());
	}

	if (parser.statements.size() == 0) {
		// no statements, return empty successful result
		return make_unique<MaterializedQueryResult>(StatementType::INVALID_STATEMENT);
	}

	return RunStatements(query, parser.statements, allow_stream_result);
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
	// and close any open appenders and prepared statements
	for (auto &statement : prepared_statement_objects) {
		statement->is_invalidated = true;
	}
	for (auto &appender : appenders) {
		appender->Invalidate("Database that this appender belongs to has been closed!", false);
	}
	appenders.clear();
}

string ClientContext::VerifyQuery(string query, unique_ptr<SQLStatement> statement) {
	assert(statement->type == StatementType::SELECT_STATEMENT);
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
#ifdef DEBUG
	auto &orig_expr_list = select_stmt->node->GetSelectList();
	auto &de_expr_list = deserialized_stmt->node->GetSelectList();
	auto &cp_expr_list = copied_stmt->node->GetSelectList();
	assert(orig_expr_list.size() == de_expr_list.size() && cp_expr_list.size() == de_expr_list.size());
	for (idx_t i = 0; i < orig_expr_list.size(); i++) {
		// run the ToString, to verify that it doesn't crash
		orig_expr_list[i]->ToString();
		// check that the expressions are equivalent
		assert(orig_expr_list[i]->Equals(de_expr_list[i].get()));
		assert(orig_expr_list[i]->Equals(cp_expr_list[i].get()));
		assert(de_expr_list[i]->Equals(cp_expr_list[i].get()));
		// check that the hashes are equivalent too
		assert(orig_expr_list[i]->Hash() == de_expr_list[i]->Hash());
		assert(orig_expr_list[i]->Hash() == cp_expr_list[i]->Hash());
	}
	// now perform additional checking within the expressions
	for (idx_t outer_idx = 0; outer_idx < orig_expr_list.size(); outer_idx++) {
		auto hash = orig_expr_list[outer_idx]->Hash();
		for (idx_t inner_idx = 0; inner_idx < orig_expr_list.size(); inner_idx++) {
			auto hash2 = orig_expr_list[inner_idx]->Hash();
			if (hash != hash2) {
				// if the hashes are not equivalent, the expressions should not be equivalent
				assert(!orig_expr_list[outer_idx]->Equals(orig_expr_list[inner_idx].get()));
			}
		}
	}
#endif

	// disable profiling if it is enabled
	bool profiling_is_enabled = profiler.IsEnabled();
	if (profiling_is_enabled) {
		profiler.Disable();
	}

	// see below
	auto statement_copy_for_explain = select_stmt->Copy();

	auto original_result = make_unique<MaterializedQueryResult>(StatementType::SELECT_STATEMENT),
	     copied_result = make_unique<MaterializedQueryResult>(StatementType::SELECT_STATEMENT),
	     deserialized_result = make_unique<MaterializedQueryResult>(StatementType::SELECT_STATEMENT),
	     unoptimized_result = make_unique<MaterializedQueryResult>(StatementType::SELECT_STATEMENT);
	// execute the original statement
	try {
		auto result = RunStatementInternal(query, move(statement), false);
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
			RunStatementInternal(explain_q, move(explain_stmt), false);
		} catch (std::exception &ex) {
			return "EXPLAIN failed but query did not (" + string(ex.what()) + ")";
		}
	}

	// now execute the copied statement
	try {
		auto result = RunStatementInternal(query, move(copied_stmt), false);
		copied_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
	} catch (Exception &ex) {
		copied_result->error = ex.what();
	}
	// now execute the deserialized statement
	try {
		auto result = RunStatementInternal(query, move(deserialized_stmt), false);
		deserialized_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
	} catch (Exception &ex) {
		deserialized_result->error = ex.what();
	}
	// now execute the unoptimized statement
	enable_optimizer = false;
	try {
		auto result = RunStatementInternal(query, move(unoptimized_stmt), false);
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

template <class T> void ClientContext::RunFunctionInTransaction(T &&fun) {
	lock_guard<mutex> client_guard(context_lock);
	if (is_invalidated) {
		throw Exception("Failed: database has been closed!");
	}
	if (transaction.HasActiveTransaction() && transaction.ActiveTransaction().is_invalidated) {
		throw Exception("Failed: transaction has been invalidated!");
	}
	// check if we are on AutoCommit. In this case we should start a transaction
	if (transaction.IsAutoCommit()) {
		transaction.BeginTransaction();
	}
	try {
		fun();
	} catch (Exception &ex) {
		if (transaction.IsAutoCommit()) {
			transaction.Rollback();
		} else {
			transaction.Invalidate();
		}
		throw ex;
	}
	if (transaction.IsAutoCommit()) {
		transaction.Commit();
	}
}

unique_ptr<TableDescription> ClientContext::TableInfo(string schema_name, string table_name) {
	unique_ptr<TableDescription> result;
	RunFunctionInTransaction([&]() {
		// obtain the table info
		auto table = db.catalog->GetEntry<TableCatalogEntry>(*this, schema_name, table_name, true);
		if (!table) {
			return;
		}
		// write the table info to the result
		result = make_unique<TableDescription>();
		result->schema = schema_name;
		result->table = table_name;
		for (auto &column : table->columns) {
			result->columns.push_back(ColumnDefinition(column.name, column.type));
		}
	});
	return result;
}

void ClientContext::Append(TableDescription &description, DataChunk &chunk) {
	RunFunctionInTransaction([&]() {
		auto table_entry = db.catalog->GetEntry<TableCatalogEntry>(*this, description.schema, description.table);
		// verify that the table columns and types match up
		if (description.columns.size() != table_entry->columns.size()) {
			throw Exception("Failed to append: table entry has different number of columns!");
		}
		for (idx_t i = 0; i < description.columns.size(); i++) {
			if (description.columns[i].type != table_entry->columns[i].type) {
				throw Exception("Failed to append: table entry has different number of columns!");
			}
		}
		table_entry->storage->Append(*table_entry, *this, chunk);
	});
}

void ClientContext::TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns) {
	RunFunctionInTransaction([&]() {
		// bind the expressions
		Binder binder(*this);
		auto result = relation.Bind(binder);
		assert(result.names.size() == result.types.size());
		for (idx_t i = 0; i < result.names.size(); i++) {
			result_columns.push_back(ColumnDefinition(result.names[i], result.types[i]));
		}
	});
}

unique_ptr<QueryResult> ClientContext::Execute(shared_ptr<Relation> relation) {
	string query;
	if (query_verification_enabled) {
		// run the ToString method of any relation we run, mostly to ensure it doesn't crash
		relation->ToString();
		if (relation->IsReadOnly()) {
			// verify read only statements by running a select statement
			auto select = make_unique<SelectStatement>();
			select->node = relation->GetQueryNode();
			RunStatement(query, move(select), false);
		}
	}
	auto &expected_columns = relation->Columns();
	auto relation_stmt = make_unique<RelationStatement>(relation);
	auto result = RunStatement(query, move(relation_stmt), false);
	// verify that the result types and result names of the query match the expected result types/names
	if (result->types.size() == expected_columns.size()) {
		bool mismatch = false;
		for (idx_t i = 0; i < result->types.size(); i++) {
			if (result->sql_types[i] != expected_columns[i].type || result->names[i] != expected_columns[i].name) {
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
	string err_str = "Result mismatch in query!\nExpected the following columns: ";
	for (idx_t i = 0; i < expected_columns.size(); i++) {
		err_str += i == 0 ? "[" : ", ";
		err_str += expected_columns[i].name + " " + SQLTypeToString(expected_columns[i].type);
	}
	err_str += "]\nBut result contained the following: ";
	for (idx_t i = 0; i < result->types.size(); i++) {
		err_str += i == 0 ? "[" : ", ";
		err_str += result->names[i] + " " + SQLTypeToString(result->sql_types[i]);
	}
	err_str += "]";
	return make_unique<MaterializedQueryResult>(err_str);
}
