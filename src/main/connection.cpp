#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/relation/query_relation.hpp"
#include "duckdb/main/relation/read_csv_relation.hpp"
#include "duckdb/main/relation/table_relation.hpp"
#include "duckdb/main/relation/table_function_relation.hpp"
#include "duckdb/main/relation/value_relation.hpp"
#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

Connection::Connection(DatabaseInstance &database) : context(make_shared<ClientContext>(database.shared_from_this())) {
	ConnectionManager::Get(database).AddConnection(*context);
#ifdef DEBUG
	EnableProfiling();
	context->config.emit_profiler_output = false;
#endif
}

Connection::Connection(DuckDB &database) : Connection(*database.instance) {
}

Connection::~Connection() {
	ConnectionManager::Get(*context->db).RemoveConnection(*context);
}

string Connection::GetProfilingInformation(ProfilerPrintFormat format) {
	auto &profiler = QueryProfiler::Get(*context);
	if (format == ProfilerPrintFormat::JSON) {
		return profiler.ToJSON();
	} else {
		return profiler.QueryTreeToString();
	}
}

void Connection::Interrupt() {
	context->Interrupt();
}

void Connection::EnableProfiling() {
	context->EnableProfiling();
}

void Connection::DisableProfiling() {
	context->DisableProfiling();
}

void Connection::EnableQueryVerification() {
	ClientConfig::GetConfig(*context).query_verification_enabled = true;
}

void Connection::DisableQueryVerification() {
	ClientConfig::GetConfig(*context).query_verification_enabled = false;
}

void Connection::ForceParallelism() {
	ClientConfig::GetConfig(*context).verify_parallelism = true;
}

unique_ptr<QueryResult> Connection::SendQuery(const string &query) {
	return context->Query(query, true);
}

unique_ptr<MaterializedQueryResult> Connection::Query(const string &query) {
	auto result = context->Query(query, false);
	D_ASSERT(result->type == QueryResultType::MATERIALIZED_RESULT);
	return unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
}

unique_ptr<MaterializedQueryResult> Connection::Query(unique_ptr<SQLStatement> statement) {
	auto result = context->Query(move(statement), false);
	D_ASSERT(result->type == QueryResultType::MATERIALIZED_RESULT);
	return unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
}

unique_ptr<PendingQueryResult> Connection::PendingQuery(const string &query, bool allow_stream_result) {
	return context->PendingQuery(query, allow_stream_result);
}

unique_ptr<PendingQueryResult> Connection::PendingQuery(unique_ptr<SQLStatement> statement, bool allow_stream_result) {
	return context->PendingQuery(move(statement), allow_stream_result);
}

unique_ptr<PreparedStatement> Connection::Prepare(const string &query) {
	return context->Prepare(query);
}

unique_ptr<PreparedStatement> Connection::Prepare(unique_ptr<SQLStatement> statement) {
	return context->Prepare(move(statement));
}

unique_ptr<QueryResult> Connection::QueryParamsRecursive(const string &query, vector<Value> &values) {
	auto statement = Prepare(query);
	if (!statement->success) {
		return make_unique<MaterializedQueryResult>(statement->error);
	}
	return statement->Execute(values, false);
}

unique_ptr<TableDescription> Connection::TableInfo(const string &table_name) {
	return TableInfo(DEFAULT_SCHEMA, table_name);
}

unique_ptr<TableDescription> Connection::TableInfo(const string &schema_name, const string &table_name) {
	return context->TableInfo(schema_name, table_name);
}

vector<unique_ptr<SQLStatement>> Connection::ExtractStatements(const string &query) {
	return context->ParseStatements(query);
}

unique_ptr<LogicalOperator> Connection::ExtractPlan(const string &query) {
	return context->ExtractPlan(query);
}

void Connection::Append(TableDescription &description, DataChunk &chunk) {
	ChunkCollection collection(*context);
	collection.Append(chunk);
	Append(description, collection);
}

void Connection::Append(TableDescription &description, ChunkCollection &collection) {
	context->Append(description, collection);
}

shared_ptr<Relation> Connection::Table(const string &table_name) {
	return Table(DEFAULT_SCHEMA, table_name);
}

shared_ptr<Relation> Connection::Table(const string &schema_name, const string &table_name) {
	auto table_info = TableInfo(schema_name, table_name);
	if (!table_info) {
		throw Exception("Table does not exist!");
	}
	return make_shared<TableRelation>(context, move(table_info));
}

shared_ptr<Relation> Connection::View(const string &tname) {
	return View(DEFAULT_SCHEMA, tname);
}

shared_ptr<Relation> Connection::View(const string &schema_name, const string &table_name) {
	return make_shared<ViewRelation>(context, schema_name, table_name);
}

shared_ptr<Relation> Connection::TableFunction(const string &fname) {
	vector<Value> values;
	named_parameter_map_t named_parameters;
	return TableFunction(fname, values, named_parameters);
}

shared_ptr<Relation> Connection::TableFunction(const string &fname, const vector<Value> &values,
                                               const named_parameter_map_t &named_parameters) {
	return make_shared<TableFunctionRelation>(context, fname, values, named_parameters);
}

shared_ptr<Relation> Connection::TableFunction(const string &fname, const vector<Value> &values) {
	return make_shared<TableFunctionRelation>(context, fname, values);
}

shared_ptr<Relation> Connection::Values(const vector<vector<Value>> &values) {
	vector<string> column_names;
	return Values(values, column_names);
}

shared_ptr<Relation> Connection::Values(const vector<vector<Value>> &values, const vector<string> &column_names,
                                        const string &alias) {
	return make_shared<ValueRelation>(context, values, column_names, alias);
}

shared_ptr<Relation> Connection::Values(const string &values) {
	vector<string> column_names;
	return Values(values, column_names);
}

shared_ptr<Relation> Connection::Values(const string &values, const vector<string> &column_names, const string &alias) {
	return make_shared<ValueRelation>(context, values, column_names, alias);
}

shared_ptr<Relation> Connection::ReadCSV(const string &csv_file) {
	BufferedCSVReaderOptions options;
	options.file_path = csv_file;
	options.auto_detect = true;
	BufferedCSVReader reader(*context, options);
	vector<ColumnDefinition> column_list;
	for (idx_t i = 0; i < reader.sql_types.size(); i++) {
		column_list.emplace_back(reader.col_names[i], reader.sql_types[i]);
	}
	return make_shared<ReadCSVRelation>(context, csv_file, move(column_list), true);
}

shared_ptr<Relation> Connection::ReadCSV(const string &csv_file, const vector<string> &columns) {
	// parse columns
	vector<ColumnDefinition> column_list;
	for (auto &column : columns) {
		auto col_list = Parser::ParseColumnList(column, context->GetParserOptions());
		if (col_list.size() != 1) {
			throw ParserException("Expected a single column definition");
		}
		column_list.push_back(move(col_list[0]));
	}
	return make_shared<ReadCSVRelation>(context, csv_file, move(column_list));
}

unordered_set<string> Connection::GetTableNames(const string &query) {
	return context->GetTableNames(query);
}

shared_ptr<Relation> Connection::RelationFromQuery(const string &query, const string &alias, const string &error) {
	return RelationFromQuery(QueryRelation::ParseStatement(*context, query, error), alias);
}

shared_ptr<Relation> Connection::RelationFromQuery(unique_ptr<SelectStatement> select_stmt, const string &alias) {
	return make_shared<QueryRelation>(context, move(select_stmt), alias);
}

void Connection::BeginTransaction() {
	auto result = Query("BEGIN TRANSACTION");
	if (!result->success) {
		throw Exception(result->error);
	}
}

void Connection::Commit() {
	auto result = Query("COMMIT");
	if (!result->success) {
		throw Exception(result->error);
	}
}

void Connection::Rollback() {
	auto result = Query("ROLLBACK");
	if (!result->success) {
		throw Exception(result->error);
	}
}

void Connection::SetAutoCommit(bool auto_commit) {
	context->transaction.SetAutoCommit(auto_commit);
}

bool Connection::IsAutoCommit() {
	return context->transaction.IsAutoCommit();
}
bool Connection::HasActiveTransaction() {
	return context->transaction.HasActiveTransaction();
}

} // namespace duckdb
