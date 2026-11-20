#include "duckdb/main/connection.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/main/statement_iterator.hpp"

namespace duckdb {

Connection::Connection(DatabaseInstance &database) : Connection(database, ConnectionType::USER) {
}

Connection::Connection(DatabaseInstance &database, ConnectionType connection_type)
    : context(make_shared_ptr<ClientContext>(database.shared_from_this())) {
	context->connection_type = connection_type;
	auto &connection_manager = ConnectionManager::Get(database);
	connection_manager.AssignConnectionId(*this);
	connection_manager.AddConnection(*context);
}

Connection::Connection(DuckDB &database) : Connection(*database.instance) {
}

Connection Connection::CreateInternal(DatabaseInstance &database) {
	return Connection(database, ConnectionType::INTERNAL);
}

Connection Connection::CreateInternal(ClientContext &caller) {
	auto con = CreateInternal(*caller.db);
	auto &context = *con.context;
	context.base_context = caller.base_context ? caller.base_context : caller.shared_from_this();
	context.internal_depth = caller.internal_depth + 1;
	return con;
}

Connection::Connection(Connection &&other) noexcept {
	std::swap(context, other.context);
	std::swap(connection_id, other.connection_id);
}

Connection &Connection::operator=(Connection &&other) noexcept {
	std::swap(context, other.context);
	std::swap(connection_id, other.connection_id);
	return *this;
}

Connection::~Connection() {
	if (!context) {
		return;
	}
	ConnectionManager::Get(*context->db).RemoveConnection(*context);
}

string Connection::GetProfilingInformation(const ProfilerPrintFormat &format) {
	return QueryProfiler::Get(*context).ToString(format);
}

void Connection::Interrupt() {
	context->Interrupt();
}

double Connection::GetQueryProgress() {
	return context->GetQueryProgress().GetPercentage();
}

void Connection::EnableProfiling() {
	context->EnableProfiling();
}

void Connection::DisableProfiling() {
	context->DisableProfiling();
}

void Connection::EnableQueryVerification() {
}

void Connection::DisableQueryVerification() {
}

void Connection::ForceParallelism() {
	ClientConfig::GetConfig(*context).verify_parallelism = true;
}

unique_ptr<QueryResult> Connection::Query(const string &query) {
	return context->Query(query, QueryParameters());
}

unique_ptr<QueryResult> Connection::Query(unique_ptr<SQLStatement> statement, shared_ptr<ResultFormat> format) {
	return context->Query(std::move(statement), std::move(format));
}

unique_ptr<QueryResult> Connection::Query(const string &query, shared_ptr<ResultFormat> format) {
	return context->Query(query, std::move(format));
}

unique_ptr<QueryResult> Connection::Submit(const string &query, const QueryParameters &query_parameters) {
	return context->Submit(query, query_parameters);
}

unique_ptr<QueryResult> Connection::Submit(unique_ptr<SQLStatement> statement,
                                           const QueryParameters &query_parameters) {
	return context->Submit(std::move(statement), query_parameters);
}

unique_ptr<QueryResult> Connection::Submit(const string &query, shared_ptr<ResultFormat> format) {
	return context->Submit(query, std::move(format));
}

unique_ptr<QueryResult> Connection::Submit(unique_ptr<SQLStatement> statement, shared_ptr<ResultFormat> format) {
	return context->Submit(std::move(statement), std::move(format));
}

unique_ptr<QueryResult> Connection::Submit(const string &query, identifier_map_t<BoundParameterData> &named_values,
                                           const QueryParameters &query_parameters) {
	return context->Submit(query, named_values, query_parameters);
}

unique_ptr<QueryResult> Connection::Submit(unique_ptr<SQLStatement> statement,
                                           identifier_map_t<BoundParameterData> &named_values,
                                           const QueryParameters &query_parameters) {
	return context->Submit(std::move(statement), named_values, query_parameters);
}

static identifier_map_t<BoundParameterData> ConvertParamListToMap(vector<Value> &param_list) {
	identifier_map_t<BoundParameterData> named_values;
	for (idx_t i = 0; i < param_list.size(); i++) {
		auto &val = param_list[i];
		named_values[Identifier(std::to_string(i + 1))] = BoundParameterData(val);
	}
	return named_values;
}

unique_ptr<QueryResult> Connection::Submit(const string &query, vector<Value> &values,
                                           const QueryParameters &query_parameters) {
	auto named_params = ConvertParamListToMap(values);
	return context->Submit(query, named_params, query_parameters);
}

unique_ptr<QueryResult> Connection::Submit(unique_ptr<SQLStatement> statement, vector<Value> &values,
                                           const QueryParameters &query_parameters) {
	auto named_params = ConvertParamListToMap(values);
	return context->Submit(std::move(statement), named_params, query_parameters);
}

unique_ptr<PreparedStatement> Connection::Prepare(const string &query) {
	return context->Prepare(query);
}

unique_ptr<PreparedStatement> Connection::Prepare(unique_ptr<SQLStatement> statement) {
	return context->Prepare(std::move(statement));
}

unique_ptr<QueryResult> Connection::QueryParamsRecursive(const string &query, vector<Value> &values) {
	auto named_params = ConvertParamListToMap(values);
	QueryParameters parameters;
	parameters.statement_args = named_params;
	return context->Query(query, parameters);
}

unique_ptr<TableDescription> Connection::TableInfo(const Identifier &database_name, const Identifier &schema_name,
                                                   const Identifier &table_name) {
	return context->TableInfo(database_name, schema_name, table_name);
}

unique_ptr<TableDescription> Connection::TableInfo(const Identifier &schema_name, const Identifier &table_name) {
	return TableInfo(Identifier::InvalidCatalog(), schema_name, table_name);
}

unique_ptr<TableDescription> Connection::TableInfo(const Identifier &table_name) {
	return TableInfo(Identifier::InvalidCatalog(), Identifier::DefaultSchema(), table_name);
}

vector<unique_ptr<SQLStatement>> Connection::ExtractStatements(const string &query, bool apply_invalidation_policy) {
	auto &client_context = *context;
	vector<unique_ptr<SQLStatement>> result;
	try {
		auto iterator = client_context.IterateStatements(query, apply_invalidation_policy);
		while (iterator.Peek()) {
			result.push_back(iterator.GetStatement());
		}
	} catch (std::exception &ex) {
		ErrorData error(ex);
		client_context.ProcessError(error, query);
		error.Throw();
	}
	return result;
}

unique_ptr<LogicalOperator> Connection::ExtractPlan(const string &query) {
	return context->ExtractPlan(query);
}

void Connection::Append(TableDescription &description, ColumnDataCollection &collection) {
	context->Append(description, collection);
}

unordered_set<string> Connection::GetTableNames(const string &query, const bool qualified) {
	return context->GetTableNames(query, qualified);
}

void Connection::BeginTransaction() {
	TransactionInfo info(TransactionType::BEGIN_TRANSACTION);
	context->RunTransactionStatement(info);
}

void Connection::Commit() {
	TransactionInfo info(TransactionType::COMMIT);
	context->RunTransactionStatement(info);
}

void Connection::Rollback() {
	TransactionInfo info(TransactionType::ROLLBACK);
	context->RunTransactionStatement(info);
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
