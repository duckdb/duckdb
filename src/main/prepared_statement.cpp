#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"

namespace duckdb {

PreparedStatement::PreparedStatement(const shared_ptr<ClientContext> &context_p, string name_p, string query_p)
    : context(context_p), name(std::move(name_p)), query(std::move(query_p)), success(true) {
	D_ASSERT(!context.expired());
}

PreparedStatement::PreparedStatement(ErrorData error) : success(false), error(std::move(error)) {
}

PreparedStatement::~PreparedStatement() {
	auto client_context = context.lock();
	if (!client_context) {
		// the client context is gone - the prepared statement has been deallocated together with it
		return;
	}
	client_context->RemovePreparedStatement(name);
}

const string &PreparedStatement::GetError() {
	D_ASSERT(HasError());
	return error.Message();
}

ErrorData &PreparedStatement::GetErrorObject() {
	return error;
}

bool PreparedStatement::HasError() const {
	return !success;
}

shared_ptr<ClientContext> PreparedStatement::TryGetContext() const {
	return context.lock();
}

shared_ptr<PreparedStatementData> PreparedStatement::TryGetData() const {
	auto client_context = context.lock();
	if (!client_context) {
		return nullptr;
	}
	return client_context->GetPreparedStatement(name);
}

shared_ptr<PreparedStatementData> PreparedStatement::GetData() const {
	auto data = TryGetData();
	if (!data) {
		throw InvalidInputException("Prepared statement \"%s\" is no longer available - the connection it was "
		                            "prepared in has been closed, or the statement has been deallocated",
		                            name);
	}
	return data;
}

idx_t PreparedStatement::ColumnCount() {
	return GetData()->types.size();
}

StatementType PreparedStatement::GetStatementType() {
	return GetData()->statement_type;
}

StatementProperties PreparedStatement::GetStatementProperties() {
	return GetData()->properties;
}

vector<LogicalType> PreparedStatement::GetTypes() {
	return GetData()->types;
}

vector<Identifier> PreparedStatement::GetNames() {
	return GetData()->names;
}

identifier_map_t<idx_t> PreparedStatement::GetNamedParameterMap() {
	auto data = GetData();
	if (!data->unbound_statement) {
		return identifier_map_t<idx_t>();
	}
	return data->unbound_statement->named_param_map;
}

idx_t PreparedStatement::GetParameterCount() {
	auto data = GetData();
	if (!data->unbound_statement) {
		return 0;
	}
	return data->unbound_statement->named_param_map.size();
}

bool PreparedStatement::TryGetParameterType(const Identifier &identifier, LogicalType &result) {
	return GetData()->TryGetType(identifier, result);
}

case_insensitive_map_t<LogicalType> PreparedStatement::GetExpectedParameterTypes() const {
	auto data = GetData();
	case_insensitive_map_t<LogicalType> expected_types(data->value_map.size());
	for (auto &it : data->value_map) {
		auto &identifier = it.first;
		D_ASSERT(it.second);
		expected_types[identifier.GetIdentifierName()] = it.second->GetValue().type();
	}
	return expected_types;
}

unique_ptr<SQLStatement>
PreparedStatement::CreateExecuteStatement(const identifier_map_t<BoundParameterData> &named_values) const {
	auto execute = make_uniq<ExecuteStatement>();
	execute->name = Identifier(name);
	// report the query that was prepared - not the generated EXECUTE - in errors and profiling output
	execute->query = query;
	execute->stmt_location = QueryLocation(0, query.size());
	// the values are already typed - pass them in pre-bound instead of as SQL literals
	execute->bound_values = named_values;
	return std::move(execute);
}

unique_ptr<QueryResult> PreparedStatement::Execute(identifier_map_t<BoundParameterData> &named_values,
                                                   bool allow_stream_result) {
	if (!success) {
		return make_uniq<MaterializedQueryResult>(
		    ErrorData(InvalidInputException("Attempting to execute an unsuccessfully prepared statement!")));
	}
	auto client_context = context.lock();
	if (!client_context) {
		return make_uniq<MaterializedQueryResult>(ErrorData(
		    InvalidInputException("Attempting to execute a prepared statement after its connection was closed!")));
	}
	PendingQueryParameters parameters;
	parameters.query_parameters.output_type =
	    allow_stream_result ? QueryResultOutputType::ALLOW_STREAMING : QueryResultOutputType::FORCE_MATERIALIZED;
	return client_context->RunInternalStatement(CreateExecuteStatement(named_values), parameters);
}

unique_ptr<QueryResult> PreparedStatement::Execute(vector<Value> &values, bool allow_stream_result) {
	identifier_map_t<BoundParameterData> named_values;
	for (idx_t i = 0; i < values.size(); i++) {
		named_values[Identifier(std::to_string(i + 1))] = BoundParameterData(values[i]);
	}
	return Execute(named_values, allow_stream_result);
}

unique_ptr<PendingQueryResult> PreparedStatement::PendingQuery(vector<Value> &values, bool allow_stream_result) {
	identifier_map_t<BoundParameterData> named_values;
	for (idx_t i = 0; i < values.size(); i++) {
		auto &val = values[i];
		named_values[Identifier(std::to_string(i + 1))] = BoundParameterData(val);
	}
	return PendingQuery(named_values, allow_stream_result);
}

unique_ptr<PendingQueryResult> PreparedStatement::PendingQuery(identifier_map_t<BoundParameterData> &named_values,
                                                               bool allow_stream_result) {
	if (!success) {
		auto exception = InvalidInputException("Attempting to execute an unsuccessfully prepared statement!");
		return make_uniq<PendingQueryResult>(ErrorData(exception));
	}
	auto client_context = context.lock();
	if (!client_context) {
		auto exception =
		    InvalidInputException("Attempting to execute a prepared statement after its connection was closed!");
		return make_uniq<PendingQueryResult>(ErrorData(exception));
	}
	PendingQueryParameters parameters;
	parameters.query_parameters.output_type =
	    allow_stream_result ? QueryResultOutputType::ALLOW_STREAMING : QueryResultOutputType::FORCE_MATERIALIZED;
	return client_context->PendingInternalStatement(CreateExecuteStatement(named_values), parameters);
}

} // namespace duckdb
