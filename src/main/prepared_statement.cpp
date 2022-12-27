#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

namespace duckdb {

PreparedStatement::PreparedStatement(shared_ptr<ClientContext> context, shared_ptr<PreparedStatementData> data_p,
                                     string query, idx_t n_param, case_insensitive_map_t<idx_t> named_param_pam_p)
    : context(move(context)), data(move(data_p)), query(move(query)), success(true), n_param(n_param),
      named_param_map(move(named_param_pam_p)) {
	D_ASSERT(data || !success);
}

PreparedStatement::PreparedStatement(PreservedError error) : context(nullptr), success(false), error(move(error)) {
}

PreparedStatement::~PreparedStatement() {
}

const string &PreparedStatement::GetError() {
	D_ASSERT(HasError());
	return error.Message();
}

bool PreparedStatement::HasError() const {
	return !success;
}

idx_t PreparedStatement::ColumnCount() {
	D_ASSERT(data);
	return data->types.size();
}

StatementType PreparedStatement::GetStatementType() {
	D_ASSERT(data);
	return data->statement_type;
}

StatementProperties PreparedStatement::GetStatementProperties() {
	D_ASSERT(data);
	return data->properties;
}

const vector<LogicalType> &PreparedStatement::GetTypes() {
	D_ASSERT(data);
	return data->types;
}

const vector<string> &PreparedStatement::GetNames() {
	D_ASSERT(data);
	return data->names;
}

vector<LogicalType> PreparedStatement::GetExpectedParameterTypes() const {
	D_ASSERT(data);
	vector<LogicalType> expected_types(data->value_map.size());
	for (auto &it : data->value_map) {
		D_ASSERT(it.second);
		expected_types[it.first - 1] = it.second->value.type();
	}
	return expected_types;
}

unique_ptr<QueryResult> PreparedStatement::Execute(vector<Value> &values, bool allow_stream_result) {
	auto pending = PendingQuery(values, allow_stream_result);
	if (pending->HasError()) {
		return make_unique<MaterializedQueryResult>(pending->GetErrorObject());
	}
	return pending->Execute();
}

unique_ptr<PendingQueryResult> PreparedStatement::PendingQuery(vector<Value> &values, bool allow_stream_result) {
	if (!success) {
		throw InvalidInputException("Attempting to execute an unsuccessfully prepared statement!");
	}
	D_ASSERT(data);
	PendingQueryParameters parameters;
	parameters.parameters = &values;
	parameters.allow_stream_result = allow_stream_result && data->properties.allow_stream_result;
	auto result = context->PendingQuery(query, data, parameters);
	return result;
}

} // namespace duckdb
