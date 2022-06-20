#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

PreparedStatementData::PreparedStatementData(StatementType type) : statement_type(type) {
}

PreparedStatementData::~PreparedStatementData() {
}

void PreparedStatementData::Bind(vector<Value> values) {
	// set parameters
	const auto required = properties.parameter_count;
	D_ASSERT(!unbound_statement || unbound_statement->n_param == properties.parameter_count);
	if (values.size() != required) {
		throw BinderException("Parameter/argument count mismatch for prepared statement. Expected %llu, got %llu",
		                      required, values.size());
	}

	// bind the required values
	for (auto &it : value_map) {
		const idx_t i = it.first - 1;
		if (i >= values.size()) {
			throw BinderException("Could not find parameter with index %llu", i + 1);
		}
		D_ASSERT(!it.second.empty());
		if (!values[i].TryCastAs(it.second[0]->type())) {
			throw BinderException(
			    "Type mismatch for binding parameter with index %llu, expected type %s but got type %s", i + 1,
			    it.second[0]->type().ToString().c_str(), values[i].type().ToString().c_str());
		}
		for (auto &target : it.second) {
			*target = values[i];
		}
	}
}

LogicalType PreparedStatementData::GetType(idx_t param_idx) {
	auto it = value_map.find(param_idx);
	if (it == value_map.end()) {
		throw BinderException("Could not find parameter with index %llu", param_idx);
	}
	D_ASSERT(!it->second.empty());
	return it->second[0]->type();
}

} // namespace duckdb
