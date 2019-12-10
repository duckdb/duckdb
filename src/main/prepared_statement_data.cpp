#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/execution/physical_operator.hpp"

using namespace duckdb;
using namespace std;

PreparedStatementData::PreparedStatementData(StatementType type) : statement_type(type) {

}

PreparedStatementData::~PreparedStatementData() {

}

void PreparedStatementData::Bind(vector<Value> values) {
	// set parameters
	if (values.size() != value_map.size()) {
		throw BinderException("Parameter/argument count mismatch for prepared statement");
	}
	// bind the values
	for(index_t i = 0; i < values.size(); i++) {
		auto it = value_map.find(i + 1);
		if (it == value_map.end()) {
			throw BinderException("Could not find parameter with index %llu", i + 1);
		}
		assert(values[i].type == GetInternalType(it->second.target_type));
		auto &target = it->second;
		*target.value = values[i];
	}
}

SQLType PreparedStatementData::GetType(index_t param_idx) {
	auto it = value_map.find(param_idx);
	if (it == value_map.end()) {
		throw BinderException("Could not find parameter with index %llu", param_idx);
	}
	return it->second.target_type;
}
