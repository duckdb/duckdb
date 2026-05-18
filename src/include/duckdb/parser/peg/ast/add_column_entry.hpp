#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {

struct AddColumnEntry {
	LogicalType type;
	vector<string> column_path;
	unique_ptr<ParsedExpression> default_value;

	AddColumnEntry Copy() {
		AddColumnEntry result;
		result.type = type;
		result.column_path = column_path;
		result.default_value = default_value->Copy();
		return result;
	}
};

} // namespace duckdb
