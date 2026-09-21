#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {

struct AddColumnEntry {
	LogicalType type;
	vector<Identifier> column_path;
	unique_ptr<ParsedExpression> default_value;
	bool is_not_null = false;
};

} // namespace duckdb
