#pragma once
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
struct ColumnElements {
	ColumnList columns;
	vector<unique_ptr<Constraint>> constraints;
	vector<unique_ptr<ParsedExpression>> partition_keys;
	vector<unique_ptr<ParsedExpression>> sort_keys;
	case_insensitive_map_t<unique_ptr<ParsedExpression>> options;
};
} // namespace duckdb
