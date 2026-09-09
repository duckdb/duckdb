#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {
struct CreateTableDefinition {
	unique_ptr<SelectStatement> select_statement;
	ColumnList columns;
	vector<unique_ptr<Constraint>> constraints;
	vector<unique_ptr<ParsedExpression>> partition_keys;
	vector<unique_ptr<ParsedExpression>> sort_keys;
	case_insensitive_map_t<unique_ptr<ParsedExpression>> options;
};
} // namespace duckdb
