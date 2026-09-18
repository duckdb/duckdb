#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"

namespace duckdb {

struct GenericCopyOptionValue {
	bool has_value = false;
	bool is_order_list = false;
	vector<OrderByNode> order_list;
	unique_ptr<ParsedExpression> expression;
};

} // namespace duckdb
