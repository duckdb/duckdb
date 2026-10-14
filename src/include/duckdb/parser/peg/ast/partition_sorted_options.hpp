#pragma once
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"

namespace duckdb {
struct PartitionSortedOptions {
	vector<unique_ptr<ParsedExpression>> partition_keys;
	vector<OrderByNode> sort_keys;
};
} // namespace duckdb
