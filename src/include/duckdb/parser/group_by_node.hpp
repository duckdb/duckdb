//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/group_by_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

using GroupingSet = set<idx_t>;

class GroupByNode {
public:
	//! The total set of all group expressions
	vector<unique_ptr<ParsedExpression>> group_expressions;
	//! The different grouping sets as they map to the group expressions
	vector<GroupingSet> grouping_sets;

public:
	GroupByNode Copy() {
		GroupByNode node;
		node.group_expressions.reserve(group_expressions.size());
		for (auto &expr : group_expressions) {
			node.group_expressions.push_back(expr->Copy());
		}
		node.grouping_sets = grouping_sets;
		return node;
	}
};

} // namespace duckdb
