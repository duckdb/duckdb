//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/ordered_aggregate_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/group_by_node.hpp"

namespace duckdb {

class OrderedAggregateOptimizer : public Rule {
public:
	explicit OrderedAggregateOptimizer(ExpressionRewriter &rewriter);

	static unique_ptr<Expression> Apply(ClientContext &context, BoundAggregateExpression &aggr,
	                                    vector<unique_ptr<Expression>> &groups,
	                                    optional_ptr<vector<GroupingSet>> grouping_sets, bool &changes_made);
	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb
