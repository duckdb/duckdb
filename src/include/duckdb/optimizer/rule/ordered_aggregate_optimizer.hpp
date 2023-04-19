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

namespace duckdb {

class OrderedAggregateOptimizer : public Rule {
public:
	explicit OrderedAggregateOptimizer(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb
