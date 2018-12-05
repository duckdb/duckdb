//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/logical_rules/subquery_rewrite.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class SubqueryRewritingRule : public Rule {
public:
	SubqueryRewritingRule();

	unique_ptr<LogicalOperator> Apply(Rewriter &rewriter, LogicalOperator &op_root, vector<AbstractOperator> &bindings,
	                                  bool &fixed_point);
};

struct JoinCondition;
class SubqueryExpression;

void ExtractCorrelatedExpressions(LogicalOperator *op, SubqueryExpression *subquery, size_t subquery_table_index,
                                  vector<JoinCondition> &join_conditions);
} // namespace duckdb
