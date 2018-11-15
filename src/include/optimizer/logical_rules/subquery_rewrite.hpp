//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// optimizer/logical_rules/subquery_rewrite.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class SubqueryRewritingRule : public Rule {
  public:
	SubqueryRewritingRule();

	std::unique_ptr<LogicalOperator>
	Apply(Rewriter &rewriter, LogicalOperator &op_root,
	      std::vector<AbstractOperator> &bindings, bool &fixed_point);
};

struct JoinCondition;
class SubqueryExpression;

void ExtractCorrelatedExpressions(LogicalOperator *op,
                                  SubqueryExpression *subquery,
                                  size_t subquery_table_index,
                                  std::vector<JoinCondition> &join_conditions);
} // namespace duckdb
