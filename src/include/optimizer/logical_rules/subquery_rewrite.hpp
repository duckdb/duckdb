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

#include <algorithm>
#include <vector>

#include "common/exception.hpp"
#include "common/internal_types.hpp"
#include "optimizer/rule.hpp"
#include "parser/expression/list.hpp"
#include "planner/operator/list.hpp"

namespace duckdb {

class SubqueryRewritingRule : public Rule {
  public:
	SubqueryRewritingRule();

	std::unique_ptr<LogicalOperator>
	Apply(Rewriter &rewriter, LogicalOperator &op_root,
	      std::vector<AbstractOperator> &bindings);
};

} // namespace duckdb
