//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// optimizer/rewriter.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "optimizer/expression_rule.hpp"
#include "optimizer/logical_rule.hpp"

namespace duckdb {

template<class RULETYPE, class NODETYPE, class TREETYPE>
class Rewriter {
  public:
	MatchOrder match_order;
	std::vector<std::unique_ptr<RULETYPE>> rules;

	Rewriter() {}
	Rewriter(std::vector<std::unique_ptr<RULETYPE>> rules,
	                   MatchOrder match_order)
	    : rules(std::move(rules)), match_order(match_order) {}

	std::unique_ptr<TREETYPE>
	ApplyRules(std::unique_ptr<TREETYPE> root);
	
	bool MatchOperands(NODETYPE *node, TREETYPE &rel,
	                   std::vector<TREETYPE *> &bindings);
};
typedef Rewriter<ExpressionRule, ExpressionNode, AbstractExpression> ExpressionRewriter;
typedef Rewriter<LogicalRule, LogicalNode, LogicalOperator> LogicalRewriter;

} // namespace duckdb
