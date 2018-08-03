//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/parser.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "optimizer/rule.hpp"

namespace duckdb {

enum class MatchOrder { ARBITRARY = 0, DEPTH_FIRST };

class ExpressionRewriter {
  public:
	MatchOrder match_order;
	std::vector<std::unique_ptr<OptimizerRule>> rules;

	ExpressionRewriter(std::vector<std::unique_ptr<OptimizerRule>> rules,
	                   MatchOrder match_order)
	    : rules(std::move(rules)), match_order(match_order) {}
	std::unique_ptr<AbstractExpression>
	ApplyRules(std::unique_ptr<AbstractExpression> root);
	bool MatchOperands(OptimizerNode &node, AbstractExpression &rel,
	                   std::vector<AbstractExpression *> &bindings);
};

} // namespace duckdb
