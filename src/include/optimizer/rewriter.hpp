//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// optimizer/rewriter.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "optimizer/rule.hpp"

namespace duckdb {

class Rewriter {
  public:
	BindContext &context;
	MatchOrder match_order;
	std::vector<std::unique_ptr<Rule>> rules;

	Rewriter(BindContext &context) : context(context) {
	}
	Rewriter(BindContext &context, std::vector<std::unique_ptr<Rule>> rules,
	         MatchOrder match_order)
	    : context(context), match_order(match_order), rules(std::move(rules)) {
	}

	std::unique_ptr<LogicalOperator>
	ApplyRules(std::unique_ptr<LogicalOperator> root);

	static bool MatchOperands(AbstractRuleNode *node, AbstractOperator rel,
	                          std::vector<AbstractOperator> &bindings);
};

} // namespace duckdb
