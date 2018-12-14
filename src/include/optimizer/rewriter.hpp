//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class Rewriter {
public:
	BindContext &context;
	MatchOrder match_order;
	vector<unique_ptr<Rule>> rules;

	Rewriter(BindContext &context) : context(context) {
	}
	Rewriter(BindContext &context, vector<unique_ptr<Rule>> rules, MatchOrder match_order)
	    : context(context), match_order(match_order), rules(std::move(rules)) {
	}

	unique_ptr<LogicalOperator> ApplyRules(unique_ptr<LogicalOperator> root);

	static bool MatchOperands(AbstractRuleNode *node, AbstractOperator rel, vector<AbstractOperator> &bindings);
};

} // namespace duckdb
