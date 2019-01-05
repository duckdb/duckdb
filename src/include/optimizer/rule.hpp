//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/rule.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/matcher/expression_matcher.hpp"
#include "optimizer/matcher/logical_operator_matcher.hpp"

namespace duckdb {

class Rule {
public:
	virtual ~Rule() {}
	
	//! The root
	unique_ptr<LogicalOperatorMatcher> logical_root;
	//! The expression matcher of the rule
	unique_ptr<ExpressionMatcher> root;

	virtual unique_ptr<Expression> Apply(vector<Expression*>& bindings, bool &fixed_point) = 0;
};

} // namespace duckdb
