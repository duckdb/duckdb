//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/predicate_factoring.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

//! The Predicate Factoring rule extracts predicates on a common column from disjunctive or conjunctive clauses
class PredicateFactoringRule : public Rule {
public:
	explicit PredicateFactoringRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb
