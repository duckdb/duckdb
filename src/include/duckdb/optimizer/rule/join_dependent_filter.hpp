//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/join_filter_derivation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

//! Join-dependent filter derivation as proposed in https://homepages.cwi.nl/~boncz/snb-challenge/chokepoints-tpctc.pdf
//! This rule inspects filters like this:
//!     SELECT *
//!     FROM nation n1
//!     JOIN nation n2
//!     ON ((n1.n_name = 'FRANCE'
//!            AND n2.n_name = 'GERMANY')
//!        OR (n1.n_name = 'GERMANY'
//!            AND n2.n_name = 'FRANCE'));
//! The join filter as a whole cannot be pushed down, because it references tables from both sides.
//! However, we can derive from this two filters that can be pushed down, namely:
//!     WHERE (n1.n_name = 'FRANCE' OR n1.n_name = 'GERMANY')
//!      AND (n2.n_name = 'GERMANY' OR n2.n_name = 'FRANCE')
//! By adding this filter, we can reduce both sides of the join before performing the join on the original condition.
class JoinDependentFilterRule : public Rule {
public:
	explicit JoinDependentFilterRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb
