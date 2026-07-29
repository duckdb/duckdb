//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_aggregate_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

class Binder;
struct DuplicateEliminatedDomainCandidate;

//! Rewrites a selective correlated aggregate around an identity-bearing source without materializing its domain.
class DuplicateEliminatedAggregateRewriter {
public:
	static bool TryRewrite(Binder &binder, unique_ptr<LogicalOperator> &join, TableIndex domain_cte_index,
	                       LogicalOperator &rewrite_root, DuplicateEliminatedDomainCandidate &candidate,
	                       BindingReplacementGraph &output_replacements);
};

} // namespace duckdb
