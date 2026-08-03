//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_factorer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/subquery/delim_join_cte_rewriter.hpp"

namespace duckdb {

class Binder;
class DuplicateEliminatedDomainCandidate;

//! Factors a duplicate-eliminated domain from a cheaper, covering subtree of a join's outer payload.
class DuplicateEliminatedDomainFactorer {
public:
	static unique_ptr<DelimJoinCTEOptimizationAlternative>
	TryFactor(Binder &binder, unique_ptr<LogicalOperator> &join, TableIndex domain_cte_index,
	          const DuplicateEliminatedDomainCandidate &candidate);
};

} // namespace duckdb
