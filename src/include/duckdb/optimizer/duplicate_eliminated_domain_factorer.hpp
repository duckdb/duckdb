//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain_factorer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

class Binder;
struct DuplicateEliminatedDomainCandidate;

struct FactoredDuplicateEliminatedDomain {
	TableIndex cte_index;
	Identifier cte_name;
	idx_t column_count;
	unique_ptr<LogicalOperator> source;
	unique_ptr<LogicalOperator> domain;
	BindingReplacementGraph output_replacements;
};

//! Factors a duplicate-eliminated domain from a cheaper, covering subtree of a join's outer payload.
class DuplicateEliminatedDomainFactorer {
public:
	static unique_ptr<FactoredDuplicateEliminatedDomain> TryFactor(Binder &binder, unique_ptr<LogicalOperator> &join,
	                                                               const DuplicateEliminatedDomainCandidate &candidate);
};

} // namespace duckdb
