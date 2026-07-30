//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_inliner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/table_index.hpp"

namespace duckdb {

class Binder;
class LogicalOperator;
struct DuplicateEliminatedDomainCandidate;

//! Replaces generated domain references with duplicate-free copies of a cheap, deterministic source.
class DuplicateEliminatedDomainInliner {
public:
	static bool TryInline(Binder &binder, unique_ptr<LogicalOperator> &rhs, TableIndex domain_cte_index,
	                      idx_t domain_ref_count, const DuplicateEliminatedDomainCandidate &candidate);
};

} // namespace duckdb
