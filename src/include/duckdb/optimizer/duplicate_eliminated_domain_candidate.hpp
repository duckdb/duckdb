//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain_candidate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class ClientContext;
class LogicalComparisonJoin;

struct DuplicateEliminatedDomainCandidate {
	DuplicateEliminatedDomainCandidate(unique_ptr<LogicalOperator> &source_p, vector<idx_t> key_indices_p,
	                                   idx_t joins_above_p)
	    : source(source_p), key_indices(std::move(key_indices_p)), joins_above(joins_above_p) {
	}

	reference<unique_ptr<LogicalOperator>> source;
	vector<idx_t> key_indices;
	idx_t joins_above;
};

//! Finds and costs a subtree that covers every duplicate-eliminated key.
class DuplicateEliminatedDomainCandidateFinder {
public:
	static unique_ptr<DuplicateEliminatedDomainCandidate> FindBest(ClientContext &context, LogicalComparisonJoin &join,
	                                                               TableIndex domain_cte_index);
};

} // namespace duckdb
