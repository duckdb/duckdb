//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_candidate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class ClientContext;
class LogicalComparisonJoin;

class DuplicateEliminatedDomainCandidate {
public:
	DuplicateEliminatedDomainCandidate(unique_ptr<LogicalOperator> &source_p, vector<idx_t> key_indices_p)
	    : source(source_p), key_indices(std::move(key_indices_p)) {
	}

	unique_ptr<LogicalOperator> &Source() const {
		return source.get();
	}
	const vector<idx_t> &KeyIndices() const {
		return key_indices;
	}

private:
	reference<unique_ptr<LogicalOperator>> source;
	vector<idx_t> key_indices;
};

//! Finds and costs a subtree that covers every duplicate-eliminated key.
class DuplicateEliminatedDomainCandidateFinder {
public:
	static unique_ptr<DuplicateEliminatedDomainCandidate> FindBest(ClientContext &context, LogicalComparisonJoin &join,
	                                                               TableIndex domain_cte_index);
};

} // namespace duckdb
