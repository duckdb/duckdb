//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_candidate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class ClientContext;
class LogicalComparisonJoin;

enum class DuplicateEliminatedDomainCoverage : uint8_t { EXACT, SUPERSET };

class DuplicateEliminatedDomainCandidate {
public:
	unique_ptr<LogicalOperator> &Source() const {
		return source.get();
	}
	const vector<idx_t> &KeyIndices() const {
		return key_indices;
	}
	DuplicateEliminatedDomainCoverage Coverage() const {
		return coverage;
	}

private:
	friend class DuplicateEliminatedDomainAnalyzer;

	DuplicateEliminatedDomainCandidate(unique_ptr<LogicalOperator> &source_p, vector<idx_t> key_indices_p,
	                                   DuplicateEliminatedDomainCoverage coverage_p)
	    : source(source_p), key_indices(std::move(key_indices_p)), coverage(coverage_p) {
	}

	reference<unique_ptr<LogicalOperator>> source;
	vector<idx_t> key_indices;
	DuplicateEliminatedDomainCoverage coverage;
};

//! Analyzes the payload and selects a subtree that covers every duplicate-eliminated key.
class DuplicateEliminatedDomainAnalyzer {
public:
	//! A SUPERSET candidate is returned only when evaluating the complete RHS for additional groups is proven safe.
	static optional<DuplicateEliminatedDomainCandidate> FindBest(ClientContext &context, LogicalComparisonJoin &join,
	                                                             bool can_evaluate_additional_groups);
};

} // namespace duckdb
