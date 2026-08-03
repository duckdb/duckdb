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
class DuplicateEliminatedDomainCTERegistry;

class DuplicateEliminatedDomainCandidate {
public:
	optional_ptr<unique_ptr<LogicalOperator>> TryResolveSource(unique_ptr<LogicalOperator> &payload) const;
	const vector<idx_t> &KeyIndices() const {
		return key_indices;
	}
	const vector<idx_t> &SourcePath() const {
		return source_path;
	}
	idx_t SourceCardinality() const {
		return source_cardinality;
	}
	idx_t DomainCardinality() const {
		return domain_cardinality;
	}
	idx_t PayloadCardinality() const {
		return payload_cardinality;
	}
	idx_t PayloadDomainCardinality() const {
		return payload_domain_cardinality;
	}
	LogicalOperatorType SourceType() const {
		return source_type;
	}
	const vector<LogicalType> &SourceTypes() const {
		return source_types;
	}

private:
	friend class DuplicateEliminatedDomainAnalyzer;

	DuplicateEliminatedDomainCandidate(LogicalOperator &source, vector<idx_t> source_path_p,
	                                   vector<idx_t> key_indices_p, idx_t source_cardinality_p,
	                                   idx_t domain_cardinality_p, idx_t payload_cardinality_p,
	                                   idx_t payload_domain_cardinality_p)
	    : source_path(std::move(source_path_p)), key_indices(std::move(key_indices_p)),
	      source_cardinality(source_cardinality_p), domain_cardinality(domain_cardinality_p),
	      payload_cardinality(payload_cardinality_p), payload_domain_cardinality(payload_domain_cardinality_p),
	      source_type(source.type), source_types(source.types), source_bindings(source.GetColumnBindings()) {
	}

	vector<idx_t> source_path;
	vector<idx_t> key_indices;
	idx_t source_cardinality;
	idx_t domain_cardinality;
	idx_t payload_cardinality;
	idx_t payload_domain_cardinality;
	LogicalOperatorType source_type;
	vector<LogicalType> source_types;
	vector<ColumnBinding> source_bindings;
};

//! Analyzes the payload and selects a subtree that covers every duplicate-eliminated key.
class DuplicateEliminatedDomainAnalyzer {
public:
	//! Returns true when every grouped generated-domain restriction reads the same CTE keys as the outer payload.
	static bool CanEliminateEquivalentSourceDomain(LogicalComparisonJoin &join, LogicalOperator &rhs,
	                                               TableIndex domain_cte_index);
	//! A SUPERSET candidate is returned only when evaluating the complete RHS for additional groups is proven safe.
	static optional<DuplicateEliminatedDomainCandidate>
	FindBest(ClientContext &context, const DuplicateEliminatedDomainCTERegistry &cte_registry,
	         LogicalComparisonJoin &join, bool can_evaluate_additional_groups);
};

} // namespace duckdb
