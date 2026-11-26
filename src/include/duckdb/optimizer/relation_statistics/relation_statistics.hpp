//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/relation_statistics/relation_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/optimizer/relation_statistics/cardinality_value.hpp"

namespace duckdb {

enum class DistinctCountSource : uint8_t { CARDINALITY, MIN_MAX, HLL, EXACT };

struct DistinctCount {
public:
	DistinctCount(CardinalityValue distinct_count, DistinctCountSource source);

public:
	CardinalityValue distinct_count;
	DistinctCountSource source;
};

struct SemiAntiJoinDomainEvidence {
public:
	//! A bound established by predicates or a closed input, independent of estimated row retention.
	void TightenDomainBound(idx_t bound);
	void MarkUnique();
	void Invalidate();
	optional<CardinalityValue> GetSupportedDomainSize(CardinalityValue current_domain) const;
	optional_idx GetDomainBound() const;

private:
	optional_idx domain_bound;
	bool is_unique = false;
};

struct RelationColumnStats {
public:
	RelationColumnStats(ColumnBinding binding, DistinctCount domain, Identifier name);
	RelationColumnStats(ColumnBinding binding, DistinctCount total_domain, DistinctCount current_domain,
	                    Identifier name,
	                    SemiAntiJoinDomainEvidence semi_anti_join_domain_evidence = SemiAntiJoinDomainEvidence());
	optional<CardinalityValue> GetSupportedSemiAntiDomainSize() const;

public:
	ColumnBinding binding;
	//! The estimated size of the complete value domain. Equality joins use this as their denominator.
	DistinctCount total_domain;
	//! The estimated number of values from the total domain that are currently present.
	DistinctCount current_domain;
	//! Structural evidence that permits using current_domain for SEMI/ANTI joins.
	SemiAntiJoinDomainEvidence semi_anti_join_domain_evidence;
	Identifier name;
};

struct RelationStats {
public:
	RelationStats();

public:
	optional_idx FindColumn(ColumnBinding binding) const;
	optional_ptr<const RelationColumnStats> GetColumnStats(ColumnBinding binding) const;
	bool MatchesBindings(const vector<ColumnBinding> &bindings) const;
	//! Reconcile the point with its structural upper bound, then cap current column domains.
	void CapCurrentDomainsToCardinality();
	void Verify(const vector<ColumnBinding> &bindings) const;
	bool IsEmpty() const;
	//! Raw point reconciled with its independent upper bound; no cost or display floor.
	CardinalityValue GetCardinality() const;
	CardinalityValue GetCardinalityForCost() const;
	idx_t GetCardinalityForPlan() const;

public:
	vector<RelationColumnStats> columns;
	CardinalityValue cardinality;
	//! A provider or structural upper bound, never inferred from the point estimate.
	optional_idx max_cardinality;
	//! Cumulative estimated fraction of rows retained from the most recent statistics source.
	//! Aggregation and DISTINCT establish a new source.
	CardinalityValue row_retention = 1;
	//! Indexes of the columns that together identify a row of a join-order relation, e.g. the groups of an
	//! aggregate. Empty when no such key is known.
	vector<idx_t> unique_key;
	bool stats_initialized = false;
	Identifier table_name;
};

//! Looks up column statistics by binding, the first relation and column with a binding wins
class RelationColumnStatsLookup {
public:
	RelationColumnStatsLookup() = default;
	explicit RelationColumnStatsLookup(const RelationStats &stats);

public:
	void AddRelation(const RelationStats &stats);
	optional_ptr<const RelationColumnStats> Find(ColumnBinding binding) const;

private:
	column_binding_map_t<reference<const RelationColumnStats>> columns;
};

} // namespace duckdb
