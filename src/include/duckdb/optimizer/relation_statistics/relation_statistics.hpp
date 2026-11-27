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
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/optimizer/relation_statistics/cardinality_value.hpp"

namespace duckdb {

//! DERIVED describes a computed population, not the confidence of an original source observation.
enum class DistinctCountSource { CARDINALITY, MIN_MAX, HLL, EXACT, DERIVED };

struct DistinctCount {
public:
	DistinctCount(CardinalityValue distinct_count, DistinctCountSource source);
	DistinctCount(CardinalityValue distinct_count, DistinctCountSource source, bool has_usable_domain);

public:
	static DistinctCountSource CombineSources(DistinctCountSource left, DistinctCountSource right);
	//! Reconcile alternatives for the same population, keeping an actual usable point and its evidence together.
	static DistinctCount ReconcilePopulation(const DistinctCount &left, const DistinctCount &right);

public:
	CardinalityValue distinct_count;
	DistinctCountSource source;
	//! Evidence for this population point, independent of conditioning and of any pair-cost imputation.
	bool has_usable_domain;
};

//! Identity of one closed producer within an immutable statistics snapshot.
struct SupportOrigin {};

struct SupportIdentity {
	shared_ptr<const SupportOrigin> origin;
	//! Ordered coordinates in the correlated producer, not scalar domain identifiers.
	vector<idx_t> coordinates;
};

using support_identities_t = shared_ptr<const vector<SupportIdentity>>;

struct RelationTupleIdentity {
	vector<idx_t> columns;
	SupportIdentity identity;
};

struct RelationJointStats {
	vector<idx_t> columns;
	DistinctCount total_domain;
	DistinctCount current_domain;
	support_identities_t identities;
	//! Original columns functionally determined by these axes, even when later fanout duplicates their tuples.
	//! This is not row uniqueness and does not extend to payload introduced by that fanout.
	vector<idx_t> dependent_columns;
};

enum class RestrictionCombination { CONJUNCTION, DISJUNCTION };

//! A predicate satisfied by every current non-NULL value, not a claim that every allowed value occurs.
struct ColumnValueRestriction {
public:
	ColumnValueRestriction(value_set_t values, bool excluded);

public:
	static shared_ptr<const ColumnValueRestriction> Create(value_set_t values, bool excluded);
	//! A null pointer is unrestricted. Compose locally and publish one immutable result, reusing unchanged operands.
	static shared_ptr<const ColumnValueRestriction>
	Combine(const vector<shared_ptr<const ColumnValueRestriction>> &inputs, RestrictionCombination operation);

public:
	value_set_t values;
	bool excluded;
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

public:
	optional<CardinalityValue> GetSupportedSemiAntiDomainSize() const;
	optional_idx PopulationBound() const;
	void TightenPopulationBound(idx_t bound);

public:
	ColumnBinding binding;
	//! The estimated size of the complete value domain. Equality joins use this as their denominator.
	DistinctCount total_domain;
	//! The estimated number of values from the total domain that are currently present.
	DistinctCount current_domain;
	//! Structural evidence that permits using current_domain for SEMI/ANTI joins.
	SemiAntiJoinDomainEvidence semi_anti_join_domain_evidence;
	//! Equivalent origins of this scalar support. Copies share the immutable proof set.
	support_identities_t support_identities;
	shared_ptr<const ColumnValueRestriction> value_restriction;
	Identifier name;
	//! Whether a NULL value is possible; absence of source evidence is conservative.
	bool can_have_null = true;

private:
	//! A proven upper bound on the original population, independent of retained rows and NDV points.
	optional_idx population_bound;
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
	//! Remap all column-index evidence with the same projection, including retained aliases.
	void CopyEvidence(const RelationStats &source, const vector<optional_idx> &source_columns);
	//! Direct same-type value aliases, independent of statistical equality and support-coordinate closure.
	vector<idx_t> GetValueAliases() const;
	//! Append direct mapped value facts without importing numerical or population evidence.
	void CopyValueRestrictions(const RelationStats &source, const vector<optional_idx> &source_columns);
	void AddUniqueKey(vector<idx_t> key);
	bool HasUniqueKey(const vector<idx_t> &columns) const;
	//! Record or reuse the producer identity shared by materialized references and their value domains.
	void EnsureMaterializedProducerIdentity();

public:
	vector<RelationColumnStats> columns;
	CardinalityValue cardinality;
	//! A provider or structural upper bound, never inferred from the point estimate.
	optional_idx max_cardinality;
	//! Cumulative estimated fraction of rows retained from the most recent statistics source.
	//! Aggregation and DISTINCT establish a new source.
	CardinalityValue row_retention = 1;
	//! Bounded, value-owned snapshots of generic Distinctness proofs.
	vector<vector<idx_t>> unique_keys;
	//! Same-row equality (including NULL), distinct from equality of value supports.
	vector<vector<idx_t>> equivalent_columns;
	vector<vector<idx_t>> value_equivalent_columns;
	//! Original and retained populations, independent of whether fanout preserved row uniqueness.
	vector<RelationJointStats> joint_stats;
	shared_ptr<const vector<RelationTupleIdentity>> tuple_identities;
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
