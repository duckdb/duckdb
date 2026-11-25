//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/relation_statistics/equality_cardinality.hpp
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/pair.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics.hpp"
#include "duckdb/optimizer/join_order/join_order_source_stats.hpp"

namespace duckdb {

struct EqualityCardinalityState;
struct EqualityFactor;

struct EqualitySupport {
	CardinalityValue total;
	CardinalityValue current;
	support_identities_t identities;
	bool has_usable_domain = true;
};

struct EqualitySelectedSupport {
	EqualitySupport support;
	bool population_conditioned;
};

struct EqualityComponent {
	vector<ColumnBinding> bindings;
};

struct EqualityCardinalityEstimate {
	CardinalityValue selectivity = 1;
	//! A statistical match bound from generic uniqueness, never an emptiness proof.
	optional<CardinalityValue> maximum;
};

enum class EqualitySelection : uint8_t { ALL, MATCHED, UNMATCHED };

//! Finite selections keep their source population; joins may refine uncertain domains from other inputs.
enum class EqualityDomainPolicy : uint8_t { JOIN, SOURCE_LOCAL };

struct EqualityProjection {
	EqualitySelection selection = EqualitySelection::MATCHED;
	//! Raw selected-input coverage, never output fanout or the positive costing floor.
	optional<CardinalityValue> matched_rows;
	bool preserve_keys = false;
	bool preserve_identity = false;
	bool null_extended = false;
};

//! One immutable input snapshot. Normalization precedes every region estimate; requesting a region adds no proof facts.
class EqualityCardinalityModel {
public:
	//! Capture raw source observations by value after aliases and key/joint populations are finalized.
	explicit EqualityCardinalityModel(vector<RelationStats> &relations, const join_column_sources_t &sources = {});
	~EqualityCardinalityModel();

	ColumnBinding Normalize(ColumnBinding binding) const;
	vector<EqualityComponent> Components(const vector<pair<ColumnBinding, ColumnBinding>> &equalities) const;
	EqualityCardinalityEstimate Estimate(const vector<EqualityComponent> &components, const vector<idx_t> &relations,
	                                     EqualityDomainPolicy domain_policy = EqualityDomainPolicy::JOIN) const;
	//! Intersection and denominator count each normalized binding, and each proven identical support, appropriately.
	EqualitySupport Intersect(const vector<EqualitySupport> &supports, CardinalityValue &factor) const;
	EqualitySupport ColumnSupport(ColumnBinding binding) const;
	optional<EqualitySupport> JointSupport(idx_t relation, const vector<idx_t> &ordered_columns) const;
	bool PreservesMultiplicity(idx_t relation, const vector<idx_t> &participants,
	                           const vector<EqualityComponent> &components) const;
	//! Project source columns and populations once, with separate statistical selection and output proof effects.
	//! value_equalities contains only validated direct same-type value equalities, before numeric normalization.
	void Project(RelationStats &output, const vector<EqualityComponent> &components,
	             const vector<EqualityProjection> &inputs, const vector<EqualityComponent> &output_equalities,
	             const vector<pair<ColumnBinding, ColumnBinding>> &value_equalities = {}) const;
	//! Export keys/populations through one binding map. Joins drop inherited support identity unless separately proved.
	void ProjectEvidence(RelationStats &output, const vector<EqualityComponent> &components,
	                     const vector<bool> &preserved_keys, const vector<bool> &preserved_support,
	                     const vector<bool> &null_extended = {},
	                     const vector<pair<ColumnBinding, ColumnBinding>> &value_equalities = {}) const;

private:
	EqualitySupport IntersectJoint(const vector<EqualitySupport> &joint_points,
	                               const vector<vector<EqualitySupport>> &coordinates, CardinalityValue &factor) const;
	vector<EqualitySupport> ResolvePairDomains(const EqualityComponent &component) const;
	vector<EqualityFactor> BuildFactors(const vector<EqualityComponent> &components) const;
	optional<EqualitySelectedSupport> SelectSupport(idx_t relation, const vector<idx_t> &axes,
	                                                const vector<idx_t> &dependent_columns, EqualitySupport support,
	                                                const vector<EqualityComponent> &components,
	                                                const vector<EqualityFactor> &factors,
	                                                const EqualityProjection &projection) const;
	void ProjectInternal(RelationStats &output, const vector<EqualityComponent> &components,
	                     const vector<EqualityProjection> &inputs, const vector<EqualityComponent> &output_equalities,
	                     bool project_columns,
	                     const vector<pair<ColumnBinding, ColumnBinding>> &value_equalities) const;
	unique_ptr<EqualityCardinalityState> state;
};

} // namespace duckdb
