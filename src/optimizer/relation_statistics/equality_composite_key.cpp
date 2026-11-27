#include "duckdb/optimizer/relation_statistics/equality_cardinality_state.hpp"
#include "duckdb/common/set.hpp"

namespace duckdb {

optional<CardinalityValue>
EqualityCardinalityModel::OriginalProducerPopulation(const vector<ColumnBinding> &columns) const {
	optional<TableIndex> producer;
	optional<idx_t> population;
	for (auto binding : columns) {
		auto entry = state->sources.find(binding);
		if (entry == state->sources.end() || entry->second.empty()) {
			return {};
		}
		for (auto &observation : entry->second) {
			// Equivalent columns from other scans do not establish the population of this tuple.
			auto &source = observation.direct;
			if ((producer && *producer != source.binding.table_index) ||
			    (population && *population != source.source_cardinality)) {
				return {};
			}
			producer = source.binding.table_index;
			population = source.source_cardinality;
		}
	}
	return population ? optional<CardinalityValue>(CardinalityValue(*population)) : optional<CardinalityValue>();
}

struct CompositeKeyCoordinate {
	idx_t component;
	idx_t point;
};

struct CompositeKeyAdjustment {
	vector<idx_t> components;
	CardinalityValue multiplier;
};

CardinalityValue
EqualityCardinalityModel::CompositeKeyCorrection(const vector<EqualityComponent> &components,
                                                 const vector<vector<EqualitySupport>> &domains,
                                                 const vector<pair<ColumnBinding, ColumnBinding>> &equalities) const {
	// Sparse predicate seeds avoid enumerating all pairs of members in an equality class.
	set<pair<idx_t, idx_t>> pairs;
	for (auto &equality : equalities) {
		auto left = Normalize(equality.first).table_index.index;
		auto right = Normalize(equality.second).table_index.index;
		if (left != right) {
			pairs.emplace(MinValue(left, right), MaxValue(left, right));
		}
	}
	vector<vector<CompositeKeyCoordinate>> coordinates(state->relations.size());
	for (idx_t axis = 0; axis < components.size(); axis++) {
		if (domains[axis].empty()) {
			// Complete joint factors and unknown-domain priors keep their existing owners.
			continue;
		}
		auto &bindings = components[axis].bindings;
		for (idx_t p = 0; p < bindings.size(); p++) {
			auto relation = bindings[p].table_index;
			if ((p > 0 && bindings[p - 1].table_index == relation) ||
			    (p + 1 < bindings.size() && bindings[p + 1].table_index == relation) ||
			    !domains[axis][p].has_usable_domain || domains[axis][p].total.IsZero() ||
			    domains[axis][p].current.IsZero()) {
				continue;
			}
			coordinates[relation.index].push_back({axis, p});
		}
	}
	vector<CompositeKeyAdjustment> adjustments;
	vector<idx_t> uses(components.size(), 0);
	for (auto &pair : pairs) {
		auto &left = coordinates[pair.first];
		auto &right = coordinates[pair.second];
		if (left.size() < 2 || right.size() < 2) {
			continue;
		}
		vector<idx_t> axes;
		vector<ColumnBinding> left_columns, right_columns;
		CardinalityValue denominator(1), largest(0), independent(1), strongest(1);
		idx_t l = 0, r = 0;
		while (l < left.size() && r < right.size()) {
			if (left[l].component < right[r].component) {
				l++;
				continue;
			}
			if (right[r].component < left[l].component) {
				r++;
				continue;
			}
			auto axis = left[l].component;
			auto &lhs = domains[axis][left[l].point];
			auto &rhs = domains[axis][right[r].point];
			auto domain = MaxValue(lhs.total, rhs.total);
			denominator *= domain;
			largest = MaxValue(largest, domain);
			CardinalityValue factor;
			Intersect({lhs, rhs}, factor);
			independent *= factor;
			strongest = MinValue(strongest, factor);
			axes.push_back(axis);
			left_columns.push_back(components[axis].bindings[left[l++].point]);
			right_columns.push_back(components[axis].bindings[right[r++].point]);
		}
		if (axes.size() < 2 || independent.IsZero()) {
			continue;
		}
		auto lhs_population = OriginalProducerPopulation(left_columns);
		auto rhs_population = OriginalProducerPopulation(right_columns);
		if (!lhs_population || !rhs_population) {
			continue;
		}
		auto population = MinValue(*lhs_population, *rhs_population);
		// Legacy key-like guard: broad fact-to-fact joins keep independent coordinate factors.
		if (population.IsZero() || largest > population || population > CardinalityValue(8) * largest) {
			continue;
		}
		// Correct only the pair's independent denominator. Existing filter retention and the
		// scalar contributions of other participants remain in the original estimate.
		auto multiplier = MinValue(denominator / MinValue(denominator, population), strongest / independent);
		if (multiplier <= CardinalityValue(1)) {
			continue;
		}
		for (auto axis : axes) {
			uses[axis]++;
		}
		adjustments.push_back({std::move(axes), multiplier});
	}
	CardinalityValue correction(1);
	for (auto &adjustment : adjustments) {
		bool competing = false;
		for (auto axis : adjustment.components) {
			competing |= uses[axis] > 1;
		}
		if (!competing) {
			correction *= adjustment.multiplier;
		}
	}
	// This is a cost prior, not joint NDV, uniqueness or membership-coverage evidence.
	return correction;
}

} // namespace duckdb
