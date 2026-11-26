#include "duckdb/optimizer/relation_statistics/equality_cardinality.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"
#include "duckdb/common/unordered_set.hpp"

#include <algorithm>
#include "duckdb/common/map.hpp"
#include "duckdb/common/set.hpp"
#include <tuple>

namespace duckdb {

struct EqualityDisjointSets {
	vector<idx_t> parents;

	idx_t Add() {
		auto result = parents.size();
		parents.push_back(result);
		return result;
	}

	idx_t Find(idx_t value) {
		if (parents[value] != value) {
			parents[value] = Find(parents[value]);
		}
		return parents[value];
	}

	void Merge(idx_t a, idx_t b) {
		a = Find(a);
		b = Find(b);
		if (a != b) {
			parents[MaxValue(a, b)] = MinValue(a, b);
		}
	}
};

struct EqualityTupleRecord {
	idx_t relation;
	vector<idx_t> columns;
	SupportIdentity identity;
};

struct EqualityCardinalityState {
	explicit EqualityCardinalityState(vector<RelationStats> &relations) : relations(relations) {
	}

	vector<RelationStats> &relations;
	column_binding_map_t<vector<JoinColumnSources>> sources;
	vector<idx_t> offsets;
	vector<ColumnBinding> bindings;
	EqualityDisjointSets aliases;
	idx_t BindingIndex(ColumnBinding binding) const {
		D_ASSERT(binding.table_index.index < relations.size());
		D_ASSERT(binding.column_index.GetIndex() < relations[binding.table_index.index].columns.size());
		return offsets[binding.table_index.index] + binding.column_index.GetIndex();
	}

	ColumnBinding Normalize(ColumnBinding binding) {
		return bindings[aliases.Find(BindingIndex(binding))];
	}

	support_identities_t Orient(const support_identities_t &identities, const vector<idx_t> &columns,
	                            const vector<idx_t> &ordered_columns) const;
	support_identities_t MergeIdentities(const vector<SupportIdentity> &identities) const;
	void NormalizeEvidence();
};

support_identities_t EqualityCardinalityState::MergeIdentities(const vector<SupportIdentity> &identities) const {
	// Retain direct producer coordinates only. Local aliases never publish an equivalence
	// between producers or coordinates to other references of the same producer.
	map<const SupportOrigin *, set<vector<idx_t>>> seen;
	auto result = make_shared_ptr<vector<SupportIdentity>>();
	for (auto &identity : identities) {
		if (seen[identity.origin.get()].insert(identity.coordinates).second) {
			result->push_back(identity);
		}
	}
	return result->empty() ? nullptr : result;
}

support_identities_t EqualityCardinalityState::Orient(const support_identities_t &identities,
                                                      const vector<idx_t> &columns,
                                                      const vector<idx_t> &ordered_columns) const {
	if (!identities) {
		return nullptr;
	}
	vector<SupportIdentity> result;
	for (auto &identity : *identities) {
		SupportIdentity oriented {identity.origin, {}};
		for (auto column : ordered_columns) {
			auto position = std::find(columns.begin(), columns.end(), column);
			if (position == columns.end()) {
				break;
			}
			oriented.coordinates.push_back(identity.coordinates[position - columns.begin()]);
		}
		if (oriented.coordinates.size() == ordered_columns.size()) {
			result.push_back(std::move(oriented));
		}
	}
	return MergeIdentities(result);
}

static void AddEqualityTupleRecord(vector<EqualityTupleRecord> &records, idx_t relation, const vector<idx_t> &columns,
                                   const SupportIdentity &identity) {
	D_ASSERT(columns.size() == identity.coordinates.size());
	vector<pair<idx_t, idx_t>> axes;
	for (idx_t i = 0; i < columns.size(); i++) {
		axes.emplace_back(columns[i], identity.coordinates[i]);
	}
	std::sort(axes.begin(), axes.end());
	axes.erase(std::unique(axes.begin(), axes.end()), axes.end());
	EqualityTupleRecord record {relation, {}, {identity.origin, {}}};
	for (auto &axis : axes) {
		record.columns.push_back(axis.first);
		record.identity.coordinates.push_back(axis.second);
	}
	auto same = [&](const EqualityTupleRecord &other) {
		return record.relation == other.relation && record.columns == other.columns &&
		       record.identity.origin == other.identity.origin &&
		       record.identity.coordinates == other.identity.coordinates;
	};
	if (std::none_of(records.begin(), records.end(), same)) {
		records.push_back(std::move(record));
	}
}

static void AddEqualityTupleRecords(vector<EqualityTupleRecord> &records, idx_t relation, const RelationStats &stats) {
	for (auto &joint : stats.joint_stats) {
		if (joint.identities) {
			for (auto &identity : *joint.identities) {
				AddEqualityTupleRecord(records, relation, joint.columns, identity);
			}
		}
	}
	if (stats.tuple_identities) {
		for (auto &tuple : *stats.tuple_identities) {
			AddEqualityTupleRecord(records, relation, tuple.columns, tuple.identity);
		}
	}
}

void EqualityCardinalityState::NormalizeEvidence() {
	vector<EqualityTupleRecord> records;
	for (idx_t r = 0; r < relations.size(); r++) {
		auto &stats = relations[r];
		offsets.push_back(bindings.size());
		for (idx_t c = 0; c < stats.columns.size(); c++) {
			bindings.emplace_back(TableIndex(r), ProjectionIndex(c));
			aliases.Add();
		}
		for (auto &group : stats.equivalent_columns) {
			for (auto column : group) {
				aliases.Merge(offsets[r] + group[0], offsets[r] + column);
			}
		}
		AddEqualityTupleRecords(records, r, stats);
	}
	// Roots are the smallest binding in each class. Descending insertion keeps members in binding order.
	vector<idx_t> next_alias(bindings.size(), DConstants::INVALID_INDEX);
	for (idx_t i = bindings.size(); i > 0; i--) {
		auto column = i - 1;
		auto root = aliases.Find(column);
		D_ASSERT(bindings[root].table_index == bindings[column].table_index);
		if (root != column) {
			next_alias[column] = next_alias[root];
			next_alias[root] = column;
		}
	}
	for (idx_t r = 0; r < relations.size(); r++) {
		auto &stats = relations[r];
		stats.equivalent_columns.clear();
		for (idx_t root = offsets[r]; root < offsets[r] + stats.columns.size(); root++) {
			if (aliases.Find(root) != root) {
				continue;
			}
			if (next_alias[root] != DConstants::INVALID_INDEX) {
				vector<idx_t> columns;
				for (auto index = root; index != DConstants::INVALID_INDEX; index = next_alias[index]) {
					columns.push_back(index - offsets[r]);
				}
				stats.equivalent_columns.push_back(std::move(columns));
			}
			vector<SupportIdentity> identities;
			auto total = stats.columns[root - offsets[r]].total_domain;
			auto current = stats.columns[root - offsets[r]].current_domain;
			optional_idx population_bound;
			bool can_have_null = true;
			for (auto index = root; index != DConstants::INVALID_INDEX; index = next_alias[index]) {
				auto c = index - offsets[r];
				auto &column = stats.columns[c];
				can_have_null &= column.can_have_null;
				total = DistinctCount::ReconcilePopulation(total, column.total_domain);
				current = DistinctCount::ReconcilePopulation(current, column.current_domain);
				auto population = column.PopulationBound();
				if (population.IsValid()) {
					population_bound = population_bound.IsValid()
					                       ? MinValue(population_bound.GetIndex(), population.GetIndex())
					                       : population;
				}
				if (column.support_identities) {
					for (auto &identity : *column.support_identities) {
						identities.push_back(identity);
					}
				}
				// A tuple proof supplies scalar projections. It cannot be reconstructed in the opposite direction.
				for (auto &record : records) {
					if (record.relation != r) {
						continue;
					}
					for (idx_t i = 0; i < record.columns.size(); i++) {
						if (aliases.Find(offsets[r] + record.columns[i]) == root) {
							identities.push_back(
							    SupportIdentity {record.identity.origin, {record.identity.coordinates[i]}});
						}
					}
				}
			}
			auto shared = MergeIdentities(identities);
			for (auto index = root; index != DConstants::INVALID_INDEX; index = next_alias[index]) {
				auto c = index - offsets[r];
				stats.columns[c].total_domain = total;
				stats.columns[c].current_domain = current;
				stats.columns[c].current_domain.distinct_count =
				    MinValue(MinValue(current.distinct_count, total.distinct_count), stats.cardinality);
				stats.columns[c].support_identities = shared;
				stats.columns[c].can_have_null = can_have_null;
				if (population_bound.IsValid()) {
					stats.columns[c].TightenPopulationBound(population_bound.GetIndex());
				}
			}
		}
	}
	for (idx_t r = 0; r < relations.size(); r++) {
		auto &stats = relations[r];
		for (auto &key : stats.unique_keys) {
			for (auto &column : key) {
				column = Normalize(ColumnBinding(TableIndex(r), ProjectionIndex(column))).column_index.GetIndex();
			}
			std::sort(key.begin(), key.end());
			key.erase(std::unique(key.begin(), key.end()), key.end());
		}
		std::sort(stats.unique_keys.begin(), stats.unique_keys.end());
		stats.unique_keys.erase(std::unique(stats.unique_keys.begin(), stats.unique_keys.end()),
		                        stats.unique_keys.end());
		vector<RelationJointStats> normalized;
		for (auto &joint : stats.joint_stats) {
			vector<pair<idx_t, idx_t>> axes;
			for (idx_t i = 0; i < joint.columns.size(); i++) {
				auto binding = Normalize(ColumnBinding(TableIndex(r), ProjectionIndex(joint.columns[i])));
				axes.emplace_back(binding.column_index.GetIndex(), i);
			}
			std::sort(axes.begin(), axes.end());
			auto ids = make_shared_ptr<vector<SupportIdentity>>();
			if (joint.identities) {
				for (auto &identity : *joint.identities) {
					SupportIdentity oriented {identity.origin, {}};
					optional_idx last;
					for (auto &axis : axes) {
						if (!last.IsValid() || last.GetIndex() != axis.first) {
							oriented.coordinates.push_back(identity.coordinates[axis.second]);
							last = optional_idx(axis.first);
						}
					}
					ids->push_back(std::move(oriented));
				}
			}
			joint.columns.clear();
			for (auto &axis : axes) {
				if (joint.columns.empty() || joint.columns.back() != axis.first) {
					joint.columns.push_back(axis.first);
				}
			}
			joint.identities = ids->empty() ? nullptr : ids;
			auto old = std::find_if(normalized.begin(), normalized.end(),
			                        [&](const RelationJointStats &entry) { return entry.columns == joint.columns; });
			for (auto &column : joint.dependent_columns) {
				column = Normalize(ColumnBinding(TableIndex(r), ProjectionIndex(column))).column_index.GetIndex();
			}
			if (stats.HasUniqueKey(joint.columns)) {
				for (idx_t c = 0; c < stats.columns.size(); c++) {
					joint.dependent_columns.push_back(
					    Normalize(ColumnBinding(TableIndex(r), ProjectionIndex(c))).column_index.GetIndex());
				}
			}
			std::sort(joint.dependent_columns.begin(), joint.dependent_columns.end());
			joint.dependent_columns.erase(std::unique(joint.dependent_columns.begin(), joint.dependent_columns.end()),
			                              joint.dependent_columns.end());
			if (old == normalized.end()) {
				normalized.push_back(joint);
			} else {
				old->total_domain = DistinctCount::ReconcilePopulation(old->total_domain, joint.total_domain);
				old->current_domain = DistinctCount::ReconcilePopulation(old->current_domain, joint.current_domain);
				if (old->identities) {
					ids->insert(ids->end(), old->identities->begin(), old->identities->end());
				}
				old->identities = ids->empty() ? nullptr : ids;
				old->dependent_columns.insert(old->dependent_columns.end(), joint.dependent_columns.begin(),
				                              joint.dependent_columns.end());
				std::sort(old->dependent_columns.begin(), old->dependent_columns.end());
				old->dependent_columns.erase(std::unique(old->dependent_columns.begin(), old->dependent_columns.end()),
				                             old->dependent_columns.end());
			}
		}
		stats.joint_stats = std::move(normalized);
		for (idx_t root = offsets[r]; root < offsets[r] + stats.columns.size(); root++) {
			if (aliases.Find(root) != root) {
				continue;
			}
			auto column = root - offsets[r];
			auto &scalar = stats.columns[column];
			auto population =
			    std::find_if(stats.joint_stats.begin(), stats.joint_stats.end(), [&](const RelationJointStats &joint) {
				    return joint.columns.size() == 1 && joint.columns[0] == column;
			    });
			bool unique = stats.HasUniqueKey({column});
			if (!unique && population == stats.joint_stats.end()) {
				continue;
			}
			auto total = scalar.total_domain;
			auto current = scalar.current_domain;
			if (population != stats.joint_stats.end()) {
				// Retained singleton populations take precedence over scalar approximations at equal
				// usability. An unknown singleton cannot replace an actual usable scalar alternative.
				total = total.has_usable_domain == population->total_domain.has_usable_domain
				            ? population->total_domain
				            : DistinctCount::ReconcilePopulation(total, population->total_domain);
				current = current.has_usable_domain == population->current_domain.has_usable_domain
				              ? population->current_domain
				              : DistinctCount::ReconcilePopulation(current, population->current_domain);
				total.source = DistinctCount::CombineSources(scalar.total_domain.source, total.source);
			}
			// A current unique key has one distinct tuple per estimated row. Its original population is
			// independent of that current row count and survives filtering and loss/restoration of uniqueness.
			current.distinct_count =
			    unique ? stats.cardinality
			           : MinValue(current.distinct_count, MinValue(total.distinct_count, stats.cardinality));
			total.distinct_count = MaxValue(total.distinct_count, current.distinct_count);
			if (unique) {
				total.has_usable_domain = true;
				current.has_usable_domain = true;
			}
			if (unique &&
			    (total.source == DistinctCountSource::CARDINALITY || total.source == DistinctCountSource::MIN_MAX)) {
				// A captured key population is no longer a missing-NDV row-count fallback.
				total.source = DistinctCountSource::DERIVED;
			}
			for (auto index = root; index != DConstants::INVALID_INDEX; index = next_alias[index]) {
				auto alias = index - offsets[r];
				stats.columns[alias].total_domain = total;
				stats.columns[alias].current_domain = current;
			}
			if (population != stats.joint_stats.end()) {
				population->total_domain = total;
				population->current_domain = current;
			}
		}
		auto tuples = make_shared_ptr<vector<RelationTupleIdentity>>();
		for (auto &record : records) {
			if (record.relation != r) {
				continue;
			}
			map<idx_t, idx_t> axes;
			for (idx_t i = 0; i < record.columns.size(); i++) {
				auto column =
				    Normalize(ColumnBinding(TableIndex(r), ProjectionIndex(record.columns[i]))).column_index.GetIndex();
				axes.emplace(column, record.identity.coordinates[i]);
			}
			RelationTupleIdentity tuple {{}, {record.identity.origin, {}}};
			for (auto &axis : axes) {
				tuple.columns.push_back(axis.first);
				tuple.identity.coordinates.push_back(axis.second);
			}
			tuples->push_back(std::move(tuple));
		}
		stats.tuple_identities = tuples->empty() ? nullptr : tuples;
	}
}

EqualityCardinalityModel::EqualityCardinalityModel(vector<RelationStats> &relations,
                                                   const join_column_sources_t &sources)
    : state(make_uniq<EqualityCardinalityState>(relations)) {
	state->NormalizeEvidence();
	// Keep every alias's actual observations; source IDs deduplicate provenance during factor-local selection.
	for (auto &entry : sources) {
		state->sources[Normalize(entry.first)].push_back(entry.second);
	}
}

EqualityCardinalityModel::~EqualityCardinalityModel() {
}

ColumnBinding EqualityCardinalityModel::Normalize(ColumnBinding binding) const {
	return state->Normalize(binding);
}

vector<EqualityComponent>
EqualityCardinalityModel::Components(const vector<pair<ColumnBinding, ColumnBinding>> &equalities) const {
	map<idx_t, idx_t> indexes;
	EqualityDisjointSets components;
	auto index = [&](ColumnBinding binding) {
		auto normalized = state->aliases.Find(state->BindingIndex(binding));
		auto entry = indexes.emplace(normalized, components.parents.size());
		if (entry.second) {
			components.Add();
		}
		return entry.first->second;
	};
	for (auto &equality : equalities) {
		auto left = index(equality.first);
		auto right = index(equality.second);
		components.Merge(left, right);
	}
	map<idx_t, vector<ColumnBinding>> groups;
	for (auto &entry : indexes) {
		groups[components.Find(entry.second)].push_back(state->bindings[entry.first]);
	}
	vector<EqualityComponent> result;
	for (auto &group : groups) {
		result.push_back({std::move(group.second)});
	}
	std::sort(result.begin(), result.end(), [&](const EqualityComponent &a, const EqualityComponent &b) {
		return state->BindingIndex(a.bindings[0]) < state->BindingIndex(b.bindings[0]);
	});
	return result;
}

EqualitySupport EqualityCardinalityModel::ColumnSupport(ColumnBinding binding) const {
	binding = Normalize(binding);
	auto &column = state->relations[binding.table_index.index].columns[binding.column_index.GetIndex()];
	return {column.total_domain.distinct_count, column.current_domain.distinct_count, column.support_identities,
	        column.total_domain.has_usable_domain};
}

static void ReconcileSupport(EqualitySupport &point, const EqualitySupport &other) {
	if (point.has_usable_domain != other.has_usable_domain) {
		if (other.has_usable_domain) {
			point = other;
		}
		return;
	}
	point.total = MinValue(point.total, other.total);
	point.current = MinValue(point.current, other.current);
}

EqualitySupport EqualityCardinalityModel::Intersect(const vector<EqualitySupport> &supports,
                                                    CardinalityValue &factor) const {
	factor = 1;
	if (supports.empty()) {
		return {1, 1, nullptr};
	}
	EqualityDisjointSets groups;
	map<const SupportOrigin *, map<vector<idx_t>, idx_t>> identities;
	for (idx_t i = 0; i < supports.size(); i++) {
		groups.Add();
		if (!supports[i].identities) {
			continue;
		}
		for (auto &identity : *supports[i].identities) {
			auto entry = identities[identity.origin.get()].emplace(identity.coordinates, i);
			if (!entry.second) {
				groups.Merge(i, entry.first->second);
			}
		}
	}
	map<idx_t, EqualitySupport> points;
	for (idx_t i = 0; i < supports.size(); i++) {
		auto entry = points.emplace(groups.Find(i), supports[i]);
		if (!entry.second) {
			ReconcileSupport(entry.first->second, supports[i]);
		}
	}
	auto total = points.begin()->second.total;
	CardinalityValue retention(1), denominator(1);
	bool usable = std::all_of(points.begin(), points.end(), [](const pair<const idx_t, EqualitySupport> &entry) {
		return entry.second.has_usable_domain;
	});
	for (auto &entry : points) {
		auto &point = entry.second;
		total = MinValue(total, point.total);
		if (point.current.IsZero() || point.total.IsZero()) {
			factor = supports.size() > 1 ? CardinalityValue(0) : CardinalityValue(1);
			return {total, 0, nullptr, usable};
		}
		retention *= point.current / point.total;
	}
	auto overlap = total * retention;
	for (idx_t i = 0; i < supports.size(); i++) {
		denominator *= points.at(groups.Find(i)).current;
	}
	if (supports.size() > 1) {
		factor = overlap / denominator;
	}
	// A statistical full overlap does not establish support identity.
	return {total, overlap, points.size() == 1 ? points.begin()->second.identities : nullptr, usable};
}

optional<EqualitySupport> EqualityCardinalityModel::JointSupport(idx_t relation,
                                                                 const vector<idx_t> &ordered_columns) const {
	auto &stats = state->relations[relation];
	auto ordered_axes = ordered_columns;
	for (auto &column : ordered_axes) {
		column = Normalize(ColumnBinding(TableIndex(relation), ProjectionIndex(column))).column_index.GetIndex();
	}
	auto axes = ordered_axes;
	std::sort(axes.begin(), axes.end());
	axes.erase(std::unique(axes.begin(), axes.end()), axes.end());
	vector<SupportIdentity> identities;
	if (stats.tuple_identities) {
		for (auto &tuple : *stats.tuple_identities) {
			auto source = make_shared_ptr<vector<SupportIdentity>>();
			source->push_back(tuple.identity);
			auto oriented = state->Orient(source, tuple.columns, ordered_axes);
			if (oriented) {
				identities.insert(identities.end(), oriented->begin(), oriented->end());
			}
		}
	}
	for (auto &joint : stats.joint_stats) {
		if (joint.columns == axes) {
			auto oriented = state->Orient(joint.identities, joint.columns, ordered_axes);
			if (oriented) {
				identities.insert(identities.end(), oriented->begin(), oriented->end());
			}
			return EqualitySupport {
			    joint.total_domain.distinct_count,
			    MinValue(MinValue(joint.current_domain.distinct_count, joint.total_domain.distinct_count),
			             stats.cardinality),
			    state->MergeIdentities(identities), joint.total_domain.has_usable_domain || stats.HasUniqueKey(axes)};
		}
	}
	optional<DistinctCount> total, current;
	for (auto &joint : stats.joint_stats) {
		if (joint.dependent_columns.empty() ||
		    !std::includes(axes.begin(), axes.end(), joint.columns.begin(), joint.columns.end()) ||
		    !std::includes(joint.dependent_columns.begin(), joint.dependent_columns.end(), axes.begin(), axes.end())) {
			continue;
		}
		total = total ? DistinctCount::ReconcilePopulation(*total, joint.total_domain) : joint.total_domain;
		current = current ? DistinctCount::ReconcilePopulation(*current, joint.current_domain) : joint.current_domain;
	}
	for (auto &key : stats.unique_keys) {
		if (key.empty() || !std::includes(axes.begin(), axes.end(), key.begin(), key.end())) {
			continue;
		}
		optional<DistinctCount> population;
		for (auto &joint : stats.joint_stats) {
			if (joint.columns != key) {
				continue;
			}
			population = joint.total_domain;
			auto oriented = state->Orient(joint.identities, joint.columns, ordered_axes);
			if (oriented) {
				identities.insert(identities.end(), oriented->begin(), oriented->end());
			}
			break;
		}
		if (!population && key.size() == 1) {
			population = stats.columns[key[0]].total_domain;
		}
		if (population) {
			population->has_usable_domain = true;
			total = total ? DistinctCount::ReconcilePopulation(*total, *population) : *population;
			current = DistinctCount(stats.cardinality, DistinctCountSource::CARDINALITY, true);
		}
	}
	if (!total) {
		return {};
	}
	D_ASSERT(current);
	auto count = MinValue(current->distinct_count, stats.cardinality);
	return EqualitySupport {MaxValue(total->distinct_count, count), count, state->MergeIdentities(identities),
	                        total->has_usable_domain};
}

struct EqualityFactor {
	vector<idx_t> components;
	EqualitySupport support;
	map<idx_t, EqualitySupport> source_joints;
};

static EqualitySupport SupportWithinTuple(EqualitySupport support, const EqualitySupport &tuple) {
	support.total = MinValue(support.total, tuple.total);
	support.current = MinValue(MinValue(support.current, tuple.current), support.total);
	return support;
}

EqualitySupport EqualityCardinalityModel::IntersectJoint(const vector<EqualitySupport> &joint_points,
                                                         const vector<vector<EqualitySupport>> &coordinates,
                                                         CardinalityValue &factor) const {
	auto support = Intersect(joint_points, factor);
	auto total = support.total;
	auto current = support.current;
	for (auto points : coordinates) {
		D_ASSERT(points.size() == joint_points.size());
		if (!std::all_of(points.begin(), points.end(),
		                 [](const EqualitySupport &point) { return point.has_usable_domain; })) {
			// A guessed marginal cannot restrict independently supplied joint evidence.
			continue;
		}
		for (idx_t p = 0; p < points.size(); p++) {
			points[p] = SupportWithinTuple(points[p], joint_points[p]);
		}
		CardinalityValue ignored;
		auto coordinate = Intersect(points, ignored);
		if (!coordinate.has_usable_domain) {
			continue;
		}
		for (idx_t p = 0; p < points.size(); p++) {
			// A complete bundle replaces scalar factors, but known coordinates still restrict compatible tuples.
			auto total_fraction = points[p].total.IsZero()
			                          ? CardinalityValue(0)
			                          : MinValue(CardinalityValue(1), coordinate.total / points[p].total);
			auto current_fraction = points[p].current.IsZero()
			                            ? CardinalityValue(0)
			                            : MinValue(CardinalityValue(1), coordinate.current / points[p].current);
			total = MinValue(total, joint_points[p].total * total_fraction);
			current = MinValue(current, joint_points[p].current * current_fraction);
		}
	}
	if (!support.total.IsZero()) {
		current = MinValue(current, support.current * (total / support.total));
	}
	if (!support.current.IsZero()) {
		factor *= current / support.current;
	}
	support.total = total;
	support.current = current;
	return support;
}

struct EqualityCandidateAxes {
	vector<idx_t> axes;
	bool exact;
};

static vector<EqualityCandidateAxes> FindJointCandidates(const vector<RelationStats> &relations,
                                                         const vector<EqualityComponent> &components) {
	// Only complete, one-binding-per-participant components can share a joint bundle.
	map<vector<idx_t>, vector<idx_t>> buckets;
	for (idx_t i = 0; i < components.size(); i++) {
		vector<idx_t> participants;
		for (auto &binding : components[i].bindings) {
			participants.push_back(binding.table_index.index);
		}
		if (std::adjacent_find(participants.begin(), participants.end()) == participants.end()) {
			buckets[participants].push_back(i);
		}
	}
	vector<EqualityCandidateAxes> candidates;
	for (auto &bucket : buckets) {
		candidates.push_back({bucket.second, false});
		// Retained determinant and dependency axes supply bounded additional candidates. Never enumerate subsets.
		for (auto relation : bucket.first) {
			for (auto &joint : relations[relation].joint_stats) {
				for (bool exact : {true, false}) {
					auto &columns = exact ? joint.columns : joint.dependent_columns;
					vector<idx_t> axes;
					for (auto axis : bucket.second) {
						for (auto &binding : components[axis].bindings) {
							if (binding.table_index.index == relation &&
							    std::binary_search(columns.begin(), columns.end(), binding.column_index.GetIndex())) {
								axes.push_back(axis);
							}
						}
					}
					candidates.push_back({std::move(axes), exact});
				}
			}
		}
	}
	return candidates;
}

vector<EqualityFactor> EqualityCardinalityModel::BuildFactors(const vector<EqualityComponent> &components) const {
	map<vector<idx_t>, vector<EqualitySupport>> candidates;
	auto add_candidate = [&](const vector<idx_t> &axes, bool exact) {
		if (axes.size() < 2) {
			return;
		}
		map<idx_t, vector<idx_t>> columns;
		for (auto axis : axes) {
			for (auto &binding : components[axis].bindings) {
				columns[binding.table_index.index].push_back(binding.column_index.GetIndex());
			}
		}
		vector<EqualitySupport> points;
		for (auto &entry : columns) {
			if (entry.second.size() != axes.size()) {
				return;
			}
			if (exact) {
				auto sorted = entry.second;
				std::sort(sorted.begin(), sorted.end());
				auto &joints = state->relations[entry.first].joint_stats;
				if (std::none_of(joints.begin(), joints.end(),
				                 [&](const RelationJointStats &joint) { return joint.columns == sorted; })) {
					return;
				}
			}
			auto point = JointSupport(entry.first, entry.second);
			if (!point || !point->has_usable_domain) {
				return;
			}
			points.push_back(*point);
		}
		candidates.emplace(axes, std::move(points));
	};
	for (auto &candidate : FindJointCandidates(state->relations, components)) {
		add_candidate(candidate.axes, candidate.exact);
	}
	vector<vector<idx_t>> maximal;
	for (auto &candidate : candidates) {
		bool subset = false;
		for (auto &other : candidates) {
			if (candidate.first.size() < other.first.size() &&
			    std::includes(other.first.begin(), other.first.end(), candidate.first.begin(), candidate.first.end())) {
				subset = true;
				break;
			}
		}
		if (!subset) {
			maximal.push_back(candidate.first);
		}
	}
	vector<EqualityFactor> factors;
	vector<bool> consumed(components.size(), false);
	for (idx_t i = 0; i < maximal.size(); i++) {
		bool overlapping = false;
		for (idx_t j = 0; j < maximal.size() && !overlapping; j++) {
			if (i == j) {
				continue;
			}
			for (auto axis : maximal[i]) {
				overlapping |= std::binary_search(maximal[j].begin(), maximal[j].end(), axis);
			}
		}
		if (overlapping) {
			continue;
		}
		CardinalityValue factor;
		auto &joint_points = candidates.at(maximal[i]);
		vector<vector<EqualitySupport>> coordinates;
		for (auto axis : maximal[i]) {
			vector<EqualitySupport> points;
			for (auto &binding : components[axis].bindings) {
				points.push_back(ColumnSupport(binding));
			}
			coordinates.push_back(std::move(points));
		}
		auto support = IntersectJoint(joint_points, coordinates, factor);
		EqualityFactor selected {maximal[i], std::move(support), {}};
		auto &participants = components[maximal[i][0]].bindings;
		for (idx_t p = 0; p < participants.size(); p++) {
			selected.source_joints.emplace(participants[p].table_index.index, joint_points[p]);
		}
		factors.push_back(std::move(selected));
		for (auto axis : maximal[i]) {
			consumed[axis] = true;
		}
	}
	for (idx_t i = 0; i < components.size(); i++) {
		if (consumed[i]) {
			continue;
		}
		auto &component = components[i];
		vector<EqualitySupport> points;
		for (auto &binding : component.bindings) {
			points.push_back(ColumnSupport(binding));
		}
		CardinalityValue factor;
		auto support = Intersect(points, factor);
		factors.push_back({{i}, std::move(support), {}});
	}
	return factors;
}

vector<EqualitySupport> EqualityCardinalityModel::ResolveJoinDomains(const EqualityComponent &component) const {
	JoinDomainEstimate domain;
	for (auto binding : component.bindings) {
		auto &column = state->relations[binding.table_index.index].columns[binding.column_index.GetIndex()];
		auto source = state->sources.find(binding);
		if (source == state->sources.end()) {
			domain.Update(binding, column.total_domain, nullptr);
		} else {
			for (auto &observation : source->second) {
				domain.Update(binding, column.total_domain, &observation);
			}
		}
	}
	vector<EqualitySupport> points;
	for (auto binding : component.bindings) {
		auto &original =
		    state->relations[binding.table_index.index].columns[binding.column_index.GetIndex()].total_domain;
		auto selected = original;
		if (!original.has_usable_domain || original.source == DistinctCountSource::MIN_MAX) {
			selected = domain.Resolve({binding});
			if (original.source == DistinctCountSource::MIN_MAX) {
				selected.distinct_count = MinValue(selected.distinct_count, original.distinct_count);
			}
		}
		auto point = ColumnSupport(binding);
		point.total = selected.distinct_count;
		point.current = MinValue(point.current, point.total);
		point.has_usable_domain = selected.has_usable_domain;
		points.push_back(std::move(point));
	}
	if (std::none_of(points.begin(), points.end(),
	                 [](const EqualitySupport &point) { return point.has_usable_domain; })) {
		auto scan = domain.UnknownScanBasis();
		for (auto &point : points) {
			point.total =
			    MaxValue(CardinalityValue(1),
			             scan ? MinValue(point.total, MaxValue(CardinalityValue(1),
			                                                   *scan * RelationStatisticsHelper::DEFAULT_SELECTIVITY))
			                  : point.total);
		}
	}
	return points;
}

EqualityCardinalityEstimate EqualityCardinalityModel::Estimate(const vector<EqualityComponent> &components,
                                                               const vector<idx_t> &relations,
                                                               EqualityDomainPolicy domain_policy) const {
	EqualityCardinalityEstimate result;
	auto factors = BuildFactors(components);
	auto resolve_domains = [&](const EqualityComponent &component) {
		if (domain_policy == EqualityDomainPolicy::JOIN) {
			return ResolveJoinDomains(component);
		}
		vector<EqualitySupport> points;
		for (auto binding : component.bindings) {
			auto point = ColumnSupport(binding);
			D_ASSERT(point.has_usable_domain);
			points.push_back(std::move(point));
		}
		return points;
	};
	EqualityDisjointSets blocks;
	for (idx_t r = 0; r < state->relations.size(); r++) {
		blocks.Add();
	}
	map<idx_t, CardinalityValue> priors;
	for (auto &factor : factors) {
		if (factor.components.size() > 1) {
			vector<EqualitySupport> joints;
			for (auto &entry : factor.source_joints) {
				joints.push_back(entry.second);
			}
			vector<vector<EqualitySupport>> coordinates;
			for (auto axis : factor.components) {
				auto &component = components[axis];
				if (std::all_of(component.bindings.begin(), component.bindings.end(),
				                [&](ColumnBinding binding) { return ColumnSupport(binding).has_usable_domain; })) {
					coordinates.push_back(resolve_domains(component));
				}
			}
			CardinalityValue selectivity;
			IntersectJoint(joints, coordinates, selectivity);
			result.selectivity *= selectivity;
			continue;
		}
		auto &component = components[factor.components[0]];
		auto points = resolve_domains(component);
		bool unknown = std::none_of(points.begin(), points.end(),
		                            [](const EqualitySupport &point) { return point.has_usable_domain; });
		if (!unknown) {
			CardinalityValue selectivity;
			Intersect(points, selectivity);
			result.selectivity *= selectivity;
			continue;
		}
		auto first = component.bindings.front().table_index.index;
		if (first == component.bindings.back().table_index.index) {
			// Local-only equalities belong to filter costing, including their priors. They cannot enlarge a block.
			continue;
		}
		for (idx_t i = 0; i < component.bindings.size(); i++) {
			auto relation = component.bindings[i].table_index.index;
			blocks.Merge(first, relation);
			auto entry = priors.emplace(relation, points[i].total);
			entry.first->second = MaxValue(entry.first->second, points[i].total);
		}
	}
	// Unknown coordinates share one assumed latent tuple per connected relation block. This is a cost prior,
	// not joint NDV or coverage evidence. Only fixed-prior wholly unknown closure is monotone; related informed
	// peers can change imputation/classification when components merge. Informed factors never connect blocks.
	struct UnknownBlockProduct {
		CardinalityValue minimum_domain;
		CardinalityValue domain_product;
	};
	map<idx_t, UnknownBlockProduct> products;
	for (auto &entry : priors) {
		auto block =
		    products.emplace(blocks.Find(entry.first), UnknownBlockProduct {entry.second, CardinalityValue(1)});
		block.first->second.minimum_domain = MinValue(block.first->second.minimum_domain, entry.second);
		block.first->second.domain_product *= entry.second;
	}
	for (auto &entry : products) {
		result.selectivity *= entry.second.minimum_domain / entry.second.domain_product;
	}
	map<idx_t, vector<idx_t>> covered;
	for (auto &component : components) {
		if (component.bindings.front().table_index != component.bindings.back().table_index) {
			for (auto &binding : component.bindings) {
				covered[binding.table_index.index].push_back(binding.column_index.GetIndex());
			}
		}
	}
	for (auto &entry : covered) {
		if (!state->relations[entry.first].HasUniqueKey(entry.second)) {
			continue;
		}
		CardinalityValue bound(1);
		for (auto relation : relations) {
			if (relation != entry.first) {
				bound *= state->relations[relation].cardinality;
			}
		}
		result.maximum = result.maximum ? MinValue(*result.maximum, bound) : bound;
	}
	// A tuple that determines every input cannot gain multiplicity anywhere in the region.
	// This also reconciles differing point estimates of the same complete key support.
	for (auto relation : relations) {
		if (PreservesMultiplicity(relation, relations, components)) {
			auto bound = state->relations[relation].cardinality;
			result.maximum = result.maximum ? MinValue(*result.maximum, bound) : bound;
		}
	}
	return result;
}

bool EqualityCardinalityModel::PreservesMultiplicity(idx_t relation, const vector<idx_t> &participants,
                                                     const vector<EqualityComponent> &components) const {
	for (auto participant : participants) {
		auto &input = state->relations[participant];
		if (participant != relation && input.unique_keys.empty() &&
		    (!input.max_cardinality.IsValid() || input.max_cardinality.GetIndex() > 1)) {
			return false;
		}
	}
	vector<bool> determined(state->relations.size(), false);
	determined[relation] = true;
	vector<bool> known(state->bindings.size(), false);
	bool changed;
	do {
		changed = false;
		for (auto participant : participants) {
			if (determined[participant]) {
				for (idx_t c = 0; c < state->relations[participant].columns.size(); c++) {
					known[state->aliases.Find(state->offsets[participant] + c)] = true;
				}
			}
		}
		for (auto &component : components) {
			bool has_known = false;
			for (auto &binding : component.bindings) {
				has_known |= known[state->aliases.Find(state->BindingIndex(binding))];
			}
			if (has_known) {
				for (auto &binding : component.bindings) {
					auto index = state->aliases.Find(state->BindingIndex(binding));
					changed |= !known[index];
					known[index] = true;
				}
			}
		}
		for (auto participant : participants) {
			if (determined[participant]) {
				continue;
			}
			auto &input = state->relations[participant];
			bool singleton = input.max_cardinality.IsValid() && input.max_cardinality.GetIndex() <= 1;
			vector<idx_t> columns;
			for (idx_t c = 0; c < input.columns.size(); c++) {
				if (known[state->aliases.Find(state->offsets[participant] + c)]) {
					columns.push_back(c);
				}
			}
			if (singleton || input.HasUniqueKey(columns)) {
				determined[participant] = true;
				changed = true;
			}
		}
	} while (changed);
	for (auto participant : participants) {
		if (!determined[participant]) {
			return false;
		}
	}
	return true;
}

CardinalityValue RelationStatisticsHelper::EstimateEqualitySelectionSupport(CardinalityValue current_domain,
                                                                            CardinalityValue input_rows,
                                                                            CardinalityValue direct_retention,
                                                                            CardinalityValue conditional_retention,
                                                                            EqualitySelection selection) {
	auto anti = selection == EqualitySelection::UNMATCHED;
	auto selected_domain = current_domain * direct_retention;
	auto selected_rows = input_rows * direct_retention;
	auto retained =
	    EstimateCurrentDomain(DistinctCount(selected_domain, DistinctCountSource::CARDINALITY), selected_rows,
	                          selected_rows * (anti ? 1 - conditional_retention : conditional_retention));
	return anti ? current_domain - selected_domain + retained.distinct_count : retained.distinct_count;
}

void EqualityCardinalityModel::ProjectEvidence(
    RelationStats &output, const vector<EqualityComponent> &components, const vector<EqualityProjection> &projections,
    const vector<pair<ColumnBinding, ColumnBinding>> &value_equalities) const {
	ProjectInternal(output, components, projections, components, ProjectionScope::EVIDENCE, value_equalities);
}

void EqualityCardinalityModel::Project(RelationStats &output, const vector<EqualityComponent> &components,
                                       const vector<EqualityProjection> &projections,
                                       const vector<EqualityComponent> &output_equalities,
                                       const vector<pair<ColumnBinding, ColumnBinding>> &value_equalities) const {
	ProjectInternal(output, components, projections, output_equalities, ProjectionScope::COLUMNS_AND_EVIDENCE,
	                value_equalities);
}

optional<EqualitySelectedSupport> EqualityCardinalityModel::SelectSupport(idx_t relation, const vector<idx_t> &columns,
                                                                          const vector<idx_t> &dependent_columns,
                                                                          EqualitySupport support,
                                                                          const vector<EqualityComponent> &components,
                                                                          const vector<EqualityFactor> &factors,
                                                                          const EqualityProjection &projection) const {
	if (projection.selection == EqualitySelection::ALL) {
		return EqualitySelectedSupport {std::move(support), false};
	}
	auto &input = state->relations[relation];
	CardinalityValue direct(1), conditional(1);
	bool has_matching_columns = false, determines_match = true, population_conditioned = false;
	for (auto &factor : factors) {
		auto coordinate_support = [&](ColumnBinding binding) {
			auto point = ColumnSupport(binding);
			auto joint = factor.source_joints.find(binding.table_index.index);
			return joint == factor.source_joints.end() ? point : SupportWithinTuple(point, joint->second);
		};
		vector<idx_t> axes;
		CardinalityValue source_total(1), source_current(1);
		for (auto component : factor.components) {
			for (auto &binding : components[component].bindings) {
				if (binding.table_index.index != relation) {
					continue;
				}
				axes.push_back(binding.column_index.GetIndex());
				auto point = coordinate_support(binding);
				source_total *= point.total;
				source_current *= point.current;
			}
		}
		if (axes.empty()) {
			continue;
		}
		has_matching_columns = true;
		if (factor.components.size() > 1) {
			auto &point = factor.source_joints.at(relation);
			source_total = point.total;
			source_current = point.current;
		}
		std::sort(axes.begin(), axes.end());
		auto retention = source_current.IsZero()
		                     ? CardinalityValue(0)
		                     : MinValue(CardinalityValue(1), factor.support.current / source_current);
		bool contained = std::includes(columns.begin(), columns.end(), axes.begin(), axes.end());
		bool containing = std::includes(axes.begin(), axes.end(), columns.begin(), columns.end());
		if (containing && factor.components.size() > 1) {
			// Only coordinates inside the selected tuple inherit its population ceiling.
			// An outside determinant can have more values than the dependent tuple it determines.
			support = SupportWithinTuple(support, factor.source_joints.at(relation));
		}
		bool dependent = std::includes(dependent_columns.begin(), dependent_columns.end(), axes.begin(), axes.end());
		bool overlaps = std::any_of(axes.begin(), axes.end(), [&](idx_t axis) {
			return std::binary_search(columns.begin(), columns.end(), axis);
		});
		determines_match &= contained || dependent;
		if (contained || dependent) {
			direct *= retention;
			if (projection.selection == EqualitySelection::MATCHED && (contained || containing)) {
				population_conditioned = true;
				support.total *= source_total.IsZero()
				                     ? CardinalityValue(0)
				                     : MinValue(CardinalityValue(1), factor.support.total / source_total);
				if (columns.size() == 1) {
					for (auto component : factor.components) {
						vector<EqualitySupport> points;
						bool coordinate = false;
						for (auto &binding : components[component].bindings) {
							points.push_back(coordinate_support(binding));
							coordinate |=
							    binding.table_index.index == relation && binding.column_index.GetIndex() == columns[0];
						}
						if (coordinate) {
							CardinalityValue ignored;
							support.total = MinValue(support.total, Intersect(points, ignored).total);
						}
					}
				}
			}
		} else if (containing) {
			// A selected joint bundle replaces row factors, not the overlap restrictions of its coordinates.
			// Condition the remaining bundle selection on those direct restrictions; do not count them twice.
			CardinalityValue coordinate_retention(1);
			for (auto component : factor.components) {
				vector<EqualitySupport> points;
				optional<EqualitySupport> source;
				for (auto &binding : components[component].bindings) {
					auto point = coordinate_support(binding);
					points.push_back(point);
					if (binding.table_index.index == relation &&
					    std::binary_search(columns.begin(), columns.end(), binding.column_index.GetIndex())) {
						source = point;
					}
				}
				if (!source) {
					continue;
				}
				CardinalityValue ignored;
				auto overlap = Intersect(points, ignored);
				coordinate_retention *= source->current.IsZero()
				                            ? CardinalityValue(0)
				                            : MinValue(CardinalityValue(1), overlap.current / source->current);
				if (projection.selection == EqualitySelection::MATCHED) {
					population_conditioned = true;
					support.total *= source->total.IsZero()
					                     ? CardinalityValue(0)
					                     : MinValue(CardinalityValue(1), overlap.total / source->total);
				}
			}
			direct *= coordinate_retention;
			conditional *= coordinate_retention.IsZero()
			                   ? CardinalityValue(0)
			                   : MinValue(CardinalityValue(1), retention / coordinate_retention);
		} else if (!overlaps) {
			conditional *= retention;
		} else {
			// Crossing partial tuple constraints need a conditional distribution we do not have.
			return {};
		}
	}
	if (projection.matched_rows) {
		auto fraction = input.cardinality.IsZero()
		                    ? CardinalityValue(0)
		                    : MinValue(CardinalityValue(1), *projection.matched_rows / input.cardinality);
		if (has_matching_columns && determines_match) {
			// All copies of this tuple have the same match status. Reconciled coverage removes entire tuples.
			direct = fraction;
			conditional = 1;
		} else {
			conditional = direct.IsZero() ? CardinalityValue(0) : MinValue(CardinalityValue(1), fraction / direct);
		}
	}
	support.current = RelationStatisticsHelper::EstimateEqualitySelectionSupport(
	    support.current, input.cardinality, direct, conditional, projection.selection);
	support.identities.reset();
	return EqualitySelectedSupport {std::move(support), population_conditioned};
}

void EqualityCardinalityModel::ProjectInternal(
    RelationStats &output, const vector<EqualityComponent> &components, const vector<EqualityProjection> &projections,
    const vector<EqualityComponent> &output_equalities, ProjectionScope scope,
    const vector<pair<ColumnBinding, ColumnBinding>> &value_equalities) const {
	D_ASSERT(projections.size() == state->relations.size());
	if (output.columns.empty()) {
		// No column-index evidence survives, but an omitted region can still carry inherited proof facts.
		output.unique_keys.clear();
		output.equivalent_columns.clear();
		output.value_equivalent_columns.clear();
		output.joint_stats.clear();
		output.tuple_identities.reset();
		return;
	}
	auto aliases = state->aliases;
	auto factors = BuildFactors(components);
	for (auto &component : output_equalities) {
		for (auto &binding : component.bindings) {
			aliases.Merge(state->BindingIndex(component.bindings[0]), state->BindingIndex(binding));
		}
	}
	output.unique_keys.clear();
	output.equivalent_columns.clear();
	output.value_equivalent_columns.clear();
	output.joint_stats.clear();
	map<idx_t, vector<idx_t>> outputs;
	for (idx_t i = 0; i < output.columns.size(); i++) {
		outputs[aliases.Find(state->BindingIndex(output.columns[i].binding))].push_back(i);
	}
	EqualityDisjointSets value_aliases;
	for (idx_t i = 0; i < state->bindings.size(); i++) {
		value_aliases.Add();
	}
	for (idx_t r = 0; r < state->relations.size(); r++) {
		for (auto &group : state->relations[r].value_equivalent_columns) {
			for (auto c : group) {
				value_aliases.Merge(state->offsets[r] + group[0], state->offsets[r] + c);
			}
		}
	}
	for (auto &pair : value_equalities) {
		value_aliases.Merge(state->BindingIndex(pair.first), state->BindingIndex(pair.second));
	}
	map<idx_t, vector<shared_ptr<const ColumnValueRestriction>>> restrictions;
	map<idx_t, vector<idx_t>> value_outputs;
	for (auto binding : state->bindings) {
		restrictions[value_aliases.Find(state->BindingIndex(binding))].push_back(
		    state->relations[binding.table_index.index].columns[binding.column_index.GetIndex()].value_restriction);
	}
	for (idx_t c = 0; c < output.columns.size(); c++) {
		value_outputs[value_aliases.Find(state->BindingIndex(output.columns[c].binding))].push_back(c);
	}
	if (scope == ProjectionScope::COLUMNS_AND_EVIDENCE) {
		struct SelectedColumn {
			EqualitySupport support;
			bool nullable;
			bool population_conditioned;
		};
		map<idx_t, SelectedColumn> selected_columns;
		for (auto binding : state->bindings) {
			auto root = aliases.Find(state->BindingIndex(binding));
			if (!outputs.count(root) || Normalize(binding) != binding) {
				continue;
			}
			auto r = binding.table_index.index;
			auto c = binding.column_index.GetIndex();
			auto &source = state->relations[r];
			auto &projection = projections[r];
			vector<idx_t> dependent;
			for (auto &joint : source.joint_stats) {
				if (joint.columns == vector<idx_t> {c}) {
					dependent.insert(dependent.end(), joint.dependent_columns.begin(), joint.dependent_columns.end());
				}
			}
			std::sort(dependent.begin(), dependent.end());
			auto selected = SelectSupport(r, {c}, dependent, ColumnSupport(binding), components, factors, projection);
			auto support = selected ? selected->support : ColumnSupport(binding);
			bool conditioned = selected && selected->population_conditioned;
			bool nullable = source.columns[c].can_have_null || projection.null_extended;
			auto entry = selected_columns.emplace(root, SelectedColumn {support, nullable, conditioned});
			if (!entry.second) {
				// Equal output columns share one support even when only one alias is requested.
				ReconcileSupport(entry.first->second.support, support);
				entry.first->second.nullable &= nullable;
				entry.first->second.population_conditioned |= conditioned;
			}
		}
		for (auto &column : output.columns) {
			auto binding = column.binding;
			auto r = binding.table_index.index;
			column = state->relations[r].columns[Normalize(binding).column_index.GetIndex()];
			column.binding = binding;
			auto &selected = selected_columns.at(aliases.Find(state->BindingIndex(binding)));
			column.total_domain.distinct_count = MaxValue(selected.support.total, selected.support.current);
			column.total_domain.has_usable_domain = selected.support.has_usable_domain;
			if (selected.population_conditioned) {
				column.total_domain.source = DistinctCountSource::DERIVED;
			}
			column.current_domain = DistinctCount(MinValue(selected.support.current, output.cardinality),
			                                      DistinctCountSource::CARDINALITY, selected.support.has_usable_domain);
			if (!projections[r].preserve_identity) {
				column.support_identities.reset();
			}
			column.can_have_null = selected.nullable;
		}
	}
	for (auto &entry : outputs) {
		if (entry.second.size() > 1) {
			output.equivalent_columns.push_back(entry.second);
		}
	}
	// Numeric canonical-column copies above do not change the actual value coordinate.
	for (auto &entry : value_outputs) {
		auto restriction =
		    ColumnValueRestriction::Combine(restrictions.at(entry.first), RestrictionCombination::CONJUNCTION);
		for (auto c : entry.second) {
			output.columns[c].value_restriction = restriction;
		}
		if (entry.second.size() > 1) {
			output.value_equivalent_columns.push_back(entry.second);
		}
	}
	auto project = [&](idx_t relation, const vector<idx_t> &axes, bool complete = true) -> optional<vector<idx_t>> {
		vector<idx_t> result;
		for (auto column : axes) {
			auto found = outputs.find(aliases.Find(state->offsets[relation] + column));
			if (found == outputs.end()) {
				if (complete) {
					return {};
				}
				continue;
			}
			result.push_back(found->second[0]);
		}
		std::sort(result.begin(), result.end());
		result.erase(std::unique(result.begin(), result.end()), result.end());
		return result;
	};
	auto tuples = make_shared_ptr<vector<RelationTupleIdentity>>();
	auto project_identity = [&](idx_t relation, const vector<idx_t> &axes,
	                            const SupportIdentity &identity) -> RelationTupleIdentity {
		map<idx_t, idx_t> coordinates;
		for (idx_t i = 0; i < axes.size(); i++) {
			auto found = outputs.find(aliases.Find(state->offsets[relation] + axes[i]));
			if (found != outputs.end()) {
				coordinates.emplace(found->second[0], identity.coordinates[i]);
			}
		}
		RelationTupleIdentity result {{}, {identity.origin, {}}};
		for (auto &entry : coordinates) {
			result.columns.push_back(entry.first);
			result.identity.coordinates.push_back(entry.second);
		}
		return result;
	};
	for (idx_t r = 0; r < state->relations.size(); r++) {
		auto &input = state->relations[r];
		auto &projection = projections[r];
		if (projection.preserve_keys) {
			for (auto &key : input.unique_keys) {
				auto projected = project(r, key);
				if (projected) {
					output.AddUniqueKey(std::move(*projected));
				}
			}
		}
		if (projection.preserve_identity && input.tuple_identities) {
			for (auto &tuple : *input.tuple_identities) {
				auto projected = project_identity(r, tuple.columns, tuple.identity);
				if (!projected.columns.empty()) {
					tuples->push_back(std::move(projected));
				}
			}
		}
		for (auto &joint : input.joint_stats) {
			auto projected = project(r, joint.columns);
			if (!projected || projected->empty()) {
				continue;
			}
			auto selected = SelectSupport(r, joint.columns, joint.dependent_columns,
			                              {joint.total_domain.distinct_count, joint.current_domain.distinct_count,
			                               joint.identities, joint.total_domain.has_usable_domain},
			                              components, factors, projection);
			if (!selected) {
				continue;
			}
			auto total = selected->support.total;
			auto current = MinValue(selected->support.current, output.cardinality);
			if (projected->size() == 1) {
				current = MinValue(current, output.columns[(*projected)[0]].current_domain.distinct_count);
				total = MinValue(total, output.columns[(*projected)[0]].total_domain.distinct_count);
			}
			support_identities_t identities;
			if (projection.preserve_identity && joint.identities) {
				auto projected_identities = make_shared_ptr<vector<SupportIdentity>>();
				for (auto &identity : *joint.identities) {
					auto projected_identity = project_identity(r, joint.columns, identity);
					D_ASSERT(projected_identity.columns == *projected);
					projected_identities->push_back(std::move(projected_identity.identity));
				}
				identities = projected_identities;
			}
			output.joint_stats.push_back(
			    {std::move(*projected),
			     DistinctCount(MaxValue(total, current),
			                   selected->population_conditioned ? DistinctCountSource::DERIVED
			                                                    : joint.total_domain.source,
			                   selected->support.has_usable_domain),
			     DistinctCount(current, DistinctCountSource::CARDINALITY, selected->support.has_usable_domain),
			     identities});
			if (!projection.null_extended) {
				auto dependent = project(r, joint.dependent_columns, false);
				output.joint_stats.back().dependent_columns = std::move(*dependent);
			}
		}
	}
	output.tuple_identities = tuples->empty() ? nullptr : tuples;
}

void RelationStatisticsHelper::ApplyFiniteSelection(RelationStats &stats,
                                                    const vector<ColumnValueSelection> &coordinates) {
	if (coordinates.empty()) {
		return;
	}
	RelationStats finite;
	finite.cardinality = 1;
	finite.stats_initialized = true;
	vector<idx_t> key;
	vector<EqualityComponent> components;
	for (auto &coordinate : coordinates) {
		if (coordinate.excluded && coordinate.count == 0) {
			continue;
		}
		auto axis = finite.columns.size();
		CardinalityValue count(coordinate.count);
		auto source = DistinctCountSource::EXACT;
		D_ASSERT(stats.columns[coordinate.column].total_domain.has_usable_domain);
		if (coordinate.excluded) {
			auto population = stats.columns[coordinate.column].total_domain.distinct_count;
			count = population - MinValue(population, count);
			source = DistinctCountSource::DERIVED;
		} else {
			D_ASSERT(count > 0);
		}
		ColumnBinding binding(TableIndex(1), ProjectionIndex(axis));
		finite.columns.emplace_back(binding, DistinctCount(count, source, true), Identifier());
		finite.columns.back().can_have_null = false;
		// A complement's size comes from a point estimate, not a proof about which literals occur.
		if (!coordinate.excluded) {
			finite.columns.back().TightenPopulationBound(coordinate.count);
		}
		finite.cardinality *= count;
		key.push_back(axis);
		components.push_back({{ColumnBinding(TableIndex(0), ProjectionIndex(coordinate.column)), binding}});
	}
	if (finite.columns.empty()) {
		return;
	}
	finite.AddUniqueKey(key);
	vector<RelationStats> inputs {stats, finite};
	set<vector<idx_t>> recorded;
	for (auto &candidate : FindJointCandidates(inputs, components)) {
		if (candidate.axes.size() < 2 || !recorded.insert(candidate.axes).second) {
			continue;
		}
		CardinalityValue count(1);
		auto source = DistinctCountSource::EXACT;
		for (auto axis : candidate.axes) {
			count *= inputs[1].columns[axis].total_domain.distinct_count;
			source = DistinctCount::CombineSources(source, inputs[1].columns[axis].total_domain.source);
		}
		inputs[1].joint_stats.push_back(
		    {candidate.axes, DistinctCount(count, source, true), DistinctCount(count, source, true), nullptr});
	}
	EqualityCardinalityModel model(inputs);
	// The literal candidate describes selected values, not a new observation of the source population.
	auto estimate = model.Estimate(components, {0, 1}, EqualityDomainPolicy::SOURCE_LOCAL);
	auto rows = stats.cardinality * finite.cardinality * estimate.selectivity;
	if (estimate.maximum) {
		rows = MinValue(rows, *estimate.maximum);
	}
	rows = MinValue(rows, stats.cardinality);
	if (stats.max_cardinality.IsValid()) {
		rows = MinValue(rows, CardinalityValue(stats.max_cardinality.GetIndex()));
	}
	auto result = stats;
	result.cardinality = rows;
	for (idx_t c = 0; c < result.columns.size(); c++) {
		result.columns[c].binding = ColumnBinding(TableIndex(0), ProjectionIndex(c));
	}
	vector<EqualityProjection> projections(2);
	projections[0].matched_rows = rows;
	projections[0].preserve_keys = true;
	// Finite candidates select populations but never contribute output aliases, uniqueness or support identity.
	model.Project(result, components, projections, {});
	for (idx_t c = 0; c < result.columns.size(); c++) {
		result.columns[c].binding = stats.columns[c].binding;
	}
	for (auto &coordinate : coordinates) {
		if (coordinate.excluded) {
			continue;
		}
		for (idx_t c = 0; c < result.columns.size(); c++) {
			if (model.Normalize(ColumnBinding(TableIndex(0), ProjectionIndex(c))).column_index.GetIndex() ==
			    coordinate.column) {
				result.columns[c].TightenPopulationBound(coordinate.count);
				result.columns[c].semi_anti_join_domain_evidence.TightenDomainBound(coordinate.count);
			}
		}
	}
	if (!stats.cardinality.IsZero()) {
		result.row_retention *= rows / stats.cardinality;
	}
	result.cardinality = result.GetCardinality();
	stats = std::move(result);
}

} // namespace duckdb
