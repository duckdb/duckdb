#include "duckdb/optimizer/relation_statistics/equality_cardinality_state.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/constants.hpp"

#include <algorithm>

namespace duckdb {

struct EqualityTupleRecord {
	idx_t relation;
	vector<idx_t> columns;
	SupportIdentity identity;
};

support_identities_t EqualityCardinalityState::DeduplicateIdentities(const vector<SupportIdentity> &identities) const {
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

support_identities_t EqualityCardinalityState::OrientIdentities(const support_identities_t &identities,
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
	return DeduplicateIdentities(result);
}

static void AddEqualityTupleRecord(vector<EqualityTupleRecord> &records, idx_t relation, const vector<idx_t> &columns,
                                   const SupportIdentity &identity) {
	D_ASSERT(columns.size() == identity.coordinates.size());
	vector<pair<idx_t, idx_t>> axes;
	axes.reserve(columns.size());
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

static vector<idx_t> BuildAliasChains(EqualityCardinalityState &state) {
	// Roots are the smallest binding in each class. Descending insertion keeps members in binding order.
	vector<idx_t> next_alias(state.bindings.size(), DConstants::INVALID_INDEX);
	for (idx_t i = state.bindings.size(); i > 0; i--) {
		auto column = i - 1;
		auto root = state.aliases.Find(column);
		D_ASSERT(state.bindings[root].table_index == state.bindings[column].table_index);
		if (root != column) {
			next_alias[column] = next_alias[root];
			next_alias[root] = column;
		}
	}
	return next_alias;
}

static void ReconcileAliasDomains(EqualityCardinalityState &state, idx_t r, const vector<idx_t> &next_alias,
                                  const vector<EqualityTupleRecord> &records) {
	auto &stats = state.relations[r];
	stats.equivalent_columns.clear();
	for (idx_t root = state.offsets[r]; root < state.offsets[r] + stats.columns.size(); root++) {
		if (state.aliases.Find(root) != root) {
			continue;
		}
		if (next_alias[root] != DConstants::INVALID_INDEX) {
			vector<idx_t> columns;
			for (auto index = root; index != DConstants::INVALID_INDEX; index = next_alias[index]) {
				columns.push_back(index - state.offsets[r]);
			}
			stats.equivalent_columns.push_back(std::move(columns));
		}
		vector<SupportIdentity> identities;
		auto total = stats.columns[root - state.offsets[r]].total_domain;
		auto current = stats.columns[root - state.offsets[r]].current_domain;
		optional_idx population_bound;
		bool can_have_null = true;
		for (auto index = root; index != DConstants::INVALID_INDEX; index = next_alias[index]) {
			auto c = index - state.offsets[r];
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
					if (state.aliases.Find(state.offsets[r] + record.columns[i]) == root) {
						identities.push_back(
						    SupportIdentity {record.identity.origin, {record.identity.coordinates[i]}});
					}
				}
			}
		}
		auto shared = state.DeduplicateIdentities(identities);
		for (auto index = root; index != DConstants::INVALID_INDEX; index = next_alias[index]) {
			auto c = index - state.offsets[r];
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

template <class T>
static void SortAndDeduplicate(vector<T> &values) {
	std::sort(values.begin(), values.end());
	values.erase(std::unique(values.begin(), values.end()), values.end());
}

static void NormalizeUniqueKeys(EqualityCardinalityState &state, idx_t r) {
	auto &stats = state.relations[r];
	for (auto &key : stats.unique_keys) {
		for (auto &column : key) {
			column = state.NormalizeColumn(r, column);
		}
		SortAndDeduplicate(key);
	}
	SortAndDeduplicate(stats.unique_keys);
}

static void NormalizeJointStats(EqualityCardinalityState &state, idx_t r) {
	auto &stats = state.relations[r];
	vector<RelationJointStats> normalized;
	for (auto &joint : stats.joint_stats) {
		vector<pair<idx_t, idx_t>> axes;
		axes.reserve(joint.columns.size());
		for (idx_t i = 0; i < joint.columns.size(); i++) {
			axes.emplace_back(state.NormalizeColumn(r, joint.columns[i]), i);
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
			column = state.NormalizeColumn(r, column);
		}
		if (stats.HasUniqueKey(joint.columns)) {
			for (idx_t c = 0; c < stats.columns.size(); c++) {
				joint.dependent_columns.push_back(state.NormalizeColumn(r, c));
			}
		}
		SortAndDeduplicate(joint.dependent_columns);
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
			SortAndDeduplicate(old->dependent_columns);
		}
	}
	stats.joint_stats = std::move(normalized);
}

static void ReconcileSingletonPopulations(EqualityCardinalityState &state, idx_t r, const vector<idx_t> &next_alias) {
	auto &stats = state.relations[r];
	for (idx_t root = state.offsets[r]; root < state.offsets[r] + stats.columns.size(); root++) {
		if (state.aliases.Find(root) != root) {
			continue;
		}
		auto column = root - state.offsets[r];
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
			auto alias = index - state.offsets[r];
			stats.columns[alias].total_domain = total;
			stats.columns[alias].current_domain = current;
		}
		if (population != stats.joint_stats.end()) {
			population->total_domain = total;
			population->current_domain = current;
		}
	}
}

static void NormalizeTupleIdentities(EqualityCardinalityState &state, idx_t r,
                                     const vector<EqualityTupleRecord> &records) {
	auto &stats = state.relations[r];
	auto tuples = make_shared_ptr<vector<RelationTupleIdentity>>();
	for (auto &record : records) {
		if (record.relation != r) {
			continue;
		}
		map<idx_t, idx_t> axes;
		for (idx_t i = 0; i < record.columns.size(); i++) {
			auto column = state.NormalizeColumn(r, record.columns[i]);
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

void EqualityCardinalityState::NormalizeEvidence() {
	vector<EqualityTupleRecord> records;
	offsets.reserve(relations.size());
	idx_t column_count = 0;
	for (auto &relation : relations) {
		column_count += relation.columns.size();
	}
	bindings.reserve(column_count);
	aliases.parents.reserve(column_count);
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
	auto next_alias = BuildAliasChains(*this);
	for (idx_t r = 0; r < relations.size(); r++) {
		ReconcileAliasDomains(*this, r, next_alias, records);
	}
	for (idx_t r = 0; r < relations.size(); r++) {
		NormalizeUniqueKeys(*this, r);
		NormalizeJointStats(*this, r);
		ReconcileSingletonPopulations(*this, r, next_alias);
		NormalizeTupleIdentities(*this, r, records);
	}
}

} // namespace duckdb
