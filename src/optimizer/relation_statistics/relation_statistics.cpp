#include "duckdb/optimizer/relation_statistics/relation_statistics.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/optimizer/distinctness.hpp"

#include <algorithm>

namespace duckdb {

ColumnValueRestriction::ColumnValueRestriction(value_set_t values, bool excluded)
    : values(std::move(values)), excluded(excluded) {
}

shared_ptr<const ColumnValueRestriction> ColumnValueRestriction::Create(value_set_t values, bool excluded) {
	if (excluded && values.empty()) {
		return nullptr;
	}
	return make_shared_ptr<ColumnValueRestriction>(std::move(values), excluded);
}

shared_ptr<const ColumnValueRestriction>
ColumnValueRestriction::Combine(const vector<shared_ptr<const ColumnValueRestriction>> &inputs, bool conjunction) {
	bool excluded = conjunction;
	for (auto &input : inputs) {
		if (!input) {
			if (!conjunction) {
				return nullptr;
			}
		} else if (input->excluded != conjunction) {
			excluded = !conjunction;
		}
	}
	auto intersect = conjunction != excluded;
	shared_ptr<const ColumnValueRestriction> seed;
	for (auto &input : inputs) {
		if (input && input->excluded == excluded &&
		    (!seed ||
		     (intersect ? input->values.size() < seed->values.size() : input->values.size() > seed->values.size()))) {
			seed = input;
		}
	}
	if (!seed) {
		return conjunction ? nullptr : Create({}, false);
	}
	unique_ptr<value_set_t> result;
	for (auto &input : inputs) {
		if (!input || input == seed) {
			continue;
		}
		auto &current = result ? *result : seed->values;
		if (input->excluded == excluded && !intersect) {
			auto changes = std::any_of(input->values.begin(), input->values.end(),
			                           [&](const Value &value) { return !current.count(value); });
			if (changes) {
				if (!result) {
					result = make_uniq<value_set_t>(current);
				}
				result->insert(input->values.begin(), input->values.end());
			}
		} else {
			// Same-polarity intersection; mixed-polarity composition subtracts the other set.
			auto remove = [&](const Value &value) {
				return (input->values.count(value) != 0) != (input->excluded == excluded);
			};
			if (std::any_of(current.begin(), current.end(), remove)) {
				if (!result) {
					result = make_uniq<value_set_t>(current);
				}
				for (auto it = result->begin(); it != result->end();) {
					if (remove(*it)) {
						it = result->erase(it);
					} else {
						++it;
					}
				}
			}
		}
	}
	return result ? Create(std::move(*result), excluded) : seed;
}

optional_idx RelationColumnStats::PopulationBound() const {
	return population_bound;
}

void RelationColumnStats::TightenPopulationBound(idx_t bound) {
	population_bound = optional_idx(population_bound.IsValid() ? MinValue(population_bound.GetIndex(), bound) : bound);
}

void RelationStats::EstablishSupportIdentity() {
	if (columns.empty()) {
		return;
	}
	if (tuple_identities) {
		for (auto &tuple : *tuple_identities) {
			vector<bool> covered(columns.size(), false);
			for (auto column : tuple.columns) {
				covered[column] = true;
			}
			if (std::all_of(covered.begin(), covered.end(), [](bool present) { return present; })) {
				return;
			}
		}
	}
	auto origin = make_shared_ptr<SupportOrigin>();
	RelationTupleIdentity producer {{}, {origin, {}}};
	for (idx_t c = 0; c < columns.size(); c++) {
		auto identities = make_shared_ptr<vector<SupportIdentity>>();
		if (columns[c].support_identities) {
			*identities = *columns[c].support_identities;
		}
		identities->push_back({origin, {c}});
		columns[c].support_identities = identities;
		producer.columns.push_back(c);
		producer.identity.coordinates.push_back(c);
	}
	auto tuples = make_shared_ptr<vector<RelationTupleIdentity>>();
	if (tuple_identities) {
		*tuples = *tuple_identities;
	}
	tuples->push_back(std::move(producer));
	tuple_identities = tuples;
}

DistinctCount::DistinctCount(CardinalityValue distinct_count, DistinctCountSource source)
    : DistinctCount(distinct_count, source,
                    source != DistinctCountSource::CARDINALITY && source != DistinctCountSource::DERIVED) {
}

DistinctCount::DistinctCount(CardinalityValue distinct_count, DistinctCountSource source, bool has_usable_domain)
    : distinct_count(distinct_count), source(source), has_usable_domain(has_usable_domain) {
}

DistinctCountSource DistinctCount::CombineSources(DistinctCountSource left, DistinctCountSource right) {
	if (left == DistinctCountSource::DERIVED || right == DistinctCountSource::DERIVED) {
		return DistinctCountSource::DERIVED;
	}
	return MaxValue(left, right);
}

DistinctCount DistinctCount::ReconcilePopulation(const DistinctCount &left, const DistinctCount &right) {
	auto result = left.has_usable_domain != right.has_usable_domain
	                  ? (left.has_usable_domain ? left : right)
	                  : (left.distinct_count <= right.distinct_count ? left : right);
	result.source = CombineSources(left.source, right.source);
	return result;
}

void SemiAntiJoinDomainEvidence::TightenDomainBound(idx_t bound) {
	if (!domain_bound.IsValid() || bound < domain_bound.GetIndex()) {
		domain_bound = optional_idx(bound);
	}
}

void SemiAntiJoinDomainEvidence::MarkUnique() {
	is_unique = true;
}

void SemiAntiJoinDomainEvidence::Invalidate() {
	domain_bound = optional_idx();
	is_unique = false;
}

optional_idx SemiAntiJoinDomainEvidence::GetDomainBound() const {
	return domain_bound;
}

optional<CardinalityValue> SemiAntiJoinDomainEvidence::GetSupportedDomainSize(CardinalityValue current_domain) const {
	optional<CardinalityValue> result;
	if (is_unique) {
		result = current_domain;
	}
	if (domain_bound.IsValid()) {
		auto bound = CardinalityValue(domain_bound.GetIndex());
		result = result ? MinValue(*result, bound) : bound;
	}
	return result;
}

RelationColumnStats::RelationColumnStats(ColumnBinding binding, DistinctCount domain, Identifier name)
    : RelationColumnStats(binding, domain, domain, std::move(name)) {
}

RelationColumnStats::RelationColumnStats(ColumnBinding binding, DistinctCount total_domain,
                                         DistinctCount current_domain, Identifier name,
                                         SemiAntiJoinDomainEvidence semi_anti_join_domain_evidence)
    : binding(binding), total_domain(total_domain), current_domain(current_domain),
      semi_anti_join_domain_evidence(semi_anti_join_domain_evidence), name(std::move(name)) {
}

optional<CardinalityValue> RelationColumnStats::GetSupportedSemiAntiDomainSize() const {
	return semi_anti_join_domain_evidence.GetSupportedDomainSize(current_domain.distinct_count);
}

RelationStats::RelationStats() : cardinality(1), row_retention(1), stats_initialized(false) {
}

bool RelationStats::IsEmpty() const {
	return max_cardinality.IsValid() && max_cardinality.GetIndex() == 0;
}

CardinalityValue RelationStats::GetCardinality() const {
	return max_cardinality.IsValid() ? MinValue(cardinality, CardinalityValue(max_cardinality.GetIndex()))
	                                 : cardinality;
}

CardinalityValue RelationStats::GetCardinalityForCost() const {
	return IsEmpty() ? CardinalityValue(0) : MaxValue(CardinalityValue(1), GetCardinality());
}

idx_t RelationStats::GetCardinalityForPlan() const {
	auto result = GetCardinalityForCost().GetIndex();
	return max_cardinality.IsValid() ? MinValue(result, max_cardinality.GetIndex()) : result;
}

optional_idx RelationStats::FindColumn(ColumnBinding binding) const {
	for (idx_t column_idx = 0; column_idx < columns.size(); column_idx++) {
		if (columns[column_idx].binding == binding) {
			return column_idx;
		}
	}
	return {};
}

optional_ptr<const RelationColumnStats> RelationStats::GetColumnStats(ColumnBinding binding) const {
	auto column_idx = FindColumn(binding);
	if (!column_idx.IsValid()) {
		return nullptr;
	}
	return columns[column_idx.GetIndex()];
}

bool RelationStats::MatchesBindings(const vector<ColumnBinding> &bindings) const {
	if (columns.size() != bindings.size()) {
		return false;
	}
	for (idx_t column_idx = 0; column_idx < columns.size(); column_idx++) {
		if (columns[column_idx].binding != bindings[column_idx]) {
			return false;
		}
	}
	return true;
}

void RelationStats::AddUniqueKey(vector<idx_t> key) {
	if (key.empty()) {
		return;
	}
	std::sort(key.begin(), key.end());
	key.erase(std::unique(key.begin(), key.end()), key.end());
	if (std::find(unique_keys.begin(), unique_keys.end(), key) == unique_keys.end()) {
		unique_keys.push_back(std::move(key));
		std::sort(unique_keys.begin(), unique_keys.end(), [](const vector<idx_t> &left, const vector<idx_t> &right) {
			return left.size() == right.size() ? left < right : left.size() < right.size();
		});
		if (unique_keys.size() > Distinctness::MAX_UNIQUE_SETS) {
			unique_keys.resize(Distinctness::MAX_UNIQUE_SETS);
		}
	}
}

bool RelationStats::HasUniqueKey(const vector<idx_t> &axes) const {
	if (unique_keys.empty()) {
		return false;
	}
	vector<bool> covered(columns.size(), false);
	for (auto column : axes) {
		covered[column] = true;
	}
	bool changed;
	do {
		changed = false;
		for (auto &group : equivalent_columns) {
			bool present = false;
			for (auto column : group) {
				present |= covered[column];
			}
			if (!present) {
				continue;
			}
			for (auto column : group) {
				changed |= !covered[column];
				covered[column] = true;
			}
		}
	} while (changed);
	for (auto &key : unique_keys) {
		bool complete = !key.empty();
		for (auto column : key) {
			complete &= covered[column];
		}
		if (complete) {
			return true;
		}
	}
	return false;
}

static vector<idx_t> ColumnAliases(idx_t count, const vector<vector<idx_t>> &groups) {
	vector<idx_t> parents;
	for (idx_t i = 0; i < count; i++) {
		parents.push_back(i);
	}
	auto root = [&](idx_t column) {
		while (parents[column] != column) {
			column = parents[column];
		}
		return column;
	};
	for (auto &group : groups) {
		for (idx_t i = 1; i < group.size(); i++) {
			auto first = root(group[0]);
			auto other = root(group[i]);
			parents[MaxValue(first, other)] = MinValue(first, other);
		}
	}
	for (idx_t i = 0; i < count; i++) {
		parents[i] = root(i);
	}
	return parents;
}

vector<idx_t> RelationStats::GetValueAliases() const {
	return ColumnAliases(columns.size(), value_equivalent_columns);
}

void RelationStats::CopyValueRestrictions(const RelationStats &source, const vector<optional_idx> &source_columns) {
	D_ASSERT(columns.size() == source_columns.size());
	auto value_aliases = source.GetValueAliases();
	unordered_map<idx_t, vector<shared_ptr<const ColumnValueRestriction>>> restrictions;
	unordered_map<idx_t, vector<idx_t>> value_outputs;
	for (idx_t c = 0; c < source.columns.size(); c++) {
		restrictions[value_aliases[c]].push_back(source.columns[c].value_restriction);
	}
	for (idx_t c = 0; c < source_columns.size(); c++) {
		if (source_columns[c].IsValid()) {
			value_outputs[value_aliases[source_columns[c].GetIndex()]].push_back(c);
		}
	}
	for (auto &entry : value_outputs) {
		auto restriction = ColumnValueRestriction::Combine(restrictions.at(entry.first), true);
		for (auto c : entry.second) {
			columns[c].value_restriction = restriction;
		}
		if (entry.second.size() > 1) {
			value_equivalent_columns.push_back(entry.second);
		}
	}
}

void RelationStats::CopyEvidence(const RelationStats &source, const vector<optional_idx> &source_columns) {
	D_ASSERT(columns.size() == source_columns.size());
	unique_keys.clear();
	equivalent_columns.clear();
	value_equivalent_columns.clear();
	joint_stats.clear();
	tuple_identities.reset();
	auto parents = ColumnAliases(source.columns.size(), source.equivalent_columns);
	auto root = [&](idx_t column) {
		return parents[column];
	};
	struct ProjectedColumnEvidence {
		vector<idx_t> outputs;
		optional<pair<DistinctCount, DistinctCount>> domains;
		bool can_have_null = true;
		optional_idx population_bound;
		shared_ptr<vector<SupportIdentity>> identities;
	};
	unordered_map<idx_t, ProjectedColumnEvidence> outputs;
	for (idx_t i = 0; i < source_columns.size(); i++) {
		if (source_columns[i].IsValid()) {
			outputs[root(source_columns[i].GetIndex())].outputs.push_back(i);
		}
	}
	for (idx_t i = 0; i < source.columns.size(); i++) {
		auto output = outputs.find(root(i));
		if (output == outputs.end()) {
			continue;
		}
		auto &evidence = output->second;
		auto &column = source.columns[i];
		evidence.can_have_null &= column.can_have_null;
		auto bound = column.PopulationBound();
		if (bound.IsValid() &&
		    (!evidence.population_bound.IsValid() || bound.GetIndex() < evidence.population_bound.GetIndex())) {
			evidence.population_bound = bound;
		}
		if (!evidence.domains) {
			evidence.domains = make_pair(column.total_domain, column.current_domain);
		} else {
			auto &total = evidence.domains->first;
			auto &current = evidence.domains->second;
			total = DistinctCount::ReconcilePopulation(total, column.total_domain);
			current = DistinctCount::ReconcilePopulation(current, column.current_domain);
		}
		if (!column.support_identities) {
			continue;
		}
		auto &identities = evidence.identities;
		if (!identities) {
			identities = make_shared_ptr<vector<SupportIdentity>>();
		}
		for (auto &identity : *column.support_identities) {
			auto same = [&](const SupportIdentity &entry) {
				return RefersToSameObject(*entry.origin, *identity.origin) && entry.coordinates == identity.coordinates;
			};
			if (std::find_if(identities->begin(), identities->end(), same) == identities->end()) {
				identities->push_back(identity);
			}
		}
	}
	for (auto &entry : outputs) {
		auto &evidence = entry.second;
		if (evidence.outputs.size() > 1) {
			equivalent_columns.push_back(evidence.outputs);
		}
		D_ASSERT(evidence.domains);
		for (auto output : evidence.outputs) {
			auto &domains = *evidence.domains;
			columns[output].can_have_null = evidence.can_have_null;
			columns[output].total_domain = domains.first;
			columns[output].current_domain = domains.second;
			columns[output].support_identities = evidence.identities;
			if (evidence.population_bound.IsValid()) {
				columns[output].TightenPopulationBound(evidence.population_bound.GetIndex());
			}
		}
	}
	CopyValueRestrictions(source, source_columns);
	auto remap = [&](const vector<idx_t> &axes) -> optional<vector<idx_t>> {
		vector<idx_t> result;
		for (auto axis : axes) {
			auto entry = outputs.find(root(axis));
			if (entry == outputs.end()) {
				return {};
			}
			result.push_back(entry->second.outputs[0]);
		}
		return result;
	};
	for (auto &key : source.unique_keys) {
		auto projected = remap(key);
		if (projected) {
			AddUniqueKey(std::move(*projected));
		}
	}
	shared_ptr<vector<RelationTupleIdentity>> tuples;
	auto project_tuple = [&](const vector<idx_t> &axes, const SupportIdentity &identity) {
		D_ASSERT(axes.size() == identity.coordinates.size());
		RelationTupleIdentity result;
		result.identity.origin = identity.origin;
		vector<pair<idx_t, idx_t>> projected;
		for (idx_t i = 0; i < axes.size(); i++) {
			auto entry = outputs.find(root(axes[i]));
			if (entry != outputs.end()) {
				projected.emplace_back(entry->second.outputs[0], identity.coordinates[i]);
			}
		}
		std::sort(projected.begin(), projected.end());
		projected.erase(std::unique(projected.begin(), projected.end()), projected.end());
		for (auto &entry : projected) {
			result.columns.push_back(entry.first);
			result.identity.coordinates.push_back(entry.second);
		}
		if (!result.columns.empty()) {
			if (!tuples) {
				tuples = make_shared_ptr<vector<RelationTupleIdentity>>();
			}
			auto same = [&](const RelationTupleIdentity &entry) {
				return entry.columns == result.columns &&
				       RefersToSameObject(*entry.identity.origin, *result.identity.origin) &&
				       entry.identity.coordinates == result.identity.coordinates;
			};
			if (std::find_if(tuples->begin(), tuples->end(), same) == tuples->end()) {
				tuples->push_back(std::move(result));
			}
		}
	};
	if (source.tuple_identities) {
		for (auto &tuple : *source.tuple_identities) {
			project_tuple(tuple.columns, tuple.identity);
		}
	}
	for (auto &joint : source.joint_stats) {
		auto projected = remap(joint.columns);
		if (projected) {
			joint_stats.push_back({std::move(*projected), joint.total_domain, joint.current_domain, joint.identities});
			auto &dependent = joint_stats.back().dependent_columns;
			for (auto column : joint.dependent_columns) {
				auto found = outputs.find(root(column));
				if (found != outputs.end()) {
					dependent.push_back(found->second.outputs[0]);
				}
			}
			std::sort(dependent.begin(), dependent.end());
			dependent.erase(std::unique(dependent.begin(), dependent.end()), dependent.end());
		}
		// A projected tuple identity remains valid even when its NDV needs a dropped axis.
		if (joint.identities) {
			for (auto &identity : *joint.identities) {
				project_tuple(joint.columns, identity);
			}
		}
	}
	if (tuples) {
		tuple_identities = std::move(tuples);
	}
}

void RelationStats::CapCurrentDomainsToCardinality() {
	cardinality = GetCardinality();
	for (auto &column : columns) {
		column.current_domain.distinct_count =
		    MinValue(column.current_domain.distinct_count, MinValue(column.total_domain.distinct_count, cardinality));
	}
	for (auto &joint : joint_stats) {
		joint.current_domain.distinct_count =
		    MinValue(joint.current_domain.distinct_count, MinValue(joint.total_domain.distinct_count, cardinality));
	}
}

RelationColumnStatsLookup::RelationColumnStatsLookup(const RelationStats &stats) {
	AddRelation(stats);
}

void RelationColumnStatsLookup::AddRelation(const RelationStats &stats) {
	for (auto &column : stats.columns) {
		columns.emplace(column.binding, column);
	}
}

optional_ptr<const RelationColumnStats> RelationColumnStatsLookup::Find(ColumnBinding binding) const {
	auto entry = columns.find(binding);
	if (entry == columns.end()) {
		return nullptr;
	}
	return entry->second.get();
}

void RelationStats::Verify(const vector<ColumnBinding> &bindings) const {
	D_ASSERT(!stats_initialized || MatchesBindings(bindings));
	if (!stats_initialized) {
		return;
	}
	D_ASSERT(cardinality == GetCardinality());
	D_ASSERT(row_retention >= 0 && row_retention <= 1);
	for (auto &column : columns) {
		(void)column;
		D_ASSERT(column.current_domain.distinct_count <= column.total_domain.distinct_count);
		D_ASSERT(column.current_domain.distinct_count <= cardinality);
	}
	for (auto &group : value_equivalent_columns) {
		for (auto column : group) {
			(void)column;
			D_ASSERT(column < columns.size());
		}
	}
}

} // namespace duckdb
