#include "duckdb/optimizer/relation_statistics/equality_cardinality_state.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"

#include <algorithm>

namespace duckdb {

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
	result.reserve(groups.size());
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

void ReconcileEqualitySupport(EqualitySupport &point, const EqualitySupport &other) {
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
	groups.parents.reserve(supports.size());
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
			ReconcileEqualitySupport(entry.first->second, supports[i]);
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
		column = state->NormalizeColumn(relation, column);
	}
	auto axes = ordered_axes;
	std::sort(axes.begin(), axes.end());
	axes.erase(std::unique(axes.begin(), axes.end()), axes.end());
	vector<SupportIdentity> identities;
	if (stats.tuple_identities) {
		for (auto &tuple : *stats.tuple_identities) {
			auto source = make_shared_ptr<vector<SupportIdentity>>();
			source->push_back(tuple.identity);
			auto oriented = state->OrientIdentities(source, tuple.columns, ordered_axes);
			if (oriented) {
				identities.insert(identities.end(), oriented->begin(), oriented->end());
			}
		}
	}
	for (auto &joint : stats.joint_stats) {
		if (joint.columns == axes) {
			auto oriented = state->OrientIdentities(joint.identities, joint.columns, ordered_axes);
			if (oriented) {
				identities.insert(identities.end(), oriented->begin(), oriented->end());
			}
			return EqualitySupport {
			    joint.total_domain.distinct_count,
			    MinValue(MinValue(joint.current_domain.distinct_count, joint.total_domain.distinct_count),
			             stats.cardinality),
			    state->DeduplicateIdentities(identities),
			    joint.total_domain.has_usable_domain || stats.HasUniqueKey(axes)};
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
			auto oriented = state->OrientIdentities(joint.identities, joint.columns, ordered_axes);
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
	return EqualitySupport {MaxValue(total->distinct_count, count), count, state->DeduplicateIdentities(identities),
	                        total->has_usable_domain};
}

EqualitySupport SupportWithinTuple(EqualitySupport support, const EqualitySupport &tuple) {
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
	vector<EqualitySupport> points;
	for (auto &coordinate_points : coordinates) {
		points = coordinate_points;
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

enum class JointSupportRequirement { STORED_AXES, ALLOW_DERIVED };

struct EqualityCandidateAxes {
	vector<idx_t> axes;
	JointSupportRequirement requirement;
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
		candidates.push_back({bucket.second, JointSupportRequirement::ALLOW_DERIVED});
		// Retained determinant and dependency axes supply bounded additional candidates. Never enumerate subsets.
		for (auto relation : bucket.first) {
			for (auto &joint : relations[relation].joint_stats) {
				for (auto requirement :
				     {JointSupportRequirement::STORED_AXES, JointSupportRequirement::ALLOW_DERIVED}) {
					auto &columns =
					    requirement == JointSupportRequirement::STORED_AXES ? joint.columns : joint.dependent_columns;
					vector<idx_t> axes;
					for (auto axis : bucket.second) {
						for (auto &binding : components[axis].bindings) {
							if (binding.table_index.index == relation &&
							    std::binary_search(columns.begin(), columns.end(), binding.column_index.GetIndex())) {
								axes.push_back(axis);
							}
						}
					}
					candidates.push_back({std::move(axes), requirement});
				}
			}
		}
	}
	return candidates;
}

vector<EqualityFactor> EqualityCardinalityModel::BuildFactors(const vector<EqualityComponent> &components) const {
	map<vector<idx_t>, vector<EqualitySupport>> candidates;
	auto add_candidate = [&](const vector<idx_t> &axes, JointSupportRequirement requirement) {
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
			if (requirement == JointSupportRequirement::STORED_AXES) {
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
		add_candidate(candidate.axes, candidate.requirement);
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
	points.reserve(component.bindings.size());
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
	return EstimateInternal(components, relations, domain_policy, {});
}

EqualityCardinalityEstimate
EqualityCardinalityModel::EstimateJoin(const vector<pair<ColumnBinding, ColumnBinding>> &equalities,
                                       const vector<idx_t> &relations) const {
	return EstimateInternal(Components(equalities), relations, EqualityDomainPolicy::JOIN, equalities);
}

EqualityCardinalityEstimate
EqualityCardinalityModel::EstimateInternal(const vector<EqualityComponent> &components, const vector<idx_t> &relations,
                                           EqualityDomainPolicy domain_policy,
                                           const vector<pair<ColumnBinding, ColumnBinding>> &equalities) const {
	EqualityCardinalityEstimate result;
	auto factors = BuildFactors(components);
	vector<vector<EqualitySupport>> scalar_domains;
	if (equalities.size() > 1 && components.size() > 1 && !state->sources.empty()) {
		scalar_domains.resize(components.size());
	}
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
	blocks.parents.reserve(state->relations.size());
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
			if (!scalar_domains.empty()) {
				scalar_domains[factor.components[0]] = std::move(points);
			}
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
	if (!scalar_domains.empty() && !result.selectivity.IsZero()) {
		result.selectivity *= CompositeKeyCorrection(components, scalar_domains, equalities);
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
	vector<idx_t> columns;
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
			columns.clear();
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
