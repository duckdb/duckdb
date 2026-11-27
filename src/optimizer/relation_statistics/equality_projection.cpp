#include "duckdb/optimizer/relation_statistics/equality_cardinality_state.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"

#include <algorithm>

namespace duckdb {

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
	vector<idx_t> axes;
	for (auto &factor : factors) {
		axes.clear();
		auto coordinate_support = [&](ColumnBinding binding) {
			auto point = ColumnSupport(binding);
			auto joint = factor.source_joints.find(binding.table_index.index);
			return joint == factor.source_joints.end() ? point : SupportWithinTuple(point, joint->second);
		};
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

struct ProjectedValueRestrictions {
public:
	ProjectedValueRestrictions(const EqualityCardinalityState &state, const RelationStats &output,
	                           const vector<pair<ColumnBinding, ColumnBinding>> &value_equalities) {
		EqualityDisjointSets value_aliases;
		value_aliases.parents.reserve(state.bindings.size());
		for (idx_t i = 0; i < state.bindings.size(); i++) {
			value_aliases.Add();
		}
		for (idx_t r = 0; r < state.relations.size(); r++) {
			for (auto &group : state.relations[r].value_equivalent_columns) {
				for (auto c : group) {
					value_aliases.Merge(state.offsets[r] + group[0], state.offsets[r] + c);
				}
			}
		}
		for (auto &pair : value_equalities) {
			value_aliases.Merge(state.BindingIndex(pair.first), state.BindingIndex(pair.second));
		}
		for (auto binding : state.bindings) {
			restrictions[value_aliases.Find(state.BindingIndex(binding))].push_back(
			    state.relations[binding.table_index.index].columns[binding.column_index.GetIndex()].value_restriction);
		}
		for (idx_t c = 0; c < output.columns.size(); c++) {
			value_outputs[value_aliases.Find(state.BindingIndex(output.columns[c].binding))].push_back(c);
		}
	}

public:
	void Apply(RelationStats &output) const {
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
	}

public:
	map<idx_t, vector<shared_ptr<const ColumnValueRestriction>>> restrictions;
	map<idx_t, vector<idx_t>> value_outputs;
};

struct EqualityCardinalityModel::OutputBindings {
public:
	explicit OutputBindings(const EqualityDisjointSets &input_aliases) : aliases(input_aliases) {
	}

public:
	enum class RequiredAxes { ALL, PRESENT };

	optional<vector<idx_t>> Project(idx_t relation_offset, const vector<idx_t> &axes,
	                                RequiredAxes required = RequiredAxes::ALL) {
		vector<idx_t> result;
		for (auto column : axes) {
			auto found = outputs.find(aliases.Find(relation_offset + column));
			if (found == outputs.end()) {
				if (required == RequiredAxes::ALL) {
					return {};
				}
				continue;
			}
			result.push_back(found->second[0]);
		}
		std::sort(result.begin(), result.end());
		result.erase(std::unique(result.begin(), result.end()), result.end());
		return result;
	}

	RelationTupleIdentity ProjectIdentity(idx_t relation_offset, const vector<idx_t> &axes,
	                                      const SupportIdentity &identity) {
		map<idx_t, idx_t> coordinates;
		for (idx_t i = 0; i < axes.size(); i++) {
			auto found = outputs.find(aliases.Find(relation_offset + axes[i]));
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
	}

public:
	EqualityDisjointSets aliases;
	map<idx_t, vector<idx_t>> outputs;
};

void EqualityCardinalityModel::ProjectColumnDomains(RelationStats &output, OutputBindings &bindings,
                                                    const vector<EqualityComponent> &components,
                                                    const vector<EqualityFactor> &factors,
                                                    const vector<EqualityProjection> &projections) const {
	struct SelectedColumn {
		EqualitySupport support;
		bool nullable;
		bool population_conditioned;
	};
	map<idx_t, SelectedColumn> selected_columns;
	for (auto binding : state->bindings) {
		auto root = bindings.aliases.Find(state->BindingIndex(binding));
		if (!bindings.outputs.count(root) || Normalize(binding) != binding) {
			continue;
		}
		auto r = binding.table_index.index;
		auto c = binding.column_index.GetIndex();
		auto &source = state->relations[r];
		auto &projection = projections[r];
		vector<idx_t> dependent;
		for (auto &joint : source.joint_stats) {
			if (joint.columns.size() == 1 && joint.columns[0] == c) {
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
			ReconcileEqualitySupport(entry.first->second.support, support);
			entry.first->second.nullable &= nullable;
			entry.first->second.population_conditioned |= conditioned;
		}
	}
	for (auto &column : output.columns) {
		auto binding = column.binding;
		auto r = binding.table_index.index;
		column = state->relations[r].columns[Normalize(binding).column_index.GetIndex()];
		column.binding = binding;
		auto &selected = selected_columns.at(bindings.aliases.Find(state->BindingIndex(binding)));
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
	OutputBindings bindings(state->aliases);
	auto &aliases = bindings.aliases;
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
	auto &outputs = bindings.outputs;
	for (idx_t i = 0; i < output.columns.size(); i++) {
		outputs[aliases.Find(state->BindingIndex(output.columns[i].binding))].push_back(i);
	}
	ProjectedValueRestrictions value_restrictions(*state, output, value_equalities);
	if (scope == ProjectionScope::COLUMNS_AND_EVIDENCE) {
		ProjectColumnDomains(output, bindings, components, factors, projections);
	}
	for (auto &entry : outputs) {
		if (entry.second.size() > 1) {
			output.equivalent_columns.push_back(entry.second);
		}
	}
	// Numeric canonical-column copies above do not change the actual value coordinate.
	value_restrictions.Apply(output);
	auto tuples = make_shared_ptr<vector<RelationTupleIdentity>>();
	for (idx_t r = 0; r < state->relations.size(); r++) {
		auto &input = state->relations[r];
		auto &projection = projections[r];
		if (projection.preserve_keys) {
			for (auto &key : input.unique_keys) {
				auto projected = bindings.Project(state->offsets[r], key);
				if (projected) {
					output.AddUniqueKey(std::move(*projected));
				}
			}
		}
		if (projection.preserve_identity && input.tuple_identities) {
			for (auto &tuple : *input.tuple_identities) {
				auto projected = bindings.ProjectIdentity(state->offsets[r], tuple.columns, tuple.identity);
				if (!projected.columns.empty()) {
					tuples->push_back(std::move(projected));
				}
			}
		}
		for (auto &joint : input.joint_stats) {
			auto projected = bindings.Project(state->offsets[r], joint.columns);
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
					auto projected_identity = bindings.ProjectIdentity(state->offsets[r], joint.columns, identity);
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
				auto dependent =
				    bindings.Project(state->offsets[r], joint.dependent_columns, OutputBindings::RequiredAxes::PRESENT);
				output.joint_stats.back().dependent_columns = std::move(*dependent);
			}
		}
	}
	output.tuple_identities = tuples->empty() ? nullptr : tuples;
}

} // namespace duckdb
