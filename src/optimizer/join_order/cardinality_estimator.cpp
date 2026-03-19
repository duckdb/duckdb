#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/query_graph_manager.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/storage/data_table.hpp"

#include <math.h>

namespace duckdb {

// The filter was made on top of a logical sample or other projection,
// but no specific columns are referenced. See issue 4978 number 4.
bool CardinalityEstimator::EmptyFilter(FilterInfo &filter_info) {
	if (!filter_info.left_set && !filter_info.right_set) {
		return true;
	}
	return false;
}

void CardinalityEstimator::AddRelationStats(FilterInfo &filter_info) {
	D_ASSERT(!filter_info.set.get().Empty());
	// Use whichever binding is valid: prefer left_binding, fall back to right_binding.
	// left_binding may be INVALID_INDEX when left_set is empty (e.g. residual predicates
	// or single-column filters where only the right side references a relation).
	auto binding = filter_info.left_binding;
	if (!binding.table_index.IsValid()) {
		binding = filter_info.right_binding;
	}
	if (!binding.table_index.IsValid()) {
		// No valid binding (EmptyFilter), nothing to record
		return;
	}
	for (const RelationsSetToStats &r2tdom : relation_set_stats) {
		auto &i_set = r2tdom.equivalent_relations;
		if (i_set.find(binding) != i_set.end()) {
			// found an equivalent filter
			return;
		}
	}

	auto key = ColumnBinding(binding.table_index, binding.column_index);
	RelationsSetToStats new_r2tdom(column_binding_set_t({key}));

	relation_set_stats.emplace_back(new_r2tdom);
}

bool CardinalityEstimator::SingleColumnFilter(FilterInfo &filter_info) {
	if (filter_info.left_set && filter_info.right_set && filter_info.set.get().count > 1) {
		// Both set and are from different relations
		return false;
	}
	if (EmptyFilter(filter_info)) {
		return false;
	}
	if (filter_info.join_type == JoinType::SEMI || filter_info.join_type == JoinType::ANTI) {
		return false;
	}
	return true;
}

vector<idx_t> CardinalityEstimator::DetermineMatchingEquivalentSets(optional_ptr<FilterInfo> filter_info) {
	vector<idx_t> matching_equivalent_sets;
	idx_t equivalent_relation_index = 0;

	for (const RelationsSetToStats &r2tdom : relation_set_stats) {
		auto &i_set = r2tdom.equivalent_relations;
		if (i_set.find(filter_info->left_binding) != i_set.end()) {
			matching_equivalent_sets.push_back(equivalent_relation_index);
		} else if (i_set.find(filter_info->right_binding) != i_set.end()) {
			// don't add both left and right to the matching_equivalent_sets
			// since both left and right get added to that index anyway.
			matching_equivalent_sets.push_back(equivalent_relation_index);
		}
		equivalent_relation_index++;
	}
	return matching_equivalent_sets;
}

void CardinalityEstimator::AddToEquivalenceSets(optional_ptr<FilterInfo> filter_info,
                                                vector<idx_t> matching_equivalent_sets) {
	D_ASSERT(matching_equivalent_sets.size() <= 2);
	if (matching_equivalent_sets.size() > 1) {
		// an equivalence relation is connecting two sets of equivalence relations
		// so push all relations from the second set into the first. Later we will delete
		// the second set.
		for (ColumnBinding i : relation_set_stats.at(matching_equivalent_sets[1]).equivalent_relations) {
			relation_set_stats.at(matching_equivalent_sets[0]).equivalent_relations.insert(i);
		}
		for (auto &column_name : relation_set_stats.at(matching_equivalent_sets[1]).column_names) {
			relation_set_stats.at(matching_equivalent_sets[0]).column_names.push_back(column_name);
		}
		relation_set_stats.at(matching_equivalent_sets[1]).equivalent_relations.clear();
		relation_set_stats.at(matching_equivalent_sets[1]).column_names.clear();
		relation_set_stats.at(matching_equivalent_sets[0]).filters.push_back(filter_info);
		// add all values of one set to the other, delete the empty one
	} else if (matching_equivalent_sets.size() == 1) {
		auto &tdom_i = relation_set_stats.at(matching_equivalent_sets.at(0));
		tdom_i.equivalent_relations.insert(filter_info->left_binding);
		tdom_i.equivalent_relations.insert(filter_info->right_binding);
		tdom_i.filters.push_back(filter_info);
	} else if (matching_equivalent_sets.empty()) {
		column_binding_set_t tmp;
		tmp.insert(filter_info->left_binding);
		tmp.insert(filter_info->right_binding);
		relation_set_stats.emplace_back(tmp);
		relation_set_stats.back().filters.push_back(filter_info);
	}
}

void CardinalityEstimator::InitEquivalentRelations(const vector<unique_ptr<FilterInfo>> &filter_infos) {
	// For each filter, we fill keep track of the index of the equivalent relation set
	// the left and right relation needs to be added to.
	for (auto &filter : filter_infos) {
		if (SingleColumnFilter(*filter)) {
			// Filter on one relation, (i.e. string or range filter on a column).
			// Grab the first relation and add it to  the equivalence_relations
			AddRelationStats(*filter);
			continue;
		} else if (EmptyFilter(*filter)) {
			continue;
		}
		D_ASSERT(!filter->left_set->Empty());
		D_ASSERT(!filter->right_set->Empty());

		auto matching_equivalent_sets = DetermineMatchingEquivalentSets(filter.get());
		AddToEquivalenceSets(filter.get(), matching_equivalent_sets);
	}
	RemoveEmptyTotalDomains();
}

void CardinalityEstimator::RemoveEmptyTotalDomains() {
	auto remove_start =
	    std::remove_if(relation_set_stats.begin(), relation_set_stats.end(),
	                   [](RelationsSetToStats &r_2_tdom) { return r_2_tdom.equivalent_relations.empty(); });
	relation_set_stats.erase(remove_start, relation_set_stats.end());
}

double CardinalityEstimator::GetNumerator(JoinRelationSet &set) {
	double numerator = 1;
	for (idx_t i = 0; i < set.count; i++) {
		auto &single_node_set = set_manager.GetJoinRelation(set.relations[i]);
		auto card_helper = relation_set_2_cardinality[single_node_set.ToString()];
		numerator *= card_helper.cardinality_before_filters == 0 ? 1 : card_helper.cardinality_before_filters;
	}
	return numerator;
}

bool EdgeConnects(FilterInfoWithTotalDomains &edge, Subgraph2Denominator &subgraph) {
	if (edge.filter_info->left_set) {
		if (JoinRelationSet::IsSubset(*subgraph.relations, *edge.filter_info->left_set)) {
			// cool
			return true;
		}
	}
	if (edge.filter_info->right_set) {
		if (JoinRelationSet::IsSubset(*subgraph.relations, *edge.filter_info->right_set)) {
			return true;
		}
	}
	return false;
}

vector<FilterInfoWithTotalDomains> GetEdges(vector<RelationsSetToStats> &relations_to_tdom,
                                            JoinRelationSet &requested_set) {
	vector<FilterInfoWithTotalDomains> res;
	for (auto &relation_2_tdom : relations_to_tdom) {
		for (auto &filter : relation_2_tdom.filters) {
			if (JoinRelationSet::IsSubset(requested_set, filter->set.get()) && filter->left_set != filter->right_set) {
				FilterInfoWithTotalDomains new_edge(filter, relation_2_tdom);
				res.push_back(new_edge);
			}
		}
	}
	return res;
}

vector<idx_t> SubgraphsConnectedByEdge(FilterInfoWithTotalDomains &edge, vector<Subgraph2Denominator> &subgraphs) {
	vector<idx_t> res;
	if (subgraphs.empty()) {
		return res;
	} else {
		// check the combinations of subgraphs and see if the edge connects two of them,
		// if so, return the indexes of the two subgraphs within the vector
		for (idx_t outer = 0; outer != subgraphs.size(); outer++) {
			// check if the edge connects two subgraphs.
			for (idx_t inner = outer + 1; inner != subgraphs.size(); inner++) {
				if (EdgeConnects(edge, subgraphs.at(outer)) && EdgeConnects(edge, subgraphs.at(inner))) {
					// order is important because we will delete the inner subgraph later
					res.push_back(outer);
					res.push_back(inner);
					return res;
				}
			}
			// if the edge does not connect two subgraphs, see if the edge connects with just outer
			// merge subgraph.at(outer) with the RelationSet(s) that edge connects
			if (EdgeConnects(edge, subgraphs.at(outer))) {
				res.push_back(outer);
				return res;
			}
		}
	}
	// this edge connects only the relations it connects. Return an empty result so a new subgraph is created.
	return res;
}

JoinRelationSet &CardinalityEstimator::UpdateNumeratorRelations(Subgraph2Denominator left, Subgraph2Denominator right,
                                                                FilterInfoWithTotalDomains &filter) {
	switch (filter.filter_info->join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI: {
		if (JoinRelationSet::IsSubset(*left.relations, *filter.filter_info->left_set) &&
		    JoinRelationSet::IsSubset(*right.relations, *filter.filter_info->right_set)) {
			return *left.numerator_relations;
		}
		return *right.numerator_relations;
	}
	default:
		// cross product or inner join
		return set_manager.Union(*left.numerator_relations, *right.numerator_relations);
	}
}

// Extract the comparison type (EQUAL, LESSTHAN, etc.) from a join filter expression.
static ExpressionType GetComparisonType(FilterInfoWithTotalDomains &filter) {
	ExpressionType comparison_type = ExpressionType::INVALID;
	ExpressionIterator::EnumerateExpression(filter.filter_info->filter, [&](Expression &expr) {
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			comparison_type = expr.GetExpressionType();
		}
	});
	return comparison_type;
}

// Apply the denominator multiplier for a given comparison type and effective distinct count.
static double ApplyComparisonRatio(double base_denom, ExpressionType comparison_type, double effective_d) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return base_denom * effective_d;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_DISTINCT_FROM:
		// Assume this blows up, but use the tdom to bound it a bit.
		return base_denom * pow(effective_d, 2.0 / 3.0);
	default:
		return base_denom;
	}
}

double CardinalityEstimator::CalculateInnerJoinDenom(double base_denom, FilterInfoWithTotalDomains &filter) {
	auto effective_d = filter.GetDistinctCount();
	auto comparison_type = GetComparisonType(filter);
	if (comparison_type == ExpressionType::INVALID) {
		return base_denom * effective_d;
	}
	return ApplyComparisonRatio(base_denom, comparison_type, effective_d);
}

double CardinalityEstimator::CalculateLeftJoinDenom(double base_denom, FilterInfoWithTotalDomains &filter) {
	// For LEFT joins, cap D at |RHS| (not min(|LHS|, |RHS|) like INNER joins).
	// This gives estimate = |LHS| * |RHS| / min(D, |RHS|) = max(|LHS|, inner_estimate),
	// ensuring the estimate is always >= |LHS| (every LHS row is preserved in a LEFT join).
	auto right_card = GetNumerator(*filter.filter_info->right_set);
	auto raw_d = filter.GetDistinctCount();
	auto effective_d = right_card > 0 ? MinValue(right_card, raw_d) : raw_d;
	auto comparison_type = GetComparisonType(filter);
	if (comparison_type == ExpressionType::INVALID) {
		return base_denom * effective_d;
	}
	return ApplyComparisonRatio(base_denom, comparison_type, effective_d);
}

double CardinalityEstimator::CalculateSemiAntiJoinDenom(double base_denom, Subgraph2Denominator &left,
                                                        Subgraph2Denominator &right,
                                                        FilterInfoWithTotalDomains &filter) {
	if (JoinRelationSet::IsSubset(*left.relations, *filter.filter_info->left_set) &&
	    JoinRelationSet::IsSubset(*right.relations, *filter.filter_info->right_set)) {
		return left.denom * DEFAULT_SEMI_ANTI_SELECTIVITY;
	}
	return right.denom * DEFAULT_SEMI_ANTI_SELECTIVITY;
}

// Given two subgraphs, compute the updated denominator for the join between them.
double CardinalityEstimator::CalculateUpdatedDenom(Subgraph2Denominator left, Subgraph2Denominator right,
                                                   FilterInfoWithTotalDomains &filter) {
	double base_denom = left.denom * right.denom;
	switch (filter.filter_info->join_type) {
	case JoinType::LEFT:
		return CalculateLeftJoinDenom(base_denom, filter);
	case JoinType::INNER:
		return CalculateInnerJoinDenom(base_denom, filter);
	case JoinType::SEMI:
	case JoinType::ANTI:
		return CalculateSemiAntiJoinDenom(base_denom, left, right, filter);
	default:
		// Cross product: no join condition reduces the denominator.
		return base_denom;
	}
}

DenomInfo CardinalityEstimator::GetDenominator(JoinRelationSet &set) {
	vector<Subgraph2Denominator> subgraphs;

	// Finding the denominator is tricky. You need to go through the tdoms in decreasing order
	// Then loop through all filters in the equivalence set of the tdom to see if both the
	// left and right relations are in the new set, if so you can use that filter.
	// You must also make sure that the filters all relations in the given set, so we use subgraphs
	// that should eventually merge into one connected graph that joins all the relations
	// TODO: Implement a method to cache subgraphs so you don't have to build them up every
	//  time the cardinality of a new set is requested

	// relations_to_tdoms has already been sorted by largest to smallest total domain
	// then we look through the filters for the relations_to_tdoms,
	// and we start to choose the filters that join relations in the set.

	// edges are guaranteed to be in order of largest tdom to smallest tdom.

	// Track which INNER equality equivalence groups have been used to build the subgraph
	unordered_set<idx_t> applied_equivalence_groups;

	// For multi-condition LEFT joins (e.g. A LEFT JOIN B ON k1=k1 AND k2=k2), independent
	// conditions on the same RHS should each contribute to the denominator — but their product
	// must not exceed |RHS|. If it did, the estimate would drop below |LHS|, which is wrong
	// because a LEFT join always preserves every LHS row.
	// We track the accumulated denominator contribution per RHS and cap at |RHS|.
	reference_map_t<JoinRelationSet, double> left_join_rhs_accumulated;

	// Record the ratio a LEFT join edge contributes on its own (base_denom = 1).
	auto record_left_join_ratio = [&](FilterInfoWithTotalDomains &edge) {
		if (edge.filter_info->join_type != JoinType::LEFT) {
			return;
		}
		double ratio = CalculateLeftJoinDenom(1.0, edge);
		double right_card = GetNumerator(*edge.filter_info->right_set);
		left_join_rhs_accumulated[*edge.filter_info->right_set] = right_card > 0 ? MinValue(ratio, right_card) : ratio;
	};

	// Apply the incremental denominator for a LEFT join condition after the first one.
	// We cap the running product per RHS at |RHS| to ensure estimate >= |LHS|.
	auto apply_left_join_increment = [&](FilterInfoWithTotalDomains &edge, double &target_denom) {
		if (edge.filter_info->join_type != JoinType::LEFT) {
			return;
		}
		double ratio = CalculateLeftJoinDenom(1.0, edge);
		double right_card = GetNumerator(*edge.filter_info->right_set);
		auto &accumulated = left_join_rhs_accumulated[*edge.filter_info->right_set];
		double new_accumulated = right_card > 0 ? MinValue(accumulated * ratio, right_card) : accumulated * ratio;
		if (new_accumulated > accumulated && accumulated > 0) {
			target_denom *= new_accumulated / accumulated;
		}
		accumulated = new_accumulated;
	};

	// For multi-key INNER joins (>=2 equality conditions on the same join pair), we treat the
	// composite key as a FK/PK relationship and enforce the FK/PK floor:
	//   estimate = max(|LHS|, |RHS|)  <=>  denom = base * min(|LHS|, |RHS|)
	// This is always correct for composite FK/PK (e.g. TPC-H Q9: ps_suppkey=l_suppkey AND
	// ps_partkey=l_partkey). Using the product D₁×D₂ instead would be wrong in both directions:
	// - If D₁×D₂ < cap: the product overestimates (gives more rows than FK/PK), because
	//   the product assumes independent keys but composite PK keys are correlated.
	// - If D₁ > cap: the first edge alone can push the denom above cap, giving an estimate
	//   below the FK/PK floor.
	// For a single equality condition, the standard D-based formula is used as-is.
	// Key: the full relation set of the edge (union of left and right), unique per join pair.
	// Value: the raw D contribution of the first equality edge for this pair, used to divide
	// it back out when the second condition arrives and we replace it with cap.
	reference_map_t<JoinRelationSet, double> inner_join_pair_first_d;

	auto is_inner_equality = [](FilterInfoWithTotalDomains &edge) {
		if (edge.filter_info->join_type != JoinType::INNER) {
			return false;
		}
		auto ctype = GetComparisonType(edge);
		return ctype == ExpressionType::COMPARE_EQUAL || ctype == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	};

	// Record the raw D of the first equality edge for a join pair.
	auto record_inner_join_ratio = [&](FilterInfoWithTotalDomains &edge) {
		if (!is_inner_equality(edge)) {
			return;
		}
		double ratio = CalculateInnerJoinDenom(1.0, edge);
		inner_join_pair_first_d[edge.filter_info->set.get()] = ratio;
	};

	// Returns true if handled (i.e. the edge was for a known join pair in the first-D map).
	// On the second equality condition for a pair, replaces the first-edge D contribution in
	// target_denom with cap = min(|LHS|, |RHS|), enforcing the FK/PK floor.
	// Subsequent conditions for the same pair are no-ops (first_d already == cap).
	auto apply_inner_join_increment = [&](FilterInfoWithTotalDomains &edge, double &target_denom) -> bool {
		if (!is_inner_equality(edge)) {
			return false;
		}
		auto it = inner_join_pair_first_d.find(edge.filter_info->set.get());
		if (it == inner_join_pair_first_d.end()) {
			return false;
		}
		auto left_card = GetNumerator(*edge.filter_info->left_set);
		auto right_card = GetNumerator(*edge.filter_info->right_set);
		double cap = (left_card > 0 && right_card > 0) ? MinValue(left_card, right_card) : 0;
		auto &first_d = it->second;
		if (cap > 0 && first_d != cap) {
			// Replace the first-edge D contribution with cap (FK/PK floor).
			// target_denom = base × first_d  →  new = base × cap
			target_denom = target_denom / first_d * cap;
			first_d = cap;
		}
		return true;
	};

	unordered_set<idx_t> unused_edge_tdoms;
	// Record edge_equivalence_index whenever an edge is actively used to build the subgraph.
	auto record_equivalence_group = [&](const FilterInfoWithTotalDomains &edge) {
		if (edge.filter_info->edge_equivalence_index.IsValid()) {
			applied_equivalence_groups.insert(edge.filter_info->edge_equivalence_index.GetIndex());
		}
	};
	auto edges = GetEdges(relation_set_stats, set);
	for (auto &edge : edges) {
		if (subgraphs.size() == 1 && subgraphs.at(0).relations->ToString() == set.ToString()) {
			// The subgraph already connects all desired relations.
			// For LEFT joins, allow additional conditions to contribute incrementally,
			// but cap the accumulated product per RHS at |RHS| so the estimate stays >= |LHS|.
			// For INNER equality joins, apply the FK/PK multi-key cap (accumulated product of
			// equality condition denominators, capped at min(|LHS|, |RHS|)).
			// Other INNER join conditions get a mild penalty via denom_multiplier — unless the
			// edge is transitively implied by equality conditions already in the subgraph.
			if (edge.filter_info->join_type == JoinType::LEFT) {
				apply_left_join_increment(edge, subgraphs.at(0).denom);
			} else if (apply_inner_join_increment(edge, subgraphs.at(0).denom)) {
				// FK/PK multi-key increment applied, skip unused_edge_tdoms penalty.
			} else if (edge.filter_info->edge_equivalence_index.IsValid() &&
			           applied_equivalence_groups.count(edge.filter_info->edge_equivalence_index.GetIndex())) {
				// Transitively implied by equality conditions already used to build the subgraph,
				// skip the penalty entirely.
				continue;
			} else if (edge.has_distinct_count_hll) {
				unused_edge_tdoms.insert(edge.distinct_count_hll);
			}
			continue;
		}

		auto subgraph_connections = SubgraphsConnectedByEdge(edge, subgraphs);
		if (subgraph_connections.empty()) {
			// create a subgraph out of left and right, then merge right into left and add left to subgraphs.
			// this helps cover a case where there are no subgraphs yet, and the only join filter is a SEMI JOIN
			auto left_subgraph = Subgraph2Denominator();
			auto right_subgraph = Subgraph2Denominator();
			left_subgraph.relations = edge.filter_info->left_set;
			left_subgraph.numerator_relations = edge.filter_info->left_set;
			right_subgraph.relations = edge.filter_info->right_set;
			right_subgraph.numerator_relations = edge.filter_info->right_set;
			left_subgraph.numerator_relations = &UpdateNumeratorRelations(left_subgraph, right_subgraph, edge);
			left_subgraph.relations = &edge.filter_info->set.get();
			left_subgraph.denom = CalculateUpdatedDenom(left_subgraph, right_subgraph, edge);
			record_left_join_ratio(edge);
			record_inner_join_ratio(edge);
			record_equivalence_group(edge);
			subgraphs.push_back(left_subgraph);
		} else if (subgraph_connections.size() == 1) {
			auto left_subgraph = &subgraphs.at(subgraph_connections.at(0));
			auto right_subgraph = Subgraph2Denominator();
			right_subgraph.relations = edge.filter_info->right_set;
			right_subgraph.numerator_relations = edge.filter_info->right_set;
			if (JoinRelationSet::IsSubset(*left_subgraph->relations, *right_subgraph.relations)) {
				right_subgraph.relations = edge.filter_info->left_set;
				right_subgraph.numerator_relations = edge.filter_info->left_set;
			}

			if (JoinRelationSet::IsSubset(*left_subgraph->relations, *edge.filter_info->left_set) &&
			    JoinRelationSet::IsSubset(*left_subgraph->relations, *edge.filter_info->right_set)) {
				// Edge connects the same subgraph to itself — no new relation is added.
				// Apply the incremental denominator contribution for LEFT or INNER multi-key joins.
				if (edge.filter_info->join_type == JoinType::LEFT) {
					apply_left_join_increment(edge, left_subgraph->denom);
				} else {
					apply_inner_join_increment(edge, left_subgraph->denom);
				}
				continue;
			}
			left_subgraph->numerator_relations = &UpdateNumeratorRelations(*left_subgraph, right_subgraph, edge);
			left_subgraph->relations = &set_manager.Union(*left_subgraph->relations, *right_subgraph.relations);
			left_subgraph->denom = CalculateUpdatedDenom(*left_subgraph, right_subgraph, edge);
			record_left_join_ratio(edge);
			record_inner_join_ratio(edge);
			record_equivalence_group(edge);
		} else if (subgraph_connections.size() == 2) {
			// The two subgraphs in the subgraph_connections can be merged by this edge.
			D_ASSERT(subgraph_connections.at(0) < subgraph_connections.at(1));
			auto subgraph_to_merge_into = &subgraphs.at(subgraph_connections.at(0));
			auto subgraph_to_delete = &subgraphs.at(subgraph_connections.at(1));
			subgraph_to_merge_into->relations =
			    &set_manager.Union(*subgraph_to_merge_into->relations, *subgraph_to_delete->relations);
			subgraph_to_merge_into->numerator_relations =
			    &UpdateNumeratorRelations(*subgraph_to_merge_into, *subgraph_to_delete, edge);
			subgraph_to_merge_into->denom = CalculateUpdatedDenom(*subgraph_to_merge_into, *subgraph_to_delete, edge);
			record_left_join_ratio(edge);
			record_inner_join_ratio(edge);
			record_equivalence_group(edge);
			subgraph_to_delete->relations = nullptr;
			auto remove_start = std::remove_if(subgraphs.begin(), subgraphs.end(),
			                                   [](Subgraph2Denominator &s) { return !s.relations; });
			subgraphs.erase(remove_start, subgraphs.end());
		}
	}

	// Slight penalty to cardinality for unused INNER join edges.
	auto denom_multiplier = 1.0 + static_cast<double>(unused_edge_tdoms.size());

	// It's possible cross-products were added and are not present in the filters in the relation_2_tdom
	// structures. When that's the case, merge all remaining subgraphs as if they are connected by a cross product
	if (subgraphs.size() > 1) {
		auto final_subgraph = subgraphs.at(0);
		for (auto merge_with = subgraphs.begin() + 1; merge_with != subgraphs.end(); merge_with++) {
			D_ASSERT(final_subgraph.relations && merge_with->relations);
			final_subgraph.relations = &set_manager.Union(*final_subgraph.relations, *merge_with->relations);
			D_ASSERT(final_subgraph.numerator_relations && merge_with->numerator_relations);
			final_subgraph.numerator_relations =
			    &set_manager.Union(*final_subgraph.numerator_relations, *merge_with->numerator_relations);
			final_subgraph.denom *= merge_with->denom;
		}
	}
	if (!subgraphs.empty()) {
		// Some relations are connected by cross products and will not end up in a subgraph
		// Check and make sure all relations were considered, if not, they are connected to the graph by cross products
		auto &returning_subgraph = subgraphs.at(0);
		if (returning_subgraph.relations->count != set.count) {
			for (idx_t rel_index = 0; rel_index < set.count; rel_index++) {
				auto relation_id = set.relations[rel_index];
				auto &rel = set_manager.GetJoinRelation(relation_id);
				if (!JoinRelationSet::IsSubset(*returning_subgraph.relations, rel)) {
					returning_subgraph.numerator_relations =
					    &set_manager.Union(*returning_subgraph.numerator_relations, rel);
					returning_subgraph.relations = &set_manager.Union(*returning_subgraph.relations, rel);
				}
			}
		}
	}

	// can happen if a table has cardinality 0, a tdom is set to 0, or if a cross product is used.
	if (subgraphs.empty() || subgraphs.at(0).denom == 0) {
		// denominator is 1 and numerators are a cross product of cardinalities.
		return DenomInfo(set, 1);
	}
	return DenomInfo(*subgraphs.at(0).numerator_relations, subgraphs.at(0).denom * denom_multiplier);
}

// Cardinality is calculated using logic found in
// https://blobs.duckdb.org/papers/tom-ebergen-msc-thesis-join-order-optimization-with-almost-no-statistics.pdf TL;DR
// Cardinality is estimated based on cardinality of base tables and the distinct counts of joined columns. If you have
// two tables A and B joined using A.x = B.y we assume that each tuple in A will match ~ B/(distinct(y)) tuples in B.
// The cardinality estimation then becomes (|A|x|B|) / max(distinct(x), distinct(y)).
// If there are extra joins, you can add the cardinality of the table to the numerator, and the
// distinct count of the join condition to the denominator.
// One benefit of this cardinality estimation formula is that it is associative and commutative, which means regardless
// of the order of the joins/join tree, the cardinality estimate will always be the same. The drawback of this current
// implementation, however, is that it only considers equality join conditions. Some modification have been made for
// comparison types like <, <=, >, >=, !=, but only a "penalty" was introduced, and the calculated cardinality is not
// based on stats (see CalculateUpdatedDenom()).
template <>
double CardinalityEstimator::EstimateCardinalityWithSet(JoinRelationSet &new_set) {
	if (relation_set_2_cardinality.find(new_set.ToString()) != relation_set_2_cardinality.end()) {
		return relation_set_2_cardinality[new_set.ToString()].cardinality_before_filters;
	}

	// can happen if a table has cardinality 0, or a tdom is set to 0
	auto denom = GetDenominator(new_set);
	// we pass numerator relations, because for semi and anti joins, we don't want to
	// include cardinalities of relations on the RHS of a semi/anti join.
	auto numerator = GetNumerator(denom.numerator_relations);

	double result = numerator / denom.denominator;
	auto new_entry = CardinalityHelper(result);
	relation_set_2_cardinality[new_set.ToString()] = new_entry;
	return result;
}

template <>
idx_t CardinalityEstimator::EstimateCardinalityWithSet(JoinRelationSet &new_set) {
	auto cardinality_as_double = EstimateCardinalityWithSet<double>(new_set);
	auto max = NumericLimits<idx_t>::Maximum();
	if (cardinality_as_double >= (double)max) {
		return max;
	}
	return (idx_t)cardinality_as_double;
}

bool SortTdoms(const RelationsSetToStats &a, const RelationsSetToStats &b) {
	if (a.has_distinct_count_hll && b.has_distinct_count_hll) {
		return a.distinct_count_hll > b.distinct_count_hll;
	}
	if (a.has_distinct_count_hll) {
		return a.distinct_count_hll > b.distinct_count_no_hll;
	}
	if (b.has_distinct_count_hll) {
		return a.distinct_count_no_hll > b.distinct_count_hll;
	}
	return a.distinct_count_no_hll > b.distinct_count_no_hll;
}

void CardinalityEstimator::InitCardinalityEstimatorProps(optional_ptr<JoinRelationSet> set, RelationStats &stats) {
	// Get the join relation set
	D_ASSERT(stats.stats_initialized);
	auto relation_cardinality = stats.cardinality;

	auto card_helper = CardinalityHelper((double)relation_cardinality);
	relation_set_2_cardinality[set->ToString()] = card_helper;

	UpdateTotalDomains(set, stats);

	// sort relations from greatest tdom to lowest tdom.
	std::sort(relation_set_stats.begin(), relation_set_stats.end(), SortTdoms);
}

void CardinalityEstimator::UpdateTotalDomains(optional_ptr<JoinRelationSet> set, RelationStats &stats) {
	D_ASSERT(set->count == 1);
	auto relation_id = set->relations[0];
	//! Initialize the distinct count for all columns used in joins with the current relation.
	//	D_ASSERT(stats.column_distinct_count.size() >= 1);

	for (idx_t i = 0; i < stats.column_distinct_count.size(); i++) {
		//! for every column used in a filter in the relation, get the distinct count via HLL, or assume it to be
		//! the cardinality
		// Update the relation_to_tdom set with the estimated distinct count (or tdom) calculated above
		auto key = ColumnBinding(TableIndex(relation_id.index), ProjectionIndex(i));
		for (auto &relation_to_tdom : relation_set_stats) {
			column_binding_set_t i_set = relation_to_tdom.equivalent_relations;
			if (i_set.find(key) == i_set.end()) {
				continue;
			}
			auto distinct_count = stats.column_distinct_count.at(i);
			if (distinct_count.from_hll && relation_to_tdom.has_distinct_count_hll) {
				relation_to_tdom.distinct_count_hll =
				    MaxValue(relation_to_tdom.distinct_count_hll, distinct_count.distinct_count);
			} else if (distinct_count.from_hll && !relation_to_tdom.has_distinct_count_hll) {
				relation_to_tdom.has_distinct_count_hll = true;
				relation_to_tdom.distinct_count_hll = distinct_count.distinct_count;
			} else {
				relation_to_tdom.distinct_count_no_hll =
				    MinValue(distinct_count.distinct_count, relation_to_tdom.distinct_count_no_hll);
			}
			break;
		}
	}
}

// LCOV_EXCL_START

void CardinalityEstimator::AddRelationNamesToRelationStats(vector<RelationStats> &stats) {
#ifdef DEBUG
	for (auto &total_domain : relation_set_stats) {
		for (auto &binding : total_domain.equivalent_relations) {
			D_ASSERT(binding.table_index.index < stats.size());
			string column_name;
			if (binding.column_index.index < stats[binding.table_index.index].column_names.size()) {
				column_name = stats[binding.table_index.index].column_names[binding.column_index.index];
			} else {
				column_name = "[unknown]";
			}
			total_domain.column_names.push_back(column_name);
		}
	}
#endif
}

void CardinalityEstimator::PrintRelationStats() {
	for (auto &total_domain : relation_set_stats) {
		string domain = "Following columns have the same distinct count: ";
		for (auto &column_name : total_domain.column_names) {
			domain += column_name + ", ";
		}
		bool have_hll = total_domain.has_distinct_count_hll;
		domain += "\n TOTAL DOMAIN = " +
		          to_string(have_hll ? total_domain.distinct_count_hll : total_domain.distinct_count_no_hll);
		Printer::Print(domain);
	}
}

// LCOV_EXCL_STOP

} // namespace duckdb
