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

namespace duckdb {

// The filter was made on top of a logical sample or other projection,
// but no specific columns are referenced. See issue 4978 number 4.
bool CardinalityEstimator::EmptyFilter(FilterInfo &filter_info) {
	if (!filter_info.left_set && !filter_info.right_set) {
		return true;
	}
	return false;
}

void CardinalityEstimator::AddRelationTdom(FilterInfo &filter_info) {
	D_ASSERT(filter_info.set.get().count >= 1);
	for (const RelationsToTDom &r2tdom : relations_to_tdoms) {
		auto &i_set = r2tdom.equivalent_relations;
		if (i_set.find(filter_info.left_binding) != i_set.end()) {
			// found an equivalent filter
			return;
		}
	}

	auto key = ColumnBinding(filter_info.left_binding.table_index, filter_info.left_binding.column_index);
	RelationsToTDom new_r2tdom(column_binding_set_t({key}));

	relations_to_tdoms.emplace_back(new_r2tdom);
}

bool CardinalityEstimator::SingleColumnFilter(duckdb::FilterInfo &filter_info) {
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

	for (const RelationsToTDom &r2tdom : relations_to_tdoms) {
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
		for (ColumnBinding i : relations_to_tdoms.at(matching_equivalent_sets[1]).equivalent_relations) {
			relations_to_tdoms.at(matching_equivalent_sets[0]).equivalent_relations.insert(i);
		}
		for (auto &column_name : relations_to_tdoms.at(matching_equivalent_sets[1]).column_names) {
			relations_to_tdoms.at(matching_equivalent_sets[0]).column_names.push_back(column_name);
		}
		relations_to_tdoms.at(matching_equivalent_sets[1]).equivalent_relations.clear();
		relations_to_tdoms.at(matching_equivalent_sets[1]).column_names.clear();
		relations_to_tdoms.at(matching_equivalent_sets[0]).filters.push_back(filter_info);
		// add all values of one set to the other, delete the empty one
	} else if (matching_equivalent_sets.size() == 1) {
		auto &tdom_i = relations_to_tdoms.at(matching_equivalent_sets.at(0));
		tdom_i.equivalent_relations.insert(filter_info->left_binding);
		tdom_i.equivalent_relations.insert(filter_info->right_binding);
		tdom_i.filters.push_back(filter_info);
	} else if (matching_equivalent_sets.empty()) {
		column_binding_set_t tmp;
		tmp.insert(filter_info->left_binding);
		tmp.insert(filter_info->right_binding);
		relations_to_tdoms.emplace_back(tmp);
		relations_to_tdoms.back().filters.push_back(filter_info);
	}
}

void CardinalityEstimator::InitEquivalentRelations(const vector<unique_ptr<FilterInfo>> &filter_infos) {
	// For each filter, we fill keep track of the index of the equivalent relation set
	// the left and right relation needs to be added to.
	for (auto &filter : filter_infos) {
		if (SingleColumnFilter(*filter)) {
			// Filter on one relation, (i.e. string or range filter on a column).
			// Grab the first relation and add it to  the equivalence_relations
			AddRelationTdom(*filter);
			continue;
		} else if (EmptyFilter(*filter)) {
			continue;
		}
		D_ASSERT(filter->left_set->count >= 1);
		D_ASSERT(filter->right_set->count >= 1);

		auto matching_equivalent_sets = DetermineMatchingEquivalentSets(filter.get());
		AddToEquivalenceSets(filter.get(), matching_equivalent_sets);
	}
	RemoveEmptyTotalDomains();
}

void CardinalityEstimator::RemoveEmptyTotalDomains() {
	auto remove_start = std::remove_if(relations_to_tdoms.begin(), relations_to_tdoms.end(),
	                                   [](RelationsToTDom &r_2_tdom) { return r_2_tdom.equivalent_relations.empty(); });
	relations_to_tdoms.erase(remove_start, relations_to_tdoms.end());
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

vector<FilterInfoWithTotalDomains> GetEdges(vector<RelationsToTDom> &relations_to_tdom,
                                            JoinRelationSet &requested_set) {
	vector<FilterInfoWithTotalDomains> res;
	for (auto &relation_2_tdom : relations_to_tdom) {
		for (auto &filter : relation_2_tdom.filters) {
			if (JoinRelationSet::IsSubset(requested_set, filter->set)) {
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

double CardinalityEstimator::CalculateUpdatedDenom(Subgraph2Denominator left, Subgraph2Denominator right,
                                                   FilterInfoWithTotalDomains &filter) {
	double new_denom = left.denom * right.denom;
	switch (filter.filter_info->join_type) {
	case JoinType::INNER: {
		bool set = false;
		ExpressionType comparison_type = ExpressionType::COMPARE_EQUAL;
		ExpressionIterator::EnumerateExpression(filter.filter_info->filter, [&](Expression &expr) {
			if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
				comparison_type = expr.GetExpressionType();
				set = true;
				return;
			}
		});
		if (!set) {
			new_denom *=
			    filter.has_tdom_hll ? static_cast<double>(filter.tdom_hll) : static_cast<double>(filter.tdom_no_hll);
			// no comparison is taking place, so the denominator is just the product of the left and right
			return new_denom;
		}
		// extra_ratio helps represents how many tuples will be filtered out if the comparison evaluates to
		// false. set to 1 to assume cross product.
		double extra_ratio = 1;
		switch (comparison_type) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			// extra ration stays 1
			extra_ratio = filter.has_tdom_hll ? (double)filter.tdom_hll : (double)filter.tdom_no_hll;
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHAN:
			// start with the selectivity of equality
			extra_ratio = filter.has_tdom_hll ? (double)filter.tdom_hll : (double)filter.tdom_no_hll;
			// now assume every tuple will match 2.5 times (on average)
			extra_ratio *= static_cast<double>(1) / CardinalityEstimator::DEFAULT_LT_GT_MULTIPLIER;
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_DISTINCT_FROM:
			// basically assume cross product.
			extra_ratio = 1;
			break;
		default:
			break;
		}
		new_denom *= extra_ratio;
		return new_denom;
	}
	case JoinType::SEMI:
	case JoinType::ANTI: {
		if (JoinRelationSet::IsSubset(*left.relations, *filter.filter_info->left_set) &&
		    JoinRelationSet::IsSubset(*right.relations, *filter.filter_info->right_set)) {
			new_denom = left.denom * CardinalityEstimator::DEFAULT_SEMI_ANTI_SELECTIVITY;
			return new_denom;
		}
		new_denom = right.denom * CardinalityEstimator::DEFAULT_SEMI_ANTI_SELECTIVITY;
		return new_denom;
	}
	default:
		// cross product
		return new_denom;
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
	// time the cardinality of a new set is requested

	// relations_to_tdoms has already been sorted by largest to smallest total domain
	// then we look through the filters for the relations_to_tdoms,
	// and we start to choose the filters that join relations in the set.

	// edges are guaranteed to be in order of largest tdom to smallest tdom.
	unordered_set<idx_t> unused_edge_tdoms;
	auto edges = GetEdges(relations_to_tdoms, set);
	for (auto &edge : edges) {
		if (subgraphs.size() == 1 && subgraphs.at(0).relations->ToString() == set.ToString()) {
			// the first subgraph has connected all the desired relations, just skip the rest of the edges
			if (edge.has_tdom_hll) {
				unused_edge_tdoms.insert(edge.tdom_hll);
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
			left_subgraph.relations = edge.filter_info->set.get();
			left_subgraph.denom = CalculateUpdatedDenom(left_subgraph, right_subgraph, edge);
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
				// here we have an edge that connects the same subgraph to the same subgraph. Just continue. no need to
				// update the denom
				continue;
			}
			left_subgraph->numerator_relations = &UpdateNumeratorRelations(*left_subgraph, right_subgraph, edge);
			left_subgraph->relations = &set_manager.Union(*left_subgraph->relations, *right_subgraph.relations);
			left_subgraph->denom = CalculateUpdatedDenom(*left_subgraph, right_subgraph, edge);
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
			subgraph_to_delete->relations = nullptr;
			auto remove_start = std::remove_if(subgraphs.begin(), subgraphs.end(),
			                                   [](Subgraph2Denominator &s) { return !s.relations; });
			subgraphs.erase(remove_start, subgraphs.end());
		}
	}

	// Slight penalty to cardinality for unused edges
	auto denom_multiplier = 1.0 + static_cast<double>(unused_edge_tdoms.size());

	// It's possible cross-products were added and are not present in the filters in the relation_2_tdom
	// structures. When that's the case, merge all remaining subgraphs.
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
	// can happen if a table has cardinality 0, a tdom is set to 0, or if a cross product is used.
	if (subgraphs.empty() || subgraphs.at(0).denom == 0) {
		// denominator is 1 and numerators are a cross product of cardinalities.
		return DenomInfo(set, 1, 1);
	}
	return DenomInfo(*subgraphs.at(0).numerator_relations, 1, subgraphs.at(0).denom * denom_multiplier);
}

template <>
double CardinalityEstimator::EstimateCardinalityWithSet(JoinRelationSet &new_set) {

	if (relation_set_2_cardinality.find(new_set.ToString()) != relation_set_2_cardinality.end()) {
		return relation_set_2_cardinality[new_set.ToString()].cardinality_before_filters;
	}

	// can happen if a table has cardinality 0, or a tdom is set to 0
	auto denom = GetDenominator(new_set);
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

bool SortTdoms(const RelationsToTDom &a, const RelationsToTDom &b) {
	if (a.has_tdom_hll && b.has_tdom_hll) {
		return a.tdom_hll > b.tdom_hll;
	}
	if (a.has_tdom_hll) {
		return a.tdom_hll > b.tdom_no_hll;
	}
	if (b.has_tdom_hll) {
		return a.tdom_no_hll > b.tdom_hll;
	}
	return a.tdom_no_hll > b.tdom_no_hll;
}

void CardinalityEstimator::InitCardinalityEstimatorProps(optional_ptr<JoinRelationSet> set, RelationStats &stats) {
	// Get the join relation set
	D_ASSERT(stats.stats_initialized);
	auto relation_cardinality = stats.cardinality;

	auto card_helper = CardinalityHelper((double)relation_cardinality);
	relation_set_2_cardinality[set->ToString()] = card_helper;

	UpdateTotalDomains(set, stats);

	// sort relations from greatest tdom to lowest tdom.
	std::sort(relations_to_tdoms.begin(), relations_to_tdoms.end(), SortTdoms);
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
		auto key = ColumnBinding(relation_id, i);
		for (auto &relation_to_tdom : relations_to_tdoms) {
			column_binding_set_t i_set = relation_to_tdom.equivalent_relations;
			if (i_set.find(key) == i_set.end()) {
				continue;
			}
			auto distinct_count = stats.column_distinct_count.at(i);
			if (distinct_count.from_hll && relation_to_tdom.has_tdom_hll) {
				relation_to_tdom.tdom_hll = MaxValue(relation_to_tdom.tdom_hll, distinct_count.distinct_count);
			} else if (distinct_count.from_hll && !relation_to_tdom.has_tdom_hll) {
				relation_to_tdom.has_tdom_hll = true;
				relation_to_tdom.tdom_hll = distinct_count.distinct_count;
			} else {
				relation_to_tdom.tdom_no_hll = MinValue(distinct_count.distinct_count, relation_to_tdom.tdom_no_hll);
			}
			break;
		}
	}
}

// LCOV_EXCL_START

void CardinalityEstimator::AddRelationNamesToTdoms(vector<RelationStats> &stats) {
#ifdef DEBUG
	for (auto &total_domain : relations_to_tdoms) {
		for (auto &binding : total_domain.equivalent_relations) {
			D_ASSERT(binding.table_index < stats.size());
			string column_name;
			if (binding.column_index < stats[binding.table_index].column_names.size()) {
				column_name = stats[binding.table_index].column_names[binding.column_index];
			} else {
				column_name = "[unknown]";
			}
			total_domain.column_names.push_back(column_name);
		}
	}
#endif
}

void CardinalityEstimator::PrintRelationToTdomInfo() {
	for (auto &total_domain : relations_to_tdoms) {
		string domain = "Following columns have the same distinct count: ";
		for (auto &column_name : total_domain.column_names) {
			domain += column_name + ", ";
		}
		bool have_hll = total_domain.has_tdom_hll;
		domain += "\n TOTAL DOMAIN = " + to_string(have_hll ? total_domain.tdom_hll : total_domain.tdom_no_hll);
		Printer::Print(domain);
	}
}

// LCOV_EXCL_STOP

} // namespace duckdb
