#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/limits.hpp"

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
	D_ASSERT(filter_info.set.count >= 1);
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

bool CardinalityEstimator::SingleColumnFilter(FilterInfo &filter_info) {
	if (filter_info.left_set && filter_info.right_set) {
		// Both set
		return false;
	}
	if (EmptyFilter(filter_info)) {
		return false;
	}
	return true;
}

vector<idx_t> CardinalityEstimator::DetermineMatchingEquivalentSets(FilterInfo *filter_info) {
	vector<idx_t> matching_equivalent_sets;
	auto equivalent_relation_index = 0;

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

void CardinalityEstimator::AddToEquivalenceSets(FilterInfo *filter_info, vector<idx_t> matching_equivalent_sets) {
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


vector<FilterInfoWithTotalDomains> GetEdges(vector<RelationsToTDom> &relations_to_tdom) {
	vector<FilterInfoWithTotalDomains> res;
	for (auto &relation_2_tdom : relations_to_tdom) {
		for (auto &filter : relation_2_tdom.filters) {
			FilterInfoWithTotalDomains new_edge(filter, relation_2_tdom);
			res.push_back(new_edge);
		}
	}
	return res;
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


vector<idx_t> SubgraphsConnectedByEdge(FilterInfoWithTotalDomains &edge, vector<Subgraph2Denominator> &subgraphs) {
	vector<idx_t> res;
	if (subgraphs.empty()) {
		return res;
	}
	if (subgraphs.size() == 1 && EdgeConnects(edge, subgraphs.at(0))) {
		// there is one subgraph, and the edge connects a relation outside the subgraph
		// to inside it, so we expand the subgraph.
		res.push_back(0);
		return res;
	}
	if (subgraphs.size() >= 2) {
		// check the combinations of subgraphs and see if the edge connects two of them,
		// if so, return the indexes of the two subgraphs within the vector
		for (idx_t outer = 0; outer != subgraphs.size(); outer++) {
			for (idx_t inner = outer + 1; inner != subgraphs.size(); inner++) {
				if (EdgeConnects(edge, subgraphs.at(outer)) && EdgeConnects(edge, subgraphs.at(inner))) {
					res.push_back(inner);
					res.push_back(outer);
					return res;
				}
			}
		}
	}
	throw InternalException("whoops");
}


JoinRelationSet &CardinalityEstimator::UpdateNumeratorRelations(Subgraph2Denominator left, Subgraph2Denominator right, FilterInfoWithTotalDomains &filter) {
	switch (filter.filter_info->join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI: {
		if (JoinRelationSet::IsSubset(*left.relations, *filter.filter_info->left_set) &&
		    JoinRelationSet::IsSubset(*right.relations, *filter.filter_info->right_set)) {
			return *left.relations;
		}
		return *right.relations;
	}
	default:
		// cross product
		return set_manager.Union(*left.relations, *right.relations);
	}
}


double CardinalityEstimator::CalculateUpdatedDemo(Subgraph2Denominator left, Subgraph2Denominator right, FilterInfoWithTotalDomains &filter) {
	double new_denom = left.denom * right.denom;
	switch (filter.filter_info->join_type) {
	case JoinType::INNER: {
		new_denom *= filter.has_tdom_hll ? filter.tdom_hll : filter.tdom_no_hll;
		return new_denom;
	}
	case JoinType::SEMI:
	case JoinType::ANTI: {
		if (JoinRelationSet::IsSubset(*left.relations, *filter.filter_info->left_set) &&
		    JoinRelationSet::IsSubset(*right.relations, *filter.filter_info->right_set)) {
			new_denom = left.denom * 5;
			return new_denom;
		}
		new_denom = right.denom * 5;
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
	auto edges = GetEdges(relations_to_tdoms);

	for (auto &edge : edges) {
		auto subgraph_connections = SubgraphsConnectedByEdge(edge, subgraphs);
		auto new_subgraph = Subgraph2Denominator();
		new_subgraph.relations = &edge.filter_info->set;
		new_subgraph.numerator_relations = &edge.filter_info->set;
		new_subgraph.denom = edge.has_tdom_hll ? edge.tdom_hll : edge.tdom_no_hll;
		if (subgraph_connections.empty()) {
			// edge does not connect any subgraphs. If the current edge has only one relation at both vertices,
			// create one subgraph with both relations.
			subgraphs.push_back(new_subgraph);
		}
		if (subgraph_connections.size() == 1) {
			// the current edge connections to the subgraph at the index in subgraph_connections
			// add the relations at both ends of the edge to the subgraph. (The subgraph is a JoinRelationSet, double adding relations will be fine).
			auto subgraph_to_update = subgraphs.at(subgraph_connections.at(0));
			subgraph_to_update.relations = &set_manager.Union(*subgraph_to_update.relations, edge.filter_info->set);
			subgraph_to_update.numerator_relations = &UpdateNumeratorRelations(subgraph_to_update, new_subgraph, edge);
			subgraph_to_update.denom = CalculateUpdatedDemo(subgraph_to_update, new_subgraph, edge);
		}
		if (subgraph_connections.size() == 2) {
			// The two subgraphs in the subgraph_connections can be merged by this edge.
			auto subgraph_to_merge_into = subgraphs.at(subgraph_connections.at(0));
			auto subgraph_to_delete = subgraphs.at(subgraph_connections.at(1));
			subgraph_to_merge_into.relations = &set_manager.Union(*subgraph_to_merge_into.relations, *subgraph_to_delete.relations);
			subgraph_to_merge_into.numerator_relations = &UpdateNumeratorRelations(subgraph_to_merge_into, subgraph_to_delete, edge);
			subgraph_to_delete.numerator_relations = nullptr;
			subgraph_to_merge_into.denom = CalculateUpdatedDemo(subgraph_to_merge_into, subgraph_to_delete, edge);
			auto remove_start = std::remove_if(subgraphs.begin(), subgraphs.end(),
					                                   [](Subgraph2Denominator &s) { return !s.relations; });
			subgraphs.erase(remove_start, subgraphs.end());
		}
		if (subgraphs.size() == 1 && subgraphs.at(0).relations->count == set.count) {
			// the first subgraph has connected all of the desired relations, no need to iterate
			// through the rest of the edges.
			break;
		}
	}

//	for (auto &relation_2_tdom : relations_to_tdoms) {
//		// loop through each filter in the tdom.
//		if (all_relations_joined) {
//			break;
//		}
//
//		for (auto &filter : relation_2_tdom.filters) {
//			if (actual_set.count(filter->left_binding.table_index) == 0 ||
//			    actual_set.count(filter->right_binding.table_index) == 0) {
//				continue;
//			}
//			// the join filter is on relations in the new set.
//			found_match = false;
//			vector<Subgraph2Denominator>::iterator it;
//			for (it = subgraphs.begin(); it != subgraphs.end(); it++) {
//				auto left_in = it->relations.count(filter->left_binding.table_index) > 0;
//				auto right_in = it->relations.count(filter->right_binding.table_index) > 0;
//				if (left_in && right_in) {
//					// if both left and right bindings are in the subgraph, continue.
//					// This means another filter is connecting relations already in the
//					// subgraph it, but it has a tdom that is less, and we don't care.
//					found_match = true;
//					continue;
//				}
//				if (!left_in && !right_in) {
//					// if both left and right bindings are *not* in the subgraph, continue
//					// without finding a match. This will trigger the process to add a new
//					// subgraph
//					continue;
//				}
//				idx_t find_table = left_in ? filter->right_binding.table_index : filter->left_binding.table_index;
//				auto next_subgraph = it + 1;
//				switch (filter->join_type) {
//				case JoinType::INNER: {
//					// iterate through other subgraphs and merge.
//					FindSubgraphMatchAndMerge(*it, find_table, next_subgraph, subgraphs.end());
//					// Now insert the right binding and update denominator with the
//					// tdom of the filter
//					// insert find_table again in case there was no other subgraph.
//					it->relations.insert(find_table);
//					it->numerator_relations.insert(find_table);
//					UpdateDenom(*it, relation_2_tdom, filter);
//					break;
//				}
//				case JoinType::SEMI:
//				case JoinType::ANTI: {
//					// don't insert relations into the numerator_relations.
//					auto left = it;
//					auto right = FindMatchingSubGraph(*it, find_table, next_subgraph, subgraphs.end());
//					if (right != subgraphs.end()) {
//						if (right_in) {
//							std::swap(left, right);
//						}
//						for (auto &relation : right->relations) {
//							left->relations.insert(relation);
//						}
//						right->relations.clear();
//					} else {
//						D_ASSERT(!right_in);
//						left->relations.insert(find_table);
//					}
//					left->numerator_filter_strength *= RelationStatisticsHelper::DEFAULT_SELECTIVITY;
//					break;
//				}
//				default:
//					// cross product.
//					auto left = it;
//					auto right = FindMatchingSubGraph(*it, find_table, next_subgraph, subgraphs.end());
//					if (right != subgraphs.end()) {
//						for (auto &rel : right->numerator_relations) {
//							left->relations.insert(rel);
//							left->numerator_relations.insert(rel);
//						}
//					} else {
//						it->relations.insert(find_table);
//						it->numerator_relations.insert(find_table);
//					}
//					break;
//				}
//				found_match = true;
//				break;
//			}
//			// means that the filter joins relations in the given set, but there is no
//			// connection to any subgraph in subgraphs. Add a new subgraph, and maybe later there will be
//			// a connection.
//			if (!found_match) {
//				subgraphs.emplace_back();
//				auto &subgraph = subgraphs.back();
//				subgraph.relations.insert(filter->left_binding.table_index);
//				subgraph.numerator_relations.insert(filter->left_binding.table_index);
//				if (filter->join_type == JoinType::INNER) {
//					subgraph.relations.insert(filter->right_binding.table_index);
//					subgraph.numerator_relations.insert(filter->right_binding.table_index);
//					UpdateDenom(subgraph, relation_2_tdom, filter);
//				} else if (filter->join_type == JoinType::SEMI || filter->join_type == JoinType::ANTI) {
//					subgraph.relations.insert(filter->right_binding.table_index);
//					// don't insert into numerator relations. cardinality of a semi join is
//					// ({{left_relations}} * default_selectivity)
//					subgraph.numerator_filter_strength *= RelationStatisticsHelper::DEFAULT_SELECTIVITY;
//				}
//			}
//			auto remove_start = std::remove_if(subgraphs.begin(), subgraphs.end(),
//			                                   [](Subgraph2Denominator &s) { return s.relations.empty(); });
//			subgraphs.erase(remove_start, subgraphs.end());
//
//			if (subgraphs.size() == 1 && subgraphs.at(0).relations.size() == set.count) {
//				// You have found enough filters to connect the relations. These are guaranteed
//				// to be the filters with the highest Tdoms.
//				all_relations_joined = true;
//				break;
//			}
//		}
//	}
	// TODO: It's possible cross-products were added and are not present in the filters in the relation_2_tdom
	//       structures. When that's the case, merge all subgraphs
	if (subgraphs.size() > 1) {
		auto final_subgraph = subgraphs.at(0);
		for (auto merge_with = subgraphs.begin() + 1; merge_with != subgraphs.end(); merge_with++) {
			D_ASSERT(final_subgraph.relations && merge_with->relations);
			final_subgraph.relations = &set_manager.Union(*final_subgraph.relations, *merge_with->relations);
			D_ASSERT(final_subgraph.numerator_relations && merge_with->numerator_relations);
			final_subgraph.numerator_relations = &set_manager.Union(*final_subgraph.numerator_relations, *merge_with->numerator_relations);
			final_subgraph.denom *= merge_with->denom;
			final_subgraph.numerator_filter_strength *= merge_with->numerator_filter_strength;
		}
	}
	// can happen if a table has cardinality 0, a tdom is set to 0, or if a cross product is used.
	if (subgraphs.empty() || subgraphs.at(0).denom == 0) {
		// denominator is 1 and numerators are a cross product of cardinalities.
		return DenomInfo(set, 1, 1);
	}
	return DenomInfo(*subgraphs.at(0).relations, subgraphs.at(0).numerator_filter_strength,
	                 subgraphs.at(0).denom);
}

template <>
double CardinalityEstimator::EstimateCardinalityWithSet(JoinRelationSet &new_set) {

	if (relation_set_2_cardinality.find(new_set.ToString()) != relation_set_2_cardinality.end()) {
		return relation_set_2_cardinality[new_set.ToString()].cardinality_before_filters;
	}
	auto denom = GetDenominator(new_set);
	auto numerator = GetNumerator(denom.numerator_relations) * denom.filter_strength;

	double result = numerator / denom.denominator;
	auto new_entry = CardinalityHelper(result, 1);
	relation_set_2_cardinality[new_set.ToString()] = new_entry;
	return result;
}

template <>
idx_t CardinalityEstimator::EstimateCardinalityWithSet(JoinRelationSet &new_set) {
	auto cardinality_as_double = EstimateCardinalityWithSet<double>(new_set);
	auto max = NumericLimits<idx_t>::Maximum();
	// need to add a buffer
	if (cardinality_as_double >= max) {
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
	auto relation_filter = stats.filter_strength;

	auto card_helper = CardinalityHelper(relation_cardinality, relation_filter);
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
			D_ASSERT(binding.column_index < stats.at(binding.table_index).column_names.size());
			string column_name = stats.at(binding.table_index).column_names.at(binding.column_index);
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
