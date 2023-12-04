#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/printer.hpp"
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
			// Filter on one relation, (i.e string or range filter on a column).
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

void UpdateDenom(Subgraph2Denominator &relation_2_denom, RelationsToTDom &relation_to_tdom) {
	relation_2_denom.denom *= relation_to_tdom.has_tdom_hll ? relation_to_tdom.tdom_hll : relation_to_tdom.tdom_no_hll;
}

void FindSubgraphMatchAndMerge(Subgraph2Denominator &merge_to, idx_t find_me,
                               vector<Subgraph2Denominator>::iterator subgraph,
                               vector<Subgraph2Denominator>::iterator end) {
	for (; subgraph != end; subgraph++) {
		if (subgraph->relations.count(find_me) >= 1) {
			for (auto &relation : subgraph->relations) {
				merge_to.relations.insert(relation);
			}
			subgraph->relations.clear();
			merge_to.denom *= subgraph->denom;
			return;
		}
	}
}

template <>
double CardinalityEstimator::EstimateCardinalityWithSet(JoinRelationSet &new_set) {

	if (relation_set_2_cardinality.find(new_set.ToString()) != relation_set_2_cardinality.end()) {
		return relation_set_2_cardinality[new_set.ToString()].cardinality_before_filters;
	}
	double numerator = 1;
	unordered_set<idx_t> actual_set;

	for (idx_t i = 0; i < new_set.count; i++) {
		auto &single_node_set = set_manager.GetJoinRelation(new_set.relations[i]);
		auto card_helper = relation_set_2_cardinality[single_node_set.ToString()];
		numerator *= card_helper.cardinality_before_filters;
		actual_set.insert(new_set.relations[i]);
	}

	vector<Subgraph2Denominator> subgraphs;
	bool done = false;
	bool found_match = false;

	// Finding the denominator is tricky. You need to go through the tdoms in decreasing order
	// Then loop through all filters in the equivalence set of the tdom to see if both the
	// left and right relations are in the new set, if so you can use that filter.
	// You must also make sure that the filters all relations in the given set, so we use subgraphs
	// that should eventually merge into one connected graph that joins all the relations
	// TODO: Implement a method to cache subgraphs so you don't have to build them up every
	// time the cardinality of a new set is requested

	// relations_to_tdoms has already been sorted.
	for (auto &relation_2_tdom : relations_to_tdoms) {
		// loop through each filter in the tdom.
		if (done) {
			break;
		}
		for (auto &filter : relation_2_tdom.filters) {
			if (actual_set.count(filter->left_binding.table_index) == 0 ||
			    actual_set.count(filter->right_binding.table_index) == 0) {
				continue;
			}
			// the join filter is on relations in the new set.
			found_match = false;
			vector<Subgraph2Denominator>::iterator it;
			for (it = subgraphs.begin(); it != subgraphs.end(); it++) {
				auto left_in = it->relations.count(filter->left_binding.table_index);
				auto right_in = it->relations.count(filter->right_binding.table_index);
				if (left_in && right_in) {
					// if both left and right bindings are in the subgraph, continue.
					// This means another filter is connecting relations already in the
					// subgraph it, but it has a tdom that is less, and we don't care.
					found_match = true;
					continue;
				}
				if (!left_in && !right_in) {
					// if both left and right bindings are *not* in the subgraph, continue
					// without finding a match. This will trigger the process to add a new
					// subgraph
					continue;
				}
				idx_t find_table;
				if (left_in) {
					find_table = filter->right_binding.table_index;
				} else {
					D_ASSERT(right_in);
					find_table = filter->left_binding.table_index;
				}
				auto next_subgraph = it + 1;
				// iterate through other subgraphs and merge.
				FindSubgraphMatchAndMerge(*it, find_table, next_subgraph, subgraphs.end());
				// Now insert the right binding and update denominator with the
				// tdom of the filter
				it->relations.insert(find_table);
				UpdateDenom(*it, relation_2_tdom);
				found_match = true;
				break;
			}
			// means that the filter joins relations in the given set, but there is no
			// connection to any subgraph in subgraphs. Add a new subgraph, and maybe later there will be
			// a connection.
			if (!found_match) {
				subgraphs.emplace_back();
				auto &subgraph = subgraphs.back();
				subgraph.relations.insert(filter->left_binding.table_index);
				subgraph.relations.insert(filter->right_binding.table_index);
				UpdateDenom(subgraph, relation_2_tdom);
			}
			auto remove_start = std::remove_if(subgraphs.begin(), subgraphs.end(),
			                                   [](Subgraph2Denominator &s) { return s.relations.empty(); });
			subgraphs.erase(remove_start, subgraphs.end());

			if (subgraphs.size() == 1 && subgraphs.at(0).relations.size() == new_set.count) {
				// You have found enough filters to connect the relations. These are guaranteed
				// to be the filters with the highest Tdoms.
				done = true;
				break;
			}
		}
	}
	double denom = 1;
	// TODO: It's possible cross-products were added and are not present in the filters in the relation_2_tdom
	//       structures. When that's the case, multiply the denom structures that have no intersection
	for (auto &match : subgraphs) {
		denom *= match.denom;
	}
	// can happen if a table has cardinality 0, or a tdom is set to 0
	if (denom == 0) {
		denom = 1;
	}
	auto result = numerator / denom;
	auto new_entry = CardinalityHelper((double)result, 1);
	relation_set_2_cardinality[new_set.ToString()] = new_entry;
	return result;
}

template <>
idx_t CardinalityEstimator::EstimateCardinalityWithSet(JoinRelationSet &new_set) {
	auto cardinality_as_double = EstimateCardinalityWithSet<double>(new_set);
	auto max = NumericLimits<idx_t>::Maximum();
	if (cardinality_as_double > max) {
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
