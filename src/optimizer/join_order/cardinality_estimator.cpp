#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <cmath>

namespace duckdb {

static optional_ptr<TableCatalogEntry> GetCatalogTableEntry(LogicalOperator &op) {
	D_ASSERT(op.type == LogicalOperatorType::LOGICAL_GET);
	auto &get = op.Cast<LogicalGet>();
	return get.GetTable();
}

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
	relations_to_tdoms.emplace_back(column_binding_set_t({key}));
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
		// an equivalence relation is connecting to sets of equivalence relations
		// so push all relations from the second set into the first. Later we will delete
		// the second set.
		for (ColumnBinding i : relations_to_tdoms.at(matching_equivalent_sets[1]).equivalent_relations) {
			relations_to_tdoms.at(matching_equivalent_sets[0]).equivalent_relations.insert(i);
		}
		relations_to_tdoms.at(matching_equivalent_sets[1]).equivalent_relations.clear();
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

void CardinalityEstimator::AddRelationToColumnMapping(ColumnBinding key, ColumnBinding value) {
	relation_column_to_original_column[key] = value;
}

void CardinalityEstimator::CopyRelationMap(column_binding_map_t<ColumnBinding> &child_binding_map) {
	for (auto &binding_map : relation_column_to_original_column) {
		D_ASSERT(child_binding_map.find(binding_map.first) == child_binding_map.end());
		child_binding_map[binding_map.first] = binding_map.second;
	}
}

void CardinalityEstimator::AddColumnToRelationMap(idx_t table_index, idx_t column_index) {
	relation_attributes[table_index].columns.insert(column_index);
}

void CardinalityEstimator::InitEquivalentRelations(vector<unique_ptr<FilterInfo>> &filter_infos) {
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
}

void CardinalityEstimator::VerifySymmetry(JoinNode &result, JoinNode &entry) {
	if (result.GetCardinality<double>() != entry.GetCardinality<double>()) {
		// Currently it's possible that some entries are cartesian joins.
		// When this is the case, you don't always have symmetry, but
		// if the cost of the result is less, then just assure the cardinality
		// is also less, then you have the same effect of symmetry.
		D_ASSERT(ceil(result.GetCardinality<double>()) <= ceil(entry.GetCardinality<double>()) ||
		         floor(result.GetCardinality<double>()) <= floor(entry.GetCardinality<double>()));
	}
}

void CardinalityEstimator::InitTotalDomains() {
	auto remove_start = std::remove_if(relations_to_tdoms.begin(), relations_to_tdoms.end(),
	                                   [](RelationsToTDom &r_2_tdom) { return r_2_tdom.equivalent_relations.empty(); });
	relations_to_tdoms.erase(remove_start, relations_to_tdoms.end());
}

double CardinalityEstimator::ComputeCost(JoinNode &left, JoinNode &right, double expected_cardinality) {
	return expected_cardinality + left.GetCost() + right.GetCost();
}

double CardinalityEstimator::EstimateCrossProduct(const JoinNode &left, const JoinNode &right) {
	// need to explicity use double here, otherwise auto converts it to an int, then
	// there is an autocast in the return.
	if (left.GetCardinality<double>() >= (NumericLimits<double>::Maximum() / right.GetCardinality<double>())) {
		return NumericLimits<double>::Maximum();
	}
	return left.GetCardinality<double>() * right.GetCardinality<double>();
}

void CardinalityEstimator::AddRelationColumnMapping(LogicalGet &get, idx_t relation_id) {
	for (idx_t it = 0; it < get.column_ids.size(); it++) {
		auto key = ColumnBinding(relation_id, it);
		auto value = ColumnBinding(get.table_index, get.column_ids[it]);
		AddRelationToColumnMapping(key, value);
	}
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

double CardinalityEstimator::EstimateCardinalityWithSet(JoinRelationSet &new_set) {
	double numerator = 1;
	unordered_set<idx_t> actual_set;
	for (idx_t i = 0; i < new_set.count; i++) {
		numerator *= relation_attributes[new_set.relations[i]].cardinality;
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
		// It's possible that in production, one of the D_ASSERTS above will fail and not all subgraphs
		// were connected. When this happens, just use the largest denominator of all the subgraphs.
		if (match.denom > denom) {
			denom = match.denom;
		}
	}
	// can happen if a table has cardinality 0, or a tdom is set to 0
	if (denom == 0) {
		denom = 1;
	}
	return numerator / denom;
}

static bool IsLogicalFilter(LogicalOperator &op) {
	return op.type == LogicalOperatorType::LOGICAL_FILTER;
}

static optional_ptr<LogicalGet> GetLogicalGet(LogicalOperator &op, idx_t table_index = DConstants::INVALID_INDEX) {
	optional_ptr<LogicalGet> get;
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		get = &op.Cast<LogicalGet>();
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
		get = GetLogicalGet(*op.children.at(0), table_index);
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		get = GetLogicalGet(*op.children.at(0), table_index);
		break;
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		// We should never be calling GetLogicalGet without a valid table_index.
		// We are attempting to get the catalog table for a relation (for statistics/cardinality estimation)
		// A logical join means there is a non-reorderable relation in the join plan. This means we need
		// to know the exact table index to return.
		D_ASSERT(table_index != DConstants::INVALID_INDEX);
		if (join.join_type == JoinType::MARK || join.join_type == JoinType::LEFT) {
			auto &left_child = *join.children.at(0);
			get = GetLogicalGet(left_child, table_index);
			if (get && get->table_index == table_index) {
				return get;
			}
			auto &right_child = *join.children.at(1);
			get = GetLogicalGet(right_child, table_index);
			if (get && get->table_index == table_index) {
				return get;
			}
		}
		break;
	}
	default:
		// return null pointer, maybe there is no logical get under this child
		break;
	}
	return get;
}

void CardinalityEstimator::MergeBindings(idx_t binding_index, idx_t relation_id,
                                         vector<column_binding_map_t<ColumnBinding>> &child_binding_maps) {
	for (auto &map_set : child_binding_maps) {
		for (auto &mapping : map_set) {
			ColumnBinding relation_bindings = mapping.first;
			ColumnBinding actual_bindings = mapping.second;

			if (actual_bindings.table_index == binding_index) {
				auto key = ColumnBinding(relation_id, relation_bindings.column_index);
				AddRelationToColumnMapping(key, actual_bindings);
			}
		}
	}
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

void CardinalityEstimator::InitCardinalityEstimatorProps(vector<NodeOp> &node_ops,
                                                         vector<unique_ptr<FilterInfo>> &filter_infos) {
	InitEquivalentRelations(filter_infos);
	InitTotalDomains();
	for (idx_t i = 0; i < node_ops.size(); i++) {
		auto &join_node = *node_ops[i].node;
		auto &op = node_ops[i].op;
		join_node.SetBaseTableCardinality(op.EstimateCardinality(context));
		if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = op.Cast<LogicalComparisonJoin>();
			if (join.join_type == JoinType::LEFT) {
				// If a base op is a Logical Comparison join it is probably a left join,
				// so the cost of the larger table is a fine estimate.
				// TODO: provide better estimates for cost of mark joins
				// MARK joins are used for anti and semi joins, so the cost can conceivably be
				// less than the base table cardinality.
				join_node.SetCost(join_node.GetBaseTableCardinality());
			}
		} else if (op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			// AsOf joins have the cardinality of the LHS
			join_node.SetCost(join_node.GetBaseTableCardinality());
		}
		// Total domains can be affected by filters. So we update base table cardinality first
		EstimateBaseTableCardinality(join_node, op);
		// Then update total domains.
		UpdateTotalDomains(join_node, op);
	}

	// sort relations from greatest tdom to lowest tdom.
	std::sort(relations_to_tdoms.begin(), relations_to_tdoms.end(), SortTdoms);
}

void CardinalityEstimator::UpdateTotalDomains(JoinNode &node, LogicalOperator &op) {
	auto relation_id = node.set.relations[0];
	relation_attributes[relation_id].cardinality = node.GetCardinality<double>();
	//! Initialize the distinct count for all columns used in joins with the current relation.
	idx_t distinct_count = node.GetBaseTableCardinality();
	optional_ptr<TableCatalogEntry> catalog_table;

	optional_ptr<LogicalGet> get;
	bool get_updated = true;
	for (auto &column : relation_attributes[relation_id].columns) {
		//! for every column used in a filter in the relation, get the distinct count via HLL, or assume it to be
		//! the cardinality
		ColumnBinding key = ColumnBinding(relation_id, column);
		auto actual_binding = relation_column_to_original_column.find(key);
		// each relation has columns that are either projected or used as filters
		// In order to get column statistics we need to make sure the actual binding still
		// refers to the same base table relation, as non-reorderable joins may involve 2+
		// base table relations and therefore the columns may also refer to 2 different
		// base table relations
		if (actual_binding != relation_column_to_original_column.end() &&
		    (!get || get->table_index != actual_binding->second.table_index)) {
			get = GetLogicalGet(op, actual_binding->second.table_index);
			get_updated = true;
		} else {
			get_updated = false;
		}

		if (get_updated) {
			if (get) {
				catalog_table = GetCatalogTableEntry(*get);
			} else {
				catalog_table = nullptr;
			}
		}

		if (catalog_table && actual_binding != relation_column_to_original_column.end()) {
			// Get HLL stats here
			auto base_stats = catalog_table->GetStatistics(context, actual_binding->second.column_index);
			if (base_stats) {
				distinct_count = base_stats->GetDistinctCount();
			}

			// HLL has estimation error, distinct_count can't be greater than cardinality of the table before filters
			if (distinct_count > node.GetBaseTableCardinality()) {
				distinct_count = node.GetBaseTableCardinality();
			}
		} else {
			distinct_count = node.GetBaseTableCardinality();
		}
		// Update the relation_to_tdom set with the estimated distinct count (or tdom) calculated above
		for (auto &relation_to_tdom : relations_to_tdoms) {
			column_binding_set_t i_set = relation_to_tdom.equivalent_relations;
			if (i_set.count(key) != 1) {
				continue;
			}
			if (catalog_table) {
				if (relation_to_tdom.tdom_hll < distinct_count) {
					relation_to_tdom.tdom_hll = distinct_count;
					relation_to_tdom.has_tdom_hll = true;
				}
				if (relation_to_tdom.tdom_no_hll > distinct_count) {
					relation_to_tdom.tdom_no_hll = distinct_count;
				}
			} else {
				// Here we don't have catalog statistics, and the following is how we determine
				// the tdom
				// 1. If there is any hll data in the equivalence set, use that
				// 2. Otherwise, use the table with the smallest cardinality
				if (relation_to_tdom.tdom_no_hll > distinct_count && !relation_to_tdom.has_tdom_hll) {
					relation_to_tdom.tdom_no_hll = distinct_count;
				}
			}
			break;
		}
	}
}

optional_ptr<TableFilterSet> CardinalityEstimator::GetTableFilters(LogicalOperator &op, idx_t table_index) {
	auto get = GetLogicalGet(op, table_index);
	return get ? &get->table_filters : nullptr;
}

idx_t CardinalityEstimator::InspectConjunctionAND(idx_t cardinality, idx_t column_index, ConjunctionAndFilter &filter,
                                                  unique_ptr<BaseStatistics> base_stats) {
	auto has_equality_filter = false;
	auto cardinality_after_filters = cardinality;
	for (auto &child_filter : filter.child_filters) {
		if (child_filter->filter_type != TableFilterType::CONSTANT_COMPARISON) {
			continue;
		}
		auto &comparison_filter = child_filter->Cast<ConstantFilter>();
		if (comparison_filter.comparison_type != ExpressionType::COMPARE_EQUAL) {
			continue;
		}
		auto column_count = 0;
		if (base_stats) {
			column_count = base_stats->GetDistinctCount();
		}
		auto filtered_card = cardinality;
		// column_count = 0 when there is no column count (i.e parquet scans)
		if (column_count > 0) {
			// we want the ceil of cardinality/column_count. We also want to avoid compiler errors
			filtered_card = (cardinality + column_count - 1) / column_count;
			cardinality_after_filters = filtered_card;
		}
		if (has_equality_filter) {
			cardinality_after_filters = MinValue(filtered_card, cardinality_after_filters);
		}
		has_equality_filter = true;
	}
	return cardinality_after_filters;
}

idx_t CardinalityEstimator::InspectConjunctionOR(idx_t cardinality, idx_t column_index, ConjunctionOrFilter &filter,
                                                 unique_ptr<BaseStatistics> base_stats) {
	auto has_equality_filter = false;
	auto cardinality_after_filters = cardinality;
	for (auto &child_filter : filter.child_filters) {
		if (child_filter->filter_type != TableFilterType::CONSTANT_COMPARISON) {
			continue;
		}
		auto &comparison_filter = child_filter->Cast<ConstantFilter>();
		if (comparison_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
			auto column_count = cardinality_after_filters;
			if (base_stats) {
				column_count = base_stats->GetDistinctCount();
			}
			auto increment = MaxValue<idx_t>(((cardinality + column_count - 1) / column_count), 1);
			if (has_equality_filter) {
				cardinality_after_filters += increment;
			} else {
				cardinality_after_filters = increment;
			}
			has_equality_filter = true;
		}
	}
	D_ASSERT(cardinality_after_filters > 0);
	return cardinality_after_filters;
}

idx_t CardinalityEstimator::InspectTableFilters(idx_t cardinality, LogicalOperator &op, TableFilterSet &table_filters,
                                                idx_t table_index) {
	idx_t cardinality_after_filters = cardinality;
	auto get = GetLogicalGet(op, table_index);
	unique_ptr<BaseStatistics> column_statistics;
	for (auto &it : table_filters.filters) {
		column_statistics = nullptr;
		if (get->bind_data && get->function.name.compare("seq_scan") == 0) {
			auto &table_scan_bind_data = get->bind_data->Cast<TableScanBindData>();
			column_statistics = get->function.statistics(context, &table_scan_bind_data, it.first);
		}
		if (it.second->filter_type == TableFilterType::CONJUNCTION_AND) {
			auto &filter = it.second->Cast<ConjunctionAndFilter>();
			idx_t cardinality_with_and_filter =
			    InspectConjunctionAND(cardinality, it.first, filter, std::move(column_statistics));
			cardinality_after_filters = MinValue(cardinality_after_filters, cardinality_with_and_filter);
		} else if (it.second->filter_type == TableFilterType::CONJUNCTION_OR) {
			auto &filter = it.second->Cast<ConjunctionOrFilter>();
			idx_t cardinality_with_or_filter =
			    InspectConjunctionOR(cardinality, it.first, filter, std::move(column_statistics));
			cardinality_after_filters = MinValue(cardinality_after_filters, cardinality_with_or_filter);
		}
	}
	// if the above code didn't find an equality filter (i.e country_code = "[us]")
	// and there are other table filters, use default selectivity.
	bool has_equality_filter = (cardinality_after_filters != cardinality);
	if (!has_equality_filter && !table_filters.filters.empty()) {
		cardinality_after_filters = MaxValue<idx_t>(cardinality * DEFAULT_SELECTIVITY, 1);
	}
	return cardinality_after_filters;
}

void CardinalityEstimator::EstimateBaseTableCardinality(JoinNode &node, LogicalOperator &op) {
	auto has_logical_filter = IsLogicalFilter(op);
	D_ASSERT(node.set.count == 1);
	auto relation_id = node.set.relations[0];

	double lowest_card_found = node.GetBaseTableCardinality();
	for (auto &column : relation_attributes[relation_id].columns) {
		auto card_after_filters = node.GetBaseTableCardinality();
		ColumnBinding key = ColumnBinding(relation_id, column);
		optional_ptr<TableFilterSet> table_filters;
		auto actual_binding = relation_column_to_original_column.find(key);
		if (actual_binding != relation_column_to_original_column.end()) {
			table_filters = GetTableFilters(op, actual_binding->second.table_index);
		}

		if (table_filters) {
			double inspect_result =
			    (double)InspectTableFilters(card_after_filters, op, *table_filters, actual_binding->second.table_index);
			card_after_filters = MinValue(inspect_result, (double)card_after_filters);
		}
		if (has_logical_filter) {
			card_after_filters *= DEFAULT_SELECTIVITY;
		}
		lowest_card_found = MinValue(card_after_filters, lowest_card_found);
	}
	node.SetEstimatedCardinality(lowest_card_found);
}

} // namespace duckdb
