#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

namespace duckdb {

static TableCatalogEntry *GetCatalogTableEntry(LogicalOperator *op) {
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto get = (LogicalGet *)op;
		TableCatalogEntry *entry = get->GetTable();
		return entry;
	}
	for (auto &child : op->children) {
		TableCatalogEntry *entry = GetCatalogTableEntry(child.get());
		if (entry != nullptr) {
			return entry;
		}
	}
	return nullptr;
}

// The filter was made on top of a logical sample or other projection,
// but no specific columns are referenced. See issue 4978 number 4.
bool CardinalityEstimator::EmptyFilter(FilterInfo *filter_info) {
	if (!filter_info->left_set && !filter_info->right_set) {
		return true;
	}
	return false;
}

void CardinalityEstimator::AddRelationTdom(FilterInfo *filter_info) {
	D_ASSERT(filter_info->set->count >= 1);
	for (const RelationsToTDom &r2tdom : relations_to_tdoms) {
		auto &i_set = r2tdom.equivalent_relations;
		if (i_set.find(filter_info->left_binding) != i_set.end()) {
			// found an equivalent filter
			return;
		}
	}
	auto key = ColumnBinding(filter_info->left_binding.table_index, filter_info->left_binding.column_index);
	column_binding_set_t tmp({key});
	relations_to_tdoms.emplace_back(RelationsToTDom(tmp));
}

bool CardinalityEstimator::SingleColumnFilter(FilterInfo *filter_info) {
	if (filter_info->left_set && filter_info->right_set) {
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
		relations_to_tdoms.emplace_back(RelationsToTDom(tmp));
		relations_to_tdoms.back().filters.push_back(filter_info);
	}
}

void CardinalityEstimator::AddRelationToColumnMapping(ColumnBinding key, ColumnBinding value) {
	relation_column_to_original_column[key] = value;
}

void CardinalityEstimator::CopyRelationMap(column_binding_map_t<ColumnBinding> &child_binding_map) {
	for (auto &binding_map : relation_column_to_original_column) {
		child_binding_map[binding_map.first] = binding_map.second;
	}
}

void CardinalityEstimator::AddColumnToRelationMap(idx_t table_index, idx_t column_index) {
	relation_attributes[table_index].columns.insert(column_index);
}

void CardinalityEstimator::InitEquivalentRelations(vector<unique_ptr<FilterInfo>> *filter_infos) {
	// For each filter, we fill keep track of the index of the equivalent relation set
	// the left and right relation needs to be added to.
	for (auto &filter : *filter_infos) {
		if (SingleColumnFilter(filter.get())) {
			// Filter on one relation, (i.e string or range filter on a column).
			// Grab the first relation and add it to  the equivalence_relations
			AddRelationTdom(filter.get());
			continue;
		} else if (EmptyFilter(filter.get())) {
			continue;
		}
		D_ASSERT(filter->left_set->count >= 1);
		D_ASSERT(filter->right_set->count >= 1);

		auto matching_equivalent_sets = DetermineMatchingEquivalentSets(filter.get());
		AddToEquivalenceSets(filter.get(), matching_equivalent_sets);
	}
}

void CardinalityEstimator::VerifySymmetry(JoinNode *result, JoinNode *entry) {
	if (result->GetCardinality<double>() != entry->GetCardinality<double>()) {
		// Currently it's possible that some entries are cartesian joins.
		// When this is the case, you don't always have symmetry, but
		// if the cost of the result is less, then just assure the cardinality
		// is also less, then you have the same effect of symmetry.
		D_ASSERT(ceil(result->GetCardinality<double>()) <= ceil(entry->GetCardinality<double>()) ||
		         floor(result->GetCardinality<double>()) <= floor(entry->GetCardinality<double>()));
	}
}

void CardinalityEstimator::InitTotalDomains() {
	auto remove_start = std::remove_if(relations_to_tdoms.begin(), relations_to_tdoms.end(),
	                                   [](RelationsToTDom &r_2_tdom) { return r_2_tdom.equivalent_relations.empty(); });
	relations_to_tdoms.erase(remove_start, relations_to_tdoms.end());
}

double CardinalityEstimator::ComputeCost(JoinNode *left, JoinNode *right, double expected_cardinality) {
	return expected_cardinality + left->GetCost() + right->GetCost();
}

double CardinalityEstimator::EstimateCrossProduct(const JoinNode *left, const JoinNode *right) {
	// need to explicity use double here, otherwise auto converts it to an int, then
	// there is an autocast in the return.
	return left->GetCardinality<double>() >= (NumericLimits<double>::Maximum() / right->GetCardinality<double>())
	           ? NumericLimits<double>::Maximum()
	           : left->GetCardinality<double>() * right->GetCardinality<double>();
}

void CardinalityEstimator::AddRelationColumnMapping(LogicalGet *get, idx_t relation_id) {
	for (idx_t it = 0; it < get->column_ids.size(); it++) {
		auto key = ColumnBinding(relation_id, it);
		auto value = ColumnBinding(get->table_index, get->column_ids[it]);
		AddRelationToColumnMapping(key, value);
	}
}

void UpdateDenom(Subgraph2Denominator *relation_2_denom, RelationsToTDom *relation_to_tdom) {
	relation_2_denom->denom *=
	    relation_to_tdom->has_tdom_hll ? relation_to_tdom->tdom_hll : relation_to_tdom->tdom_no_hll;
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

double CardinalityEstimator::EstimateCardinalityWithSet(JoinRelationSet *new_set) {
	double numerator = 1;
	unordered_set<idx_t> actual_set;
	for (idx_t i = 0; i < new_set->count; i++) {
		numerator *= relation_attributes[new_set->relations[i]].cardinality;
		actual_set.insert(new_set->relations[i]);
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
				UpdateDenom(&(*it), &relation_2_tdom);
				found_match = true;
				break;
			}
			// means that the filter joins relations in the given set, but there is no
			// connection to any subgraph in subgraphs. Add a new subgraph, and maybe later there will be
			// a connection.
			if (!found_match) {
				subgraphs.emplace_back(Subgraph2Denominator());
				auto subgraph = &subgraphs.back();
				subgraph->relations.insert(filter->left_binding.table_index);
				subgraph->relations.insert(filter->right_binding.table_index);
				UpdateDenom(subgraph, &relation_2_tdom);
			}
			auto remove_start = std::remove_if(subgraphs.begin(), subgraphs.end(),
			                                   [](Subgraph2Denominator &s) { return s.relations.empty(); });
			subgraphs.erase(remove_start, subgraphs.end());

			if (subgraphs.size() == 1 && subgraphs.at(0).relations.size() == new_set->count) {
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

static bool IsLogicalFilter(LogicalOperator *op) {
	return op->type == LogicalOperatorType::LOGICAL_FILTER;
}

static LogicalGet *GetLogicalGet(LogicalOperator *op) {
	LogicalGet *get = nullptr;
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_GET:
		get = (LogicalGet *)op;
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
		get = GetLogicalGet(op->children.at(0).get());
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		get = GetLogicalGet(op->children.at(0).get());
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		LogicalComparisonJoin *join = (LogicalComparisonJoin *)op;
		if (join->join_type == JoinType::MARK || join->join_type == JoinType::LEFT) {
			auto child = join->children.at(0).get();
			get = GetLogicalGet(child);
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

void CardinalityEstimator::InitCardinalityEstimatorProps(vector<struct NodeOp> *node_ops,
                                                         vector<unique_ptr<FilterInfo>> *filter_infos) {
	InitEquivalentRelations(filter_infos);
	InitTotalDomains();
	for (idx_t i = 0; i < node_ops->size(); i++) {
		auto join_node = (*node_ops)[i].node.get();
		auto op = (*node_ops)[i].op;
		join_node->SetBaseTableCardinality(op->EstimateCardinality(context));
		if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = (LogicalComparisonJoin &)*op;
			if (join.join_type == JoinType::LEFT) {
				// TODO: inspect child operators to get a more accurate cost
				// and cardinality estimation. If an base op is a Logical Comparison join
				// it is probably a left join, so cost of the larger table is a fine
				// estimate
				// No need to update a mark join cost because I say so.
				join_node->SetCost(join_node->GetBaseTableCardinality());
			}
		}
		// update cardinality with filters
		EstimateBaseTableCardinality(join_node, op);
		UpdateTotalDomains(join_node, op);
	}

	// sort relations from greatest tdom to lowest tdom.
	std::sort(relations_to_tdoms.begin(), relations_to_tdoms.end(), SortTdoms);
}

void CardinalityEstimator::UpdateTotalDomains(JoinNode *node, LogicalOperator *op) {
	auto relation_id = node->set->relations[0];
	relation_attributes[relation_id].cardinality = node->GetCardinality<double>();
	TableCatalogEntry *catalog_table = nullptr;
	auto get = GetLogicalGet(op);
	if (get) {
		catalog_table = GetCatalogTableEntry(get);
	}

	//! Initialize the tdoms for all columns the relation uses in join conditions.
	unordered_set<idx_t>::iterator ite;
	idx_t count = node->GetBaseTableCardinality();

	bool direct_filter = false;
	for (auto &column : relation_attributes[relation_id].columns) {
		//! for every column in the relation, get the count via either HLL, or assume it to be
		//! the cardinality
		ColumnBinding key = ColumnBinding(relation_id, column);

		if (catalog_table) {
			relation_attributes[relation_id].original_name = catalog_table->name;
			// Get HLL stats here
			auto actual_binding = relation_column_to_original_column[key];

			auto base_stats = catalog_table->GetStatistics(context, actual_binding.column_index);
			if (base_stats) {
				count = base_stats->GetDistinctCount();
			}

			// means you have a direct filter on a column. The count/total domain for the column
			// should be decreased to match the predicted total domain matching the filter.
			// We decrease the total domain for all columns in the equivalence set because filter pushdown
			// will mean all columns are affected.
			if (direct_filter) {
				count = node->GetCardinality<idx_t>();
			}

			// HLL has estimation error, count can't be greater than cardinality of the table before filters
			if (count > node->GetBaseTableCardinality()) {
				count = node->GetBaseTableCardinality();
			}
		} else {
			// No HLL. So if we know there is a direct filter, reduce count to cardinality with filter
			// otherwise assume the total domain is still the cardinality
			if (direct_filter) {
				count = node->GetCardinality<idx_t>();
			} else {
				count = node->GetBaseTableCardinality();
			}
		}

		for (auto &relation_to_tdom : relations_to_tdoms) {
			column_binding_set_t i_set = relation_to_tdom.equivalent_relations;
			if (i_set.count(key) != 1) {
				continue;
			}
			if (catalog_table) {
				if (relation_to_tdom.tdom_hll < count) {
					relation_to_tdom.tdom_hll = count;
					relation_to_tdom.has_tdom_hll = true;
				}
				if (relation_to_tdom.tdom_no_hll > count) {
					relation_to_tdom.tdom_no_hll = count;
				}
			} else {
				// Here we don't have catalog statistics, and the following is how we determine
				// the tdom
				// 1. If there is any hll data in the equivalence set, use that
				// 2. Otherwise, use the table with the smallest cardinality
				if (relation_to_tdom.tdom_no_hll > count && !relation_to_tdom.has_tdom_hll) {
					relation_to_tdom.tdom_no_hll = count;
				}
			}
			break;
		}
	}
}

TableFilterSet *CardinalityEstimator::GetTableFilters(LogicalOperator *op) {
	// First check table filters
	auto get = GetLogicalGet(op);
	return get ? &get->table_filters : nullptr;
}

idx_t CardinalityEstimator::InspectConjunctionAND(idx_t cardinality, idx_t column_index, ConjunctionAndFilter *filter,
                                                  unique_ptr<BaseStatistics> base_stats) {
	auto has_equality_filter = false;
	auto cardinality_after_filters = cardinality;
	for (auto &child_filter : filter->child_filters) {
		if (child_filter->filter_type != TableFilterType::CONSTANT_COMPARISON) {
			continue;
		}
		auto comparison_filter = (ConstantFilter &)*child_filter;
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

idx_t CardinalityEstimator::InspectConjunctionOR(idx_t cardinality, idx_t column_index, ConjunctionOrFilter *filter,
                                                 unique_ptr<BaseStatistics> base_stats) {
	auto has_equality_filter = false;
	auto cardinality_after_filters = cardinality;
	for (auto &child_filter : filter->child_filters) {
		if (child_filter->filter_type != TableFilterType::CONSTANT_COMPARISON) {
			continue;
		}
		auto comparison_filter = (ConstantFilter &)*child_filter;
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

idx_t CardinalityEstimator::InspectTableFilters(idx_t cardinality, LogicalOperator *op, TableFilterSet *table_filters) {
	idx_t cardinality_after_filters = cardinality;
	auto get = GetLogicalGet(op);
	unique_ptr<BaseStatistics> column_statistics;
	for (auto &it : table_filters->filters) {
		column_statistics = nullptr;
		if (get->bind_data && get->function.name.compare("seq_scan") == 0) {
			auto &table_scan_bind_data = (TableScanBindData &)*get->bind_data;
			column_statistics = get->function.statistics(context, &table_scan_bind_data, it.first);
		}
		if (it.second->filter_type == TableFilterType::CONJUNCTION_AND) {
			auto &filter = (ConjunctionAndFilter &)*it.second;
			idx_t cardinality_with_and_filter =
			    InspectConjunctionAND(cardinality, it.first, &filter, move(column_statistics));
			cardinality_after_filters = MinValue(cardinality_after_filters, cardinality_with_and_filter);
		} else if (it.second->filter_type == TableFilterType::CONJUNCTION_OR) {
			auto &filter = (ConjunctionOrFilter &)*it.second;
			idx_t cardinality_with_or_filter =
			    InspectConjunctionOR(cardinality, it.first, &filter, move(column_statistics));
			cardinality_after_filters = MinValue(cardinality_after_filters, cardinality_with_or_filter);
		}
	}
	// if the above code didn't find an equality filter (i.e country_code = "[us]")
	// and there are other table filters, use default selectivity.
	bool has_equality_filter = (cardinality_after_filters != cardinality);
	if (!has_equality_filter && !table_filters->filters.empty()) {
		cardinality_after_filters = MaxValue<idx_t>(cardinality * DEFAULT_SELECTIVITY, 1);
	}
	return cardinality_after_filters;
}

void CardinalityEstimator::EstimateBaseTableCardinality(JoinNode *node, LogicalOperator *op) {
	auto has_logical_filter = IsLogicalFilter(op);
	auto table_filters = GetTableFilters(op);

	auto card_after_filters = node->GetBaseTableCardinality();
	// Logical Filter on a seq scan
	if (has_logical_filter) {
		card_after_filters *= DEFAULT_SELECTIVITY;
	} else if (table_filters) {
		double inspect_result = (double)InspectTableFilters(card_after_filters, op, table_filters);
		card_after_filters = MinValue(inspect_result, (double)card_after_filters);
	}
	node->SetEstimatedCardinality(card_after_filters);
}

} // namespace duckdb
