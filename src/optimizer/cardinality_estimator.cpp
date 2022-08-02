#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/optimizer/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_node.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
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

bool CardinalityEstimator::SingleColumnFilter(FilterInfo *filter_info) {
	if (filter_info->left_set && filter_info->right_set) {
		// Both set
		return false;
	}
	// Filter on one relation, (i.e string or range filter on a column).
	// Grab the first relation and add it to the the equivalence_relations
	D_ASSERT(filter_info->set->count >= 1);
	for (const column_binding_set_t &i_set : equivalent_relations) {
		if (i_set.count(filter_info->left_binding) > 0) {
			// found an equivalent filter
			return true;
		}
	}
	auto key = ColumnBinding(filter_info->left_binding.table_index, filter_info->left_binding.column_index);
	column_binding_set_t tmp;
	tmp.insert(key);
	equivalent_relations.push_back(tmp);
	return true;
}

vector<idx_t> CardinalityEstimator::DetermineMatchingEquivalentSets(FilterInfo *filter_info) {
	vector<idx_t> matching_equivalent_sets;
	auto equivalent_relation_index = 0;
	// eri = equivalent relation index
	bool added_to_eri;

	for (const column_binding_set_t &i_set : equivalent_relations) {
		added_to_eri = false;
		if (i_set.count(filter_info->left_binding) > 0) {
			matching_equivalent_sets.push_back(equivalent_relation_index);
			added_to_eri = true;
		}
		// don't add both left and right to the matching_equivalent_sets
		// since both left and right get added to that index anyway.
		if (i_set.count(filter_info->right_binding) > 0 && !added_to_eri) {
			matching_equivalent_sets.push_back(equivalent_relation_index);
		}
		equivalent_relation_index += 1;
	}
	return matching_equivalent_sets;
}

void CardinalityEstimator::AddToEquivalenceSets(FilterInfo *filter_info, vector<idx_t> matching_equivalent_sets) {
	if (matching_equivalent_sets.size() > 1) {
		for (ColumnBinding i : equivalent_relations.at(matching_equivalent_sets[1])) {
			equivalent_relations.at(matching_equivalent_sets[0]).insert(i);
		}
		equivalent_relations.at(matching_equivalent_sets[1]).clear();
		// add all values of one set to the other, delete the empty one
	} else if (matching_equivalent_sets.size() == 1) {
		idx_t set_i = matching_equivalent_sets.at(0);
		equivalent_relations.at(set_i).insert(filter_info->left_binding);
		equivalent_relations.at(set_i).insert(filter_info->right_binding);
	} else if (matching_equivalent_sets.empty()) {
		column_binding_set_t tmp;
		tmp.insert(filter_info->left_binding);
		tmp.insert(filter_info->right_binding);
		equivalent_relations.push_back(tmp);
	}
}

void CardinalityEstimator::AssertEquivalentRelationSize() {
	D_ASSERT(equivalent_relations.size() == equivalent_relations_tdom_hll.size());
	D_ASSERT(equivalent_relations.size() == equivalent_relations_tdom_no_hll.size());
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
	vector<idx_t> matching_equivalent_sets;
	for (auto &filter : *filter_infos) {
		matching_equivalent_sets.clear();

		if (SingleColumnFilter(filter.get())) {
			continue;
		}
		D_ASSERT(filter->left_set->count >= 1);
		D_ASSERT(filter->right_set->count >= 1);

		matching_equivalent_sets = DetermineMatchingEquivalentSets(filter.get());
		AddToEquivalenceSets(filter.get(), matching_equivalent_sets);
	}
}

void CardinalityEstimator::VerifySymmetry(JoinNode *result, JoinNode *entry) {
	if (result->GetCardinality() != entry->GetCardinality()) {
		// Currently it's possible that some entries are cartesian joins.
		// When this is the case, you don't always have symmetry, but
		// if the cost of the result is less, then just assure the cardinality
		// is also less, then you have the same effect of symmetry.
		D_ASSERT(ceil(result->GetCardinality()) <= ceil(entry->GetCardinality()) ||
		         floor(result->GetCardinality()) <= floor(entry->GetCardinality()));
	}
}

void CardinalityEstimator::InitTotalDomains() {
	vector<column_binding_set_t>::iterator it;
	// erase empty equivalent relation sets. cast to void because we don't use
	// the returned iterator. Clang-tidy will also complain
	(void)std::remove_if(equivalent_relations.begin(), equivalent_relations.end(),
	                     [](column_binding_set_t &equivalent_rel) { return equivalent_rel.empty(); });

	// initialize equivalent relation tdom vector to have the same size as the
	// equivalent relations.
	for (auto _ : equivalent_relations) {
		equivalent_relations_tdom_hll.push_back(0);
		equivalent_relations_tdom_no_hll.push_back(NumericLimits<idx_t>::Maximum());
	}
}

double CardinalityEstimator::ComputeCost(JoinNode *left, JoinNode *right, double expected_cardinality) {

	double cost = expected_cardinality + left->GetCost() + right->GetCost();
	return cost;
}

idx_t CardinalityEstimator::GetTDom(ColumnBinding binding) {
	idx_t use_ind = 0;
	for (const column_binding_set_t &i_set : equivalent_relations) {
		if (i_set.count(binding) < 1) {
			use_ind += 1;
			continue;
		}
		auto tdom_hll = equivalent_relations_tdom_hll[use_ind];
		if (tdom_hll > 0) {
			return tdom_hll;
		}
		auto tdom_no_hll = equivalent_relations_tdom_no_hll[use_ind];
		if (tdom_no_hll > 0) {
			return tdom_no_hll;
		}
		// The total domain was initialized to 0 (possible when joining with empty tables)
		return 1;
	}
	throw InternalException("Could not get total domain of a join relations. Most likely a bug in InitTdoms");
}

void CardinalityEstimator::ResetCard() {
	lowest_card = NumericLimits<double>::Maximum();
}

void CardinalityEstimator::UpdateLowestcard(double new_card) {
	if (new_card < lowest_card) {
		lowest_card = new_card;
	}
}

double CardinalityEstimator::EstimateCrossProduct(const JoinNode *left, const JoinNode *right) {
	// need to explicity use double here, otherwise auto converts it to an int, then
	// there is an autocast in the return.
	double expected_cardinality = 0;
	if (left->GetCardinality() >= (NumericLimits<double>::Maximum() / right->GetCardinality())) {
		expected_cardinality = NumericLimits<double>::Maximum();
	} else {
		expected_cardinality = left->GetCardinality() * right->GetCardinality();
	}
	lowest_card = MinValue(expected_cardinality, lowest_card);
	return expected_cardinality;
}

void CardinalityEstimator::AddRelationColumnMapping(LogicalGet *get, idx_t relation_id) {
	for (idx_t it = 0; it < get->column_ids.size(); it++) {
		auto key = ColumnBinding(relation_id, it);
		auto value = ColumnBinding(get->table_index, get->column_ids[it]);
		AddRelationToColumnMapping(key, value);
	}
}

double CardinalityEstimator::EstimateCardinality(double left_card, double right_card, ColumnBinding left_binding,
                                                 ColumnBinding right_binding) {
	idx_t tdom_join_right = GetTDom(right_binding);
#ifdef DEBUG
	idx_t tdom_join_left = GetTDom(left_binding);
	D_ASSERT(tdom_join_left == tdom_join_right);
#endif
	D_ASSERT(tdom_join_right != 0);
	D_ASSERT(tdom_join_right != NumericLimits<idx_t>::Maximum());
	auto expected_cardinality = (left_card * right_card) / tdom_join_right;
	return expected_cardinality;
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

	AssertEquivalentRelationSize();
}

void CardinalityEstimator::UpdateTotalDomains(JoinNode *node, LogicalOperator *op) {
	auto relation_id = node->set->relations[0];
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

		// TODO: Go through table filters and find if there is a direct filter
		//  on a join filter
		if (catalog_table) {
			relation_attributes[relation_id].original_name = catalog_table->name;
			// Get HLL stats here
			auto actual_binding = relation_column_to_original_column[key];

			// sometimes base stats is null (test_709.test) returns null for base stats while
			// there is still a catalog table. Anybody know anything about this?
			auto base_stats = catalog_table->storage->GetStatistics(context, actual_binding.column_index);
			if (base_stats) {
				count = base_stats->GetDistinctCount();
			}

			// means you have a direct filter on a column. The count/total domain for the column
			// should be decreased to match the predicted total domain matching the filter.
			// We decrease the total domain for all columns in the equivalence set because filter pushdown
			// will mean all columns are affected.
			if (direct_filter) {
				count = node->GetCardinality();
			}

			// HLL has estimation error, count can't be greater than cardinality of the table before filters
			if (count > node->GetBaseTableCardinality()) {
				count = node->GetBaseTableCardinality();
			}
		} else {
			// No HLL. So if we know there is a direct filter, reduce count to cardinality with filter
			// otherwise assume the total domain is still the cardinality
			if (direct_filter) {
				count = node->GetCardinality();
			} else {
				count = node->GetBaseTableCardinality();
			}
		}

		for (idx_t ind = 0; ind < equivalent_relations.size(); ind++) {
			column_binding_set_t i_set = equivalent_relations.at(ind);
			if (i_set.count(key) == 1) {
				if (catalog_table) {
					if (equivalent_relations_tdom_hll.at(ind) < count) {
						equivalent_relations_tdom_hll.at(ind) = count;
					}
					if (equivalent_relations_tdom_no_hll.at(ind) > count) {
						equivalent_relations_tdom_no_hll.at(ind) = count;
					}
				} else {
					if (equivalent_relations_tdom_no_hll.at(ind) > count) {
						equivalent_relations_tdom_no_hll.at(ind) = count;
					}
					// If we join a parquet/csv table with a catalog table that has HLL
					// then count = cardinality.
					// In this case, prefer the lower values.
					// equivalent_relations_tdom_hll is initialized to 0 for all equivalence relations
					// so check for 0 to make sure an ideal value exists.
					if (equivalent_relations_tdom_hll.at(ind) > count || equivalent_relations_tdom_hll.at(ind) == 0) {
						equivalent_relations_tdom_hll.at(ind) = count;
					}
				}
				break;
			}
		}
	}
}

TableFilterSet *CardinalityEstimator::GetTableFilters(LogicalOperator *op) {
	// First check table filters
	auto get = GetLogicalGet(op);
	if (get) {
		return &get->table_filters;
	}
	return nullptr;
}

idx_t CardinalityEstimator::InspectConjunctionAND(idx_t cardinality, idx_t column_index, ConjunctionAndFilter *filter,
                                                  TableCatalogEntry *catalog_table) {
	auto has_equality_filter = false;
	auto cardinality_after_filters = cardinality;
	for (auto &child_filter : filter->child_filters) {
		if (child_filter->filter_type == TableFilterType::CONSTANT_COMPARISON) {
			auto comparison_filter = (ConstantFilter &)*child_filter;
			if (comparison_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
				auto base_stats = catalog_table->storage->GetStatistics(context, column_index);
				auto column_count = base_stats->GetDistinctCount();
				auto filtered_card = cardinality;
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
		}
	}
	return cardinality_after_filters;
}

idx_t CardinalityEstimator::InspectConjunctionOR(idx_t cardinality, idx_t column_index, ConjunctionOrFilter *filter,
                                                 TableCatalogEntry *catalog_table) {
	auto has_equality_filter = false;
	auto cardinality_after_filters = cardinality;
	for (auto &child_filter : filter->child_filters) {
		if (child_filter->filter_type == TableFilterType::CONSTANT_COMPARISON) {
			auto comparison_filter = (ConstantFilter &)*child_filter;
			if (comparison_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
				auto base_stats = catalog_table->storage->GetStatistics(context, column_index);
				auto column_count = base_stats->GetDistinctCount();
				auto increment = MaxValue((idx_t)((cardinality + column_count - 1) / column_count), (idx_t)1);
				if (has_equality_filter) {
					cardinality_after_filters += increment;
				} else {
					cardinality_after_filters = increment;
				}
				has_equality_filter = true;
			}
		}
	}
	D_ASSERT(cardinality_after_filters > 0);
	return cardinality_after_filters;
}

idx_t CardinalityEstimator::InspectTableFilters(idx_t cardinality, LogicalOperator *op, TableFilterSet *table_filters) {
	idx_t cardinality_after_filters = cardinality;
	auto catalog_table = GetCatalogTableEntry(op);
	unordered_map<idx_t, unique_ptr<TableFilter>>::iterator it;
	if (!catalog_table) {
		return cardinality_after_filters;
	}
	for (auto &it : table_filters->filters) {
		if (it.second->filter_type == TableFilterType::CONJUNCTION_AND) {
			auto &filter = (ConjunctionAndFilter &)*it.second;
			idx_t cardinality_with_and_filter = InspectConjunctionAND(cardinality, it.first, &filter, catalog_table);
			cardinality_after_filters = MinValue(cardinality_after_filters, cardinality_with_and_filter);
		} else if (it.second->filter_type == TableFilterType::CONJUNCTION_OR) {
			auto &filter = (ConjunctionOrFilter &)*it.second;
			idx_t cardinality_with_or_filter = InspectConjunctionOR(cardinality, it.first, &filter, catalog_table);
			cardinality_after_filters = MinValue(cardinality_after_filters, cardinality_with_or_filter);
		}
	}
	// if the above code didn't find an equality filter (i.e country_code = "[us]")
	// and there are other table filters, use default selectivity.
	bool has_equality_filter = (cardinality_after_filters != cardinality);
	if (!has_equality_filter && !table_filters->filters.empty()) {
		cardinality_after_filters = MaxValue((idx_t)(cardinality * DEFAULT_SELECTIVITY), (idx_t)1);
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
