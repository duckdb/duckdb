#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

namespace duckdb {

static ExpressionBinding GetChildColumnBinding(Expression &expr) {
	auto ret = ExpressionBinding();
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_FUNCTION: {
		// TODO: Other expression classes that can have 0 children?
		auto &func = expr.Cast<BoundFunctionExpression>();
		// no children some sort of gen_random_uuid() or equivalent.
		if (func.children.empty()) {
			ret.found_expression = true;
			ret.expression_is_constant = true;
			return ret;
		}
		break;
	}
	case ExpressionClass::BOUND_COLUMN_REF: {
		ret.found_expression = true;
		auto &new_col_ref = expr.Cast<BoundColumnRefExpression>();
		ret.child_binding = ColumnBinding(new_col_ref.binding.table_index, new_col_ref.binding.column_index);
		return ret;
	}
	case ExpressionClass::BOUND_LAMBDA_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_DEFAULT:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::BOUND_REF:
		ret.found_expression = true;
		ret.expression_is_constant = true;
		return ret;
	default:
		break;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &child) {
		auto recursive_result = GetChildColumnBinding(*child);
		if (recursive_result.found_expression) {
			ret = recursive_result;
		}
	});
	// we didn't find a Bound Column Ref
	return ret;
}

RelationStats RelationStatisticsHelper::ExtractGetStats(LogicalGet &get, ClientContext &context) {
	auto return_stats = RelationStats();

	auto base_table_cardinality = get.EstimateCardinality(context);
	auto cardinality_after_filters = base_table_cardinality;
	unique_ptr<BaseStatistics> column_statistics;

	auto table_thing = get.GetTable();
	auto name = string("some table");
	if (table_thing) {
		name = table_thing->name;
		return_stats.table_name = name;
	}

	// if we can get the catalog table, then our column statistics will be accurate
	// parquet readers etc. will still return statistics, but they initialize distinct column
	// counts to 0.
	// TODO: fix this, some file formats can encode distinct counts, we don't want to rely on
	//  getting a catalog table to know that we can use statistics.
	bool have_catalog_table_statistics = false;
	if (get.GetTable()) {
		have_catalog_table_statistics = true;
	}

	// first push back basic distinct counts for each column (if we have them).
	for (idx_t i = 0; i < get.column_ids.size(); i++) {
		bool have_distinct_count_stats = false;
		if (get.function.statistics) {
			column_statistics = get.function.statistics(context, get.bind_data.get(), get.column_ids[i]);
			if (column_statistics && have_catalog_table_statistics) {
				auto column_distinct_count = DistinctCount({column_statistics->GetDistinctCount(), true});
				return_stats.column_distinct_count.push_back(column_distinct_count);
				return_stats.column_names.push_back(name + "." + get.names.at(get.column_ids.at(i)));
				have_distinct_count_stats = true;
			}
		}
		if (!have_distinct_count_stats) {
			// currently treating the cardinality as the distinct count.
			// the cardinality estimator will update these distinct counts based
			// on the extra columns that are joined on.
			auto column_distinct_count = DistinctCount({cardinality_after_filters, false});
			return_stats.column_distinct_count.push_back(column_distinct_count);
			auto column_name = string("column");
			if (get.column_ids.at(i) < get.names.size()) {
				column_name = get.names.at(get.column_ids.at(i));
			}
			return_stats.column_names.push_back(get.GetName() + "." + column_name);
		}
	}

	if (!get.table_filters.filters.empty()) {
		column_statistics = nullptr;
		for (auto &it : get.table_filters.filters) {
			if (get.bind_data && get.function.name.compare("seq_scan") == 0) {
				auto &table_scan_bind_data = get.bind_data->Cast<TableScanBindData>();
				column_statistics = get.function.statistics(context, &table_scan_bind_data, it.first);
			}

			if (column_statistics && it.second->filter_type == TableFilterType::CONJUNCTION_AND) {
				auto &filter = it.second->Cast<ConjunctionAndFilter>();
				idx_t cardinality_with_and_filter = RelationStatisticsHelper::InspectConjunctionAND(
				    base_table_cardinality, it.first, filter, *column_statistics);
				cardinality_after_filters = MinValue(cardinality_after_filters, cardinality_with_and_filter);
			}
		}
		// if the above code didn't find an equality filter (i.e country_code = "[us]")
		// and there are other table filters (i.e cost > 50), use default selectivity.
		bool has_equality_filter = (cardinality_after_filters != base_table_cardinality);
		if (!has_equality_filter && !get.table_filters.filters.empty()) {
			cardinality_after_filters =
			    MaxValue<idx_t>(base_table_cardinality * RelationStatisticsHelper::DEFAULT_SELECTIVITY, 1);
		}
		if (base_table_cardinality == 0) {
			cardinality_after_filters = 0;
		}
	}
	return_stats.cardinality = cardinality_after_filters;
	// update the estimated cardinality of the get as well.
	// This is not updated during plan reconstruction.
	get.estimated_cardinality = cardinality_after_filters;
	get.has_estimated_cardinality = true;
	D_ASSERT(base_table_cardinality >= cardinality_after_filters);
	return_stats.stats_initialized = true;
	return return_stats;
}

RelationStats RelationStatisticsHelper::ExtractDelimGetStats(LogicalDelimGet &delim_get, ClientContext &context) {
	RelationStats stats;
	stats.table_name = delim_get.GetName();
	idx_t card = delim_get.EstimateCardinality(context);
	stats.cardinality = card;
	stats.stats_initialized = true;
	for (auto &binding : delim_get.GetColumnBindings()) {
		stats.column_distinct_count.push_back(DistinctCount({1, false}));
		stats.column_names.push_back("column" + to_string(binding.column_index));
	}
	return stats;
}

RelationStats RelationStatisticsHelper::ExtractProjectionStats(LogicalProjection &proj, RelationStats &child_stats) {
	auto proj_stats = RelationStats();
	proj_stats.cardinality = child_stats.cardinality;
	proj_stats.table_name = proj.GetName();
	for (auto &expr : proj.expressions) {
		proj_stats.column_names.push_back(expr->GetName());
		auto res = GetChildColumnBinding(*expr);
		D_ASSERT(res.found_expression);
		if (res.expression_is_constant) {
			proj_stats.column_distinct_count.push_back(DistinctCount({1, true}));
		} else {
			auto column_index = res.child_binding.column_index;
			if (column_index >= child_stats.column_distinct_count.size() && expr->ToString() == "count_star()") {
				// only one value for a count star
				proj_stats.column_distinct_count.push_back(DistinctCount({1, true}));
			} else {
				// TODO: add this back in
				//	D_ASSERT(column_index < stats.column_distinct_count.size());
				if (column_index < child_stats.column_distinct_count.size()) {
					proj_stats.column_distinct_count.push_back(child_stats.column_distinct_count.at(column_index));
				} else {
					proj_stats.column_distinct_count.push_back(DistinctCount({proj_stats.cardinality, false}));
				}
			}
		}
	}
	proj_stats.stats_initialized = true;
	return proj_stats;
}

RelationStats RelationStatisticsHelper::ExtractDummyScanStats(LogicalDummyScan &dummy_scan, ClientContext &context) {
	auto stats = RelationStats();
	idx_t card = dummy_scan.EstimateCardinality(context);
	stats.cardinality = card;
	for (idx_t i = 0; i < dummy_scan.GetColumnBindings().size(); i++) {
		stats.column_distinct_count.push_back(DistinctCount({card, false}));
		stats.column_names.push_back("dummy_scan_column");
	}
	stats.stats_initialized = true;
	stats.table_name = "dummy scan";
	return stats;
}

void RelationStatisticsHelper::CopyRelationStats(RelationStats &to, const RelationStats &from) {
	to.column_distinct_count = from.column_distinct_count;
	to.column_names = from.column_names;
	to.cardinality = from.cardinality;
	to.table_name = from.table_name;
	to.stats_initialized = from.stats_initialized;
}

RelationStats RelationStatisticsHelper::CombineStatsOfReorderableOperator(vector<ColumnBinding> &bindings,
                                                                          vector<RelationStats> relation_stats) {
	RelationStats stats;
	idx_t max_card = 0;
	for (auto &child_stats : relation_stats) {
		for (idx_t i = 0; i < child_stats.column_distinct_count.size(); i++) {
			stats.column_distinct_count.push_back(child_stats.column_distinct_count.at(i));
			stats.column_names.push_back(child_stats.column_names.at(i));
		}
		stats.table_name += "joined with " + child_stats.table_name;
		max_card = MaxValue(max_card, child_stats.cardinality);
	}
	stats.stats_initialized = true;
	stats.cardinality = max_card;
	return stats;
}

RelationStats RelationStatisticsHelper::CombineStatsOfNonReorderableOperator(LogicalOperator &op,
                                                                             vector<RelationStats> child_stats) {
	D_ASSERT(child_stats.size() == 2);
	RelationStats ret;
	idx_t child_1_card = child_stats[0].stats_initialized ? child_stats[0].cardinality : 0;
	idx_t child_2_card = child_stats[1].stats_initialized ? child_stats[1].cardinality : 0;
	ret.cardinality = MaxValue(child_1_card, child_2_card);
	ret.stats_initialized = true;
	ret.filter_strength = 1;
	ret.table_name = child_stats[0].table_name + " joined with " + child_stats[1].table_name;
	for (auto &stats : child_stats) {
		// MARK joins are nonreorderable. They won't return initialized stats
		// continue in this case.
		if (!stats.stats_initialized) {
			continue;
		}
		for (auto &distinct_count : stats.column_distinct_count) {
			ret.column_distinct_count.push_back(distinct_count);
		}
		for (auto &column_name : stats.column_names) {
			ret.column_names.push_back(column_name);
		}
	}
	return ret;
}

RelationStats RelationStatisticsHelper::ExtractExpressionGetStats(LogicalExpressionGet &expression_get,
                                                                  ClientContext &context) {
	auto stats = RelationStats();
	idx_t card = expression_get.EstimateCardinality(context);
	stats.cardinality = card;
	for (idx_t i = 0; i < expression_get.GetColumnBindings().size(); i++) {
		stats.column_distinct_count.push_back(DistinctCount({card, false}));
		stats.column_names.push_back("expression_get_column");
	}
	stats.stats_initialized = true;
	stats.table_name = "expression_get";
	return stats;
}

RelationStats RelationStatisticsHelper::ExtractWindowStats(LogicalWindow &window, RelationStats &child_stats) {
	RelationStats stats;
	stats.cardinality = child_stats.cardinality;
	stats.column_distinct_count = child_stats.column_distinct_count;
	stats.column_names = child_stats.column_names;
	stats.stats_initialized = true;
	auto num_child_columns = window.GetColumnBindings().size();

	for (idx_t column_index = child_stats.column_distinct_count.size(); column_index < num_child_columns;
	     column_index++) {
		stats.column_distinct_count.push_back(DistinctCount({child_stats.cardinality, false}));
		stats.column_names.push_back("window");
	}
	return stats;
}

RelationStats RelationStatisticsHelper::ExtractAggregationStats(LogicalAggregate &aggr, RelationStats &child_stats) {
	RelationStats stats;
	// TODO: look at child distinct count to better estimate cardinality.
	stats.cardinality = child_stats.cardinality;
	stats.column_distinct_count = child_stats.column_distinct_count;
	stats.column_names = child_stats.column_names;
	stats.stats_initialized = true;
	auto num_child_columns = aggr.GetColumnBindings().size();

	for (idx_t column_index = child_stats.column_distinct_count.size(); column_index < num_child_columns;
	     column_index++) {
		stats.column_distinct_count.push_back(DistinctCount({child_stats.cardinality, false}));
		stats.column_names.push_back("aggregate");
	}
	return stats;
}

idx_t RelationStatisticsHelper::InspectConjunctionAND(idx_t cardinality, idx_t column_index,
                                                      ConjunctionAndFilter &filter, BaseStatistics &base_stats) {
	auto cardinality_after_filters = cardinality;
	for (auto &child_filter : filter.child_filters) {
		if (child_filter->filter_type != TableFilterType::CONSTANT_COMPARISON) {
			continue;
		}
		auto &comparison_filter = child_filter->Cast<ConstantFilter>();
		if (comparison_filter.comparison_type != ExpressionType::COMPARE_EQUAL) {
			continue;
		}
		auto column_count = base_stats.GetDistinctCount();
		auto filtered_card = cardinality;
		// column_count = 0 when there is no column count (i.e parquet scans)
		if (column_count > 0) {
			// we want the ceil of cardinality/column_count. We also want to avoid compiler errors
			filtered_card = (cardinality + column_count - 1) / column_count;
			cardinality_after_filters = filtered_card;
		}
	}
	return cardinality_after_filters;
}

// TODO: Currently only simple AND filters are pushed into table scans.
//  When OR filters are pushed this function can be added
// idx_t RelationStatisticsHelper::InspectConjunctionOR(idx_t cardinality, idx_t column_index, ConjunctionOrFilter
// &filter,
//                                                     BaseStatistics &base_stats) {
//	auto has_equality_filter = false;
//	auto cardinality_after_filters = cardinality;
//	for (auto &child_filter : filter.child_filters) {
//		if (child_filter->filter_type != TableFilterType::CONSTANT_COMPARISON) {
//			continue;
//		}
//		auto &comparison_filter = child_filter->Cast<ConstantFilter>();
//		if (comparison_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
//			auto column_count = base_stats.GetDistinctCount();
//			auto increment = MaxValue<idx_t>(((cardinality + column_count - 1) / column_count), 1);
//			if (has_equality_filter) {
//				cardinality_after_filters += increment;
//			} else {
//				cardinality_after_filters = increment;
//			}
//			has_equality_filter = true;
//		}
//		if (child_filter->filter_type == TableFilterType::CONJUNCTION_AND) {
//			auto &and_filter = child_filter->Cast<ConjunctionAndFilter>();
//			cardinality_after_filters = RelationStatisticsHelper::InspectConjunctionAND(
//			    cardinality_after_filters, column_index, and_filter, base_stats);
//			continue;
//		}
//	}
//	D_ASSERT(cardinality_after_filters > 0);
//	return cardinality_after_filters;
//}

} // namespace duckdb
