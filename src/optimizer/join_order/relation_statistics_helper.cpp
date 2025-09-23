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

#include <math.h>

namespace duckdb {

static ExpressionBinding GetChildColumnBinding(Expression &expr) {
	auto ret = ExpressionBinding();
	switch (expr.GetExpressionClass()) {
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

idx_t RelationStatisticsHelper::GetDistinctCount(LogicalGet &get, ClientContext &context, idx_t column_id) {
	if (!get.function.statistics) {
		return 0;
	}
	auto column_statistics = get.function.statistics(context, get.bind_data.get(), column_id);
	if (!column_statistics) {
		return 0;
	}
	auto distinct_count = column_statistics->GetDistinctCount();
	return distinct_count;
}

RelationStats RelationStatisticsHelper::ExtractGetStats(LogicalGet &get, ClientContext &context) {
	auto return_stats = RelationStats();

	auto base_table_cardinality = get.EstimateCardinality(context);
	auto cardinality_after_filters = base_table_cardinality;
	unique_ptr<BaseStatistics> column_statistics;

	auto catalog_table = get.GetTable();
	auto name = string("some table");
	if (catalog_table) {
		name = catalog_table->name;
		return_stats.table_name = name;
	}

	// first push back basic distinct counts for each column (if we have them).
	auto &column_ids = get.GetColumnIds();
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column_id = column_ids[i].GetPrimaryIndex();
		auto distinct_count = GetDistinctCount(get, context, column_id);
		if (distinct_count > 0) {
			auto column_distinct_count = DistinctCount({distinct_count, true});
			return_stats.column_distinct_count.push_back(column_distinct_count);
			return_stats.column_names.push_back(name + "." + get.names.at(column_id));
		} else {
			// treat the cardinality as the distinct count.
			// the cardinality estimator will update these distinct counts based
			// on the extra columns that are joined on.
			auto column_distinct_count = DistinctCount({cardinality_after_filters, false});
			return_stats.column_distinct_count.push_back(column_distinct_count);
			auto column_name = string("column");
			if (column_id < get.names.size()) {
				column_name = get.names.at(column_id);
			}
			return_stats.column_names.push_back(get.GetName() + "." + column_name);
		}
	}

	if (!get.table_filters.filters.empty()) {
		column_statistics = nullptr;
		bool has_non_optional_filters = false;
		for (auto &it : get.table_filters.filters) {
			if (get.bind_data && get.function.statistics) {
				column_statistics = get.function.statistics(context, get.bind_data.get(), it.first);
			}

			if (column_statistics) {
				idx_t cardinality_with_filter =
				    InspectTableFilter(base_table_cardinality, it.first, *it.second, *column_statistics);
				cardinality_after_filters = MinValue(cardinality_after_filters, cardinality_with_filter);
			}

			if (it.second->filter_type != TableFilterType::OPTIONAL_FILTER) {
				has_non_optional_filters = true;
			}
		}
		// if the above code didn't find an equality filter (i.e country_code = "[us]")
		// and there are other table filters (i.e cost > 50), use default selectivity.
		bool has_equality_filter = (cardinality_after_filters != base_table_cardinality);
		if (!has_equality_filter && has_non_optional_filters) {
			cardinality_after_filters = MaxValue<idx_t>(
			    LossyNumericCast<idx_t>(double(base_table_cardinality) * RelationStatisticsHelper::DEFAULT_SELECTIVITY),
			    1U);
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
                                                                             const vector<RelationStats> &child_stats) {
	RelationStats ret;
	ret.cardinality = 0;

	// default predicted cardinality is the max of all child cardinalities
	vector<idx_t> child_cardinalities;
	for (auto &stats : child_stats) {
		idx_t child_cardinality = stats.stats_initialized ? stats.cardinality : 0;
		ret.cardinality = MaxValue(ret.cardinality, child_cardinality);
		child_cardinalities.push_back(child_cardinality);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		D_ASSERT(child_stats.size() == 2);
		auto &join = op.Cast<LogicalComparisonJoin>();
		switch (join.join_type) {
		case JoinType::RIGHT_ANTI:
		case JoinType::RIGHT_SEMI:
			ret.cardinality = child_cardinalities[1];
			break;
		case JoinType::ANTI:
		case JoinType::SEMI:
		case JoinType::SINGLE:
		case JoinType::MARK:
			ret.cardinality = child_cardinalities[0];
			break;
		default:
			break;
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_UNION: {
		auto &setop = op.Cast<LogicalSetOperation>();
		if (setop.setop_all) {
			// setop returns all records
			ret.cardinality = 0;
			for (auto &child_cardinality : child_cardinalities) {
				ret.cardinality += child_cardinality;
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		D_ASSERT(child_stats.size() == 2);
		ret.cardinality = MinValue(child_cardinalities[0], child_cardinalities[1]);
		break;
	}
	case LogicalOperatorType::LOGICAL_EXCEPT: {
		D_ASSERT(child_stats.size() == 2);
		ret.cardinality = child_cardinalities[0];
		break;
	}
	default:
		break;
	}

	ret.stats_initialized = true;
	ret.filter_strength = 1;
	ret.table_name = string();
	for (auto &stats : child_stats) {
		if (!ret.table_name.empty()) {
			ret.table_name += " joined with ";
		}
		ret.table_name += stats.table_name;
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
	vector<double> distinct_counts;
	for (auto &g_set : aggr.grouping_sets) {
		vector<double> set_distinct_counts;
		for (auto &ind : g_set) {
			if (aggr.groups[ind]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				continue;
			}
			auto bound_col = &aggr.groups[ind]->Cast<BoundColumnRefExpression>();
			auto col_index = bound_col->binding.column_index;
			if (col_index >= child_stats.column_distinct_count.size()) {
				// it is possible the column index of the grouping_set is not in the child stats.
				// this can happen when delim joins are present, since delim scans are not currently
				// reorderable. Meaning they don't add a relation or column_ids that could potentially
				// be grouped by. Hopefully this can be fixed with duckdb-internal#606
				continue;
			}
			double distinct_count = static_cast<double>(child_stats.column_distinct_count[col_index].distinct_count);
			set_distinct_counts.push_back(distinct_count == 0 ? 1 : distinct_count);
		}
		// We use the grouping set with the most group key columns for cardinality estimation
		if (set_distinct_counts.size() > distinct_counts.size()) {
			distinct_counts = std::move(set_distinct_counts);
		}
	}

	double new_card;
	if (distinct_counts.empty()) {
		// We have no good statistics on distinct count.
		// most likely we are running on parquet files. Therefore we divide by 2.
		new_card = static_cast<double>(child_stats.cardinality) / 2.0;
	} else {
		// Multiply distinct counts
		double product = 1;
		for (const auto &distinct_count : distinct_counts) {
			product *= distinct_count;
		}

		// Assume slight correlation for each grouping column
		const auto correction = pow(0.95, static_cast<double>(distinct_counts.size() - 1));
		product *= correction;

		// Estimate using the "Occupancy Problem",
		// where "product" is number of bins, and "child_stats.cardinality" is number of balls
		const auto mult = 1.0 - exp(-static_cast<double>(child_stats.cardinality) / product);
		if (mult == 0) { // Can become 0 with very large estimates due to double imprecision
			new_card = static_cast<double>(child_stats.cardinality);
		} else {
			new_card = product * mult;
		}
		new_card = MinValue(new_card, static_cast<double>(child_stats.cardinality));
	}

	// an ungrouped aggregate has 1 row
	stats.cardinality = aggr.groups.empty() ? 1 : LossyNumericCast<idx_t>(new_card);
	stats.column_names = child_stats.column_names;
	stats.stats_initialized = true;
	const auto aggr_column_bindings = aggr.GetColumnBindings();
	auto num_child_columns = aggr_column_bindings.size();

	for (idx_t column_index = 0; column_index < num_child_columns; column_index++) {
		const auto &binding = aggr_column_bindings[column_index];
		if (binding.table_index == aggr.group_index && column_index < distinct_counts.size()) {
			// Group column that we have the HLL of
			stats.column_distinct_count.push_back(
			    DistinctCount({LossyNumericCast<idx_t>(distinct_counts[column_index]), true}));
		} else {
			// Non-group column, or we don't have the HLL
			stats.column_distinct_count.push_back(DistinctCount({child_stats.cardinality, false}));
		}
		stats.column_names.push_back("aggregate");
	}
	return stats;
}

RelationStats RelationStatisticsHelper::ExtractEmptyResultStats(LogicalEmptyResult &empty) {
	RelationStats stats;
	for (idx_t i = 0; i < empty.GetColumnBindings().size(); i++) {
		stats.column_distinct_count.push_back(DistinctCount({0, false}));
		stats.column_names.push_back("empty_result_column");
	}
	stats.stats_initialized = true;
	return stats;
}

idx_t RelationStatisticsHelper::InspectTableFilter(idx_t cardinality, idx_t column_index, TableFilter &filter,
                                                   BaseStatistics &base_stats) {
	auto cardinality_after_filters = cardinality;
	switch (filter.filter_type) {
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		for (auto &child_filter : and_filter.child_filters) {
			cardinality_after_filters = MinValue(
			    cardinality_after_filters, InspectTableFilter(cardinality, column_index, *child_filter, base_stats));
		}
		return cardinality_after_filters;
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &comparison_filter = filter.Cast<ConstantFilter>();
		if (comparison_filter.comparison_type != ExpressionType::COMPARE_EQUAL) {
			return cardinality_after_filters;
		}
		auto column_count = base_stats.GetDistinctCount();
		// column_count = 0 when there is no column count (i.e parquet scans)
		if (column_count > 0) {
			// we want the ceil of cardinality/column_count. We also want to avoid compiler errors
			cardinality_after_filters = (cardinality + column_count - 1) / column_count;
		}
		return cardinality_after_filters;
	}
	default:
		return cardinality_after_filters;
	}
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
