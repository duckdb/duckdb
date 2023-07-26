#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/relation_manager.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/join_order/statistics_extractor.hpp"

namespace duckdb {

struct DistinctCount;

RelationStats StatisticsExtractor::ExtractOperatorStats(LogicalGet &get, ClientContext &context) {
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
	// first push back basic distinct counts for each column (if we have them).
	for (idx_t i = 0; i < get.column_ids.size(); i++ ) {
		if (get.function.statistics) {

			column_statistics = get.function.statistics(context, get.bind_data.get(), get.column_ids[i]);
			auto column_distinct_count = DistinctCount({column_statistics->GetDistinctCount(), true});
			return_stats.column_distinct_count.push_back(column_distinct_count);
			return_stats.column_names.push_back(name + "." + get.names.at(get.column_ids.at(i)));
		} else {
			// TODO: currently treating the cardinality as the distinct count.
			//  the cardinality estimator will update these distinct counts based
			//  on the extra columns that are joined on.
			// TODO: Should this be changed?
			auto column_distinct_count = DistinctCount({cardinality_after_filters, false});
			return_stats.column_distinct_count.push_back(column_distinct_count);
			// TODO: we can still get parquet column names right?
			return_stats.column_names.push_back(get.GetName() + ".some_column");
		}
	}

	if (!get.table_filters.filters.empty()) {
		column_statistics = nullptr;
		for (auto &it : get.table_filters.filters) {
			if (get.bind_data && get.function.name.compare("seq_scan") == 0) {
				auto &table_scan_bind_data = get.bind_data->Cast<TableScanBindData>();
				column_statistics = get.function.statistics(context, &table_scan_bind_data, it.first);
			}

			if (it.second->filter_type == TableFilterType::CONJUNCTION_AND) {
				auto &filter = it.second->Cast<ConjunctionAndFilter>();
				idx_t cardinality_with_and_filter =
				    StatisticsExtractor::InspectConjunctionAND(base_table_cardinality, it.first, filter, std::move(column_statistics));
				cardinality_after_filters = MinValue(cardinality_after_filters, cardinality_with_and_filter);
				// And filter has been found that is equality filter, so update the distinct count;
				// filter pull up and push down will determine that all table scans have this distinct count
				// before joining happens.

//				if (cardinality_after_filters < base_table_cardinality) {
//					for (idx_t i = 0; i < get.column_ids.size(); i++ ) {
//						auto column_id = get.column_ids.at(i);
//						if (column_id == it.first) {
//							return_stats.column_distinct_count[i].distinct_count = 1;
//							break;
//						}
//					}
//				}
			} else if (it.second->filter_type == TableFilterType::CONJUNCTION_OR) {
				auto &filter = it.second->Cast<ConjunctionOrFilter>();
				idx_t cardinality_with_or_filter =
				    StatisticsExtractor::InspectConjunctionOR(base_table_cardinality, it.first, filter, std::move(column_statistics));
				cardinality_after_filters = MinValue(cardinality_after_filters, cardinality_with_or_filter);
				// OR filter has been found that is equality filter, so update the distinct count;
				// filter pull up and push down will determine that all table scans have this distinct count
				// before joining happens.
				if (cardinality_after_filters < base_table_cardinality) {
					for (idx_t i = 0; i < get.column_ids.size(); i++ ) {
						auto column_id = get.column_ids.at(i);
						if (column_id == it.first) {
							return_stats.column_distinct_count[i].distinct_count = filter.child_filters.size();
							break;
						}
					}
				}
			}
		}
		// if the above code didn't find an equality filter (i.e country_code = "[us]")
		// and there are other table filters (i.e cost > 50), use default selectivity.
		bool has_equality_filter = (cardinality_after_filters != base_table_cardinality);
		if (!has_equality_filter && !get.table_filters.filters.empty()) {
			cardinality_after_filters = MaxValue<idx_t>(base_table_cardinality * StatisticsExtractor::DEFAULT_SELECTIVITY, 1);
		}
	}
	return_stats.cardinality = cardinality_after_filters;
	// update the estimated cardinality of the get as well.
	// This is not updated during plan reconstruction.
	get.estimated_cardinality = cardinality_after_filters;
	get.has_estimated_cardinality = true;
	D_ASSERT(base_table_cardinality >= cardinality_after_filters);
	return_stats.filter_strength = 1;//((double)base_table_cardinality / cardinality_after_filters);
	return_stats.stats_initialized = true;
	return return_stats;

	// Are their table filters?
	// if a table filter is an equality filter, do we know the distinct count?
	// if we know the distinct count, we assume independence and say the table cardinality is cardinality / distinct value count.


	// based on table cardinality we calculate distinct stats
	// distinct stats on columns with equality filters is 1
	// distinct stats on (AND) filters, (correlation maybe?)
	// distinct status on columns with OR filters (and equality) is the number of or filters

	// distinct stats on other columns ??? (leave untouched for now?)
}

idx_t StatisticsExtractor::InspectConjunctionAND(idx_t cardinality, idx_t column_index, ConjunctionAndFilter &filter,
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

idx_t StatisticsExtractor::InspectConjunctionOR(idx_t cardinality, idx_t column_index, ConjunctionOrFilter &filter,
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

}

