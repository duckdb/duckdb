#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {
using namespace std;

unique_ptr<LogicalOperator> FilterPushdown::PushdownGet(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::GET);
	auto &get = (LogicalGet &)*op;
	if (!get.tableFilters.empty()) {
		if (!filters.empty()) {
			//! We didn't managed to push down all filters to table scan
			auto logicalFilter = make_unique<LogicalFilter>();
			for (auto &f : filters) {
				logicalFilter->expressions.push_back(move(f->filter));
			}
			logicalFilter->children.push_back(move(op));
			return move(logicalFilter);
		} else {
			return op;
		}
	}
	if (!get.function.filter_pushdown) {
		// the table function does not support filter pushdown: push a LogicalFilter on top
		return FinishPushdown(move(op));
	}
	PushFilters();

	get.tableFilters = combiner.GenerateTableScanFilters(get.column_ids);
	for (auto &f : get.tableFilters) {
		f.column_index = get.column_ids[f.column_index];
	}

	GenerateFilters();
	return FinishPushdown(move(op));
}

} // namespace duckdb
