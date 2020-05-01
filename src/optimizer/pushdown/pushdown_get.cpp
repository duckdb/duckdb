#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
using namespace duckdb;
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
	//! FIXME: We only need to skip if the index is in the column being filtered
	if (!get.table || !get.table->storage->info->indexes.empty()) {
		//! now push any existing filters
		if (filters.empty()) {
			//! no filters to push
			return op;
		}
		auto filter = make_unique<LogicalFilter>();
		for (auto &f : filters) {
			filter->expressions.push_back(move(f->filter));
		}
		filter->children.push_back(move(op));
		return move(filter);
	}
	PushFilters();

	vector<unique_ptr<Filter>> filtersToPushDown;
	get.tableFilters = combiner.GenerateTableScanFilters(
	    [&](unique_ptr<Expression> filter) {
		    auto f = make_unique<Filter>();
		    f->filter = move(filter);
		    f->ExtractBindings();
		    filtersToPushDown.push_back(move(f));
	    },
	    get.column_ids);
	for (auto &f : get.tableFilters) {
		f.column_index = get.column_ids[f.column_index];
	}

	GenerateFilters();
	for (auto &f : filtersToPushDown) {
		get.expressions.push_back(move(f->filter));
	}

	if (!filters.empty()) {
		//! We didn't managed to push down all filters to table scan
		auto logicalFilter = make_unique<LogicalFilter>();
		for (auto &f : filters) {
			logicalFilter->expressions.push_back(move(f->filter));
		}
		logicalFilter->children.push_back(move(op));
		return move(logicalFilter);
	}
	return op;
}
