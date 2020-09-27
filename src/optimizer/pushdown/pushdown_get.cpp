#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {
using namespace std;

unique_ptr<LogicalOperator> FilterPushdown::PushdownGet(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::GET);
	auto &get = (LogicalGet &)*op;
	// first push down arbitrary filters
	if (get.function.pushdown_complex_filter) {
		// for the remaining filters, check if we can push any of them into the scan as well
		vector<unique_ptr<Expression>> expressions;
		for (idx_t i = 0; i < filters.size(); i++) {
			expressions.push_back(move(filters[i]->filter));
		}
		filters.clear();

		get.function.pushdown_complex_filter(optimizer.context, get, get.bind_data.get(), expressions);

		if (expressions.size() == 0) {
			return op;
		}
		// re-generate the filters
		for (auto &expr : expressions) {
			auto f = make_unique<Filter>();
			f->filter = move(expr);
			f->ExtractBindings();
			filters.push_back(move(f));
		}
	}
	if (!get.tableFilters.empty() || !get.function.filter_pushdown) {
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
