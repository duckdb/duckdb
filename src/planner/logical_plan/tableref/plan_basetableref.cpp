#include "duckdb/planner/logical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundBaseTableRef &ref) {
	auto table = ref.table;
	auto index = ref.bind_index;

	vector<column_t> column_ids;
	// look in the context for this table which columns are required
	for (auto &bound_column : ref.bound_columns) {
		column_ids.push_back(table->name_map[bound_column]);
	}
	if (require_row_id || column_ids.size() == 0) {
		// no column ids selected
		// the query is like SELECT COUNT(*) FROM table, or SELECT 42 FROM table
		// return just the row id
		column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}
	return make_unique<LogicalGet>(table, index, column_ids);
}
