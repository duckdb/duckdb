#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalGet &op) {
	assert(op.children.empty());
	unordered_map<idx_t, vector<TableFilter>> table_filter_umap;
	if (!op.table) {
		return make_unique<PhysicalDummyScan>(op.types);
	} else {
		for (auto &tableFilter : op.tableFilters) {
			for (idx_t i = 0; i < op.column_ids.size(); i++) {
				if (tableFilter.column_index == op.column_ids[i]) {
					tableFilter.column_index = i;
					auto filter = table_filter_umap.find(i);
					if (filter != table_filter_umap.end()) {
						filter->second.push_back(tableFilter);
					} else {
						table_filter_umap.insert(make_pair(i, vector<TableFilter>{tableFilter}));
					}
					break;
				}
			}
		}
		dependencies.insert(op.table);
		return make_unique<PhysicalTableScan>(op, *op.table, *op.table->storage, op.column_ids, move(op.expressions),
		                                      move(table_filter_umap));
	}
}
