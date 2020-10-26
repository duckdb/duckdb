#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/function/table/table_scan.hpp"

namespace duckdb {
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalGet &op) {
	assert(op.children.empty());

	// create the table filter map
	unordered_map<idx_t, vector<TableFilter>> table_filter_umap;
	for (auto &tableFilter : op.tableFilters) {
		// find the relative column index from the absolute column index into the table
		idx_t column_index = INVALID_INDEX;
		for (idx_t i = 0; i < op.column_ids.size(); i++) {
			if (tableFilter.column_index == op.column_ids[i]) {
				column_index = i;
				break;
			}
		}
		if (column_index == INVALID_INDEX) {
			throw InternalException("Could not find column index for table filter");
		}
		tableFilter.column_index = column_index;
		auto filter = table_filter_umap.find(column_index);
		if (filter != table_filter_umap.end()) {
			filter->second.push_back(tableFilter);
		} else {
			table_filter_umap.insert(make_pair(column_index, vector<TableFilter>{tableFilter}));
		}
	}

	if (op.function.dependency) {
		op.function.dependency(dependencies, op.bind_data.get());
	}
	// create the table scan node
	if (!op.function.projection_pushdown) {
		// function does not support projection pushdown
		auto node = make_unique<PhysicalTableScan>(op.returned_types, op.function, move(op.bind_data), op.column_ids,
		                                           op.names, move(table_filter_umap));
		// first check if an additional projection is necessary
		if (op.column_ids.size() == op.returned_types.size()) {
			bool projection_necessary = false;
			for (idx_t i = 0; i < op.column_ids.size(); i++) {
				if (op.column_ids[i] != i) {
					projection_necessary = true;
					break;
				}
			}
			if (!projection_necessary) {
				// a projection is not necessary if all columns have been requested in-order
				// in that case we just return the node
				return move(node);
			}
		}
		// push a projection on top that does the projection
		vector<LogicalType> types;
		vector<unique_ptr<Expression>> expressions;
		for (auto &column_id : op.column_ids) {
			if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
				types.push_back(LogicalType::BIGINT);
				expressions.push_back(make_unique<BoundConstantExpression>(Value::BIGINT(0)));
			} else {
				auto type = op.returned_types[column_id];
				types.push_back(type);
				expressions.push_back(make_unique<BoundReferenceExpression>(type, column_id));
			}
		}
		auto projection = make_unique<PhysicalProjection>(move(types), move(expressions));
		projection->children.push_back(move(node));
		return move(projection);
	} else {
		return make_unique<PhysicalTableScan>(op.types, op.function, move(op.bind_data), op.column_ids, op.names,
		                                      move(table_filter_umap));
	}
}

} // namespace duckdb
