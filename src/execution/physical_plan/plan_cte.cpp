#include "duckdb/execution/operator/set/physical_cte.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCTE &op) {
	D_ASSERT(op.children.size() == 2);

	// Create the working_table that the PhysicalCTE will use for evaluation.
	auto working_table = std::make_shared<ColumnDataCollection>(context, op.children[0]->types);

	// Add the ColumnDataCollection to the context of this PhysicalPlanGenerator
	recursive_cte_tables[op.table_index] = working_table;

	// Create the plan for the left side. This is the materialization.
	auto left = CreatePlan(*op.children[0]);
	// Register this CTE as materialized.
	materialized_cte_ids.push_back(op.table_index);
	// Initialize an empty vector to collect the scan operators.
	materialized_ctes[op.table_index] = vector<PhysicalOperator *>();
	auto right = CreatePlan(*op.children[1]);

	auto cte = make_uniq<PhysicalCTE>(op.ctename, op.table_index, op.children[1]->types, std::move(left),
	                                  std::move(right), op.estimated_cardinality);
	cte->working_table = working_table;
	cte->cte_scans = materialized_ctes[op.table_index];

	return std::move(cte);
}

} // namespace duckdb
