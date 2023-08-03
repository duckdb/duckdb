#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"

namespace duckdb
{
unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalRecursiveCTE &op)
{
	D_ASSERT(op.children.size() == 2);
	// Create the working_table that the PhysicalRecursiveCTE will use for evaluation.
	auto working_table = std::make_shared<ColumnDataCollection>(context, op.types);
	// Add the ColumnDataCollection to the context of this PhysicalPlanGenerator
	recursive_cte_tables[op.table_index] = working_table;
	LogicalOperator* left_pop = ((LogicalOperator*)op.children[0].get());
	auto left = CreatePlan(*left_pop);
	LogicalOperator* right_pop = ((LogicalOperator*)op.children[1].get());
	auto right = CreatePlan(*right_pop);
	auto cte = make_uniq<PhysicalRecursiveCTE>(op.types, op.union_all, std::move(left), std::move(right), op.estimated_cardinality);
	cte->working_table = working_table;
	return std::move(cte);
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCTERef &op) {
	D_ASSERT(op.children.empty());

	auto chunk_scan =
	    make_uniq<PhysicalColumnDataScan>(op.types, PhysicalOperatorType::RECURSIVE_CTE_SCAN, op.estimated_cardinality);

	// CreatePlan of a LogicalRecursiveCTE must have happened before.
	auto cte = recursive_cte_tables.find(op.cte_index);
	if (cte == recursive_cte_tables.end()) {
		throw InvalidInputException("Referenced recursive CTE does not exist.");
	}
	chunk_scan->collection = cte->second.get();
	return std::move(chunk_scan);
}

} // namespace duckdb
