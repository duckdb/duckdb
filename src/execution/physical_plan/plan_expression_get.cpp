#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/operator/scan/physical_expression_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalExpressionGet &op) {
	D_ASSERT(op.children.size() == 1);
	auto plan = CreatePlan(*op.children[0]);

	auto expr_scan = make_uniq<PhysicalExpressionScan>(op.types, std::move(op.expressions), op.estimated_cardinality);
	expr_scan->children.push_back(std::move(plan));
	if (!expr_scan->IsFoldable()) {
		return std::move(expr_scan);
	}
	auto &allocator = Allocator::Get(context);
	// simple expression scan (i.e. no subqueries to evaluate and no prepared statement parameters)
	// we can evaluate all the expressions right now and turn this into a chunk collection scan
	auto chunk_scan = make_uniq<PhysicalColumnDataScan>(op.types, PhysicalOperatorType::COLUMN_DATA_SCAN,
	                                                    expr_scan->expressions.size(),
	                                                    make_uniq<ColumnDataCollection>(context, op.types));

	DataChunk chunk;
	chunk.Initialize(allocator, op.types);

	ColumnDataAppendState append_state;
	chunk_scan->collection->InitializeAppend(append_state);
	for (idx_t expression_idx = 0; expression_idx < expr_scan->expressions.size(); expression_idx++) {
		chunk.Reset();
		expr_scan->EvaluateExpression(context, expression_idx, nullptr, chunk);
		chunk_scan->collection->Append(append_state, chunk);
	}
	return std::move(chunk_scan);
}

} // namespace duckdb
