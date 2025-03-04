#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/operator/scan/physical_expression_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalExpressionGet &op) {
	D_ASSERT(op.children.size() == 1);
	auto &plan = CreatePlan(*op.children[0]);

	auto &expr_scan_ref = Make<PhysicalExpressionScan>(op.types, std::move(op.expressions), op.estimated_cardinality);
	expr_scan_ref.children.push_back(plan);

	auto &cast_expr_scan_ref = expr_scan_ref.Cast<PhysicalExpressionScan>();
	if (!cast_expr_scan_ref.IsFoldable()) {
		return expr_scan_ref;
	}

	auto &allocator = Allocator::Get(context);
	// simple expression scan (i.e. no subqueries to evaluate and no prepared statement parameters)
	// we can evaluate all the expressions right now and turn this into a chunk collection scan
	auto &chunk_scan_ref = Make<PhysicalColumnDataScan>(op.types, PhysicalOperatorType::COLUMN_DATA_SCAN,
	                                                    cast_expr_scan_ref.expressions.size(),
	                                                    make_uniq<ColumnDataCollection>(context, op.types));
	auto &cast_chunk_scan_ref = chunk_scan_ref.Cast<PhysicalColumnDataScan>();

	DataChunk chunk;
	chunk.Initialize(allocator, op.types);

	ColumnDataAppendState append_state;
	cast_chunk_scan_ref.collection->InitializeAppend(append_state);
	for (idx_t expression_idx = 0; expression_idx < cast_expr_scan_ref.expressions.size(); expression_idx++) {
		chunk.Reset();
		cast_expr_scan_ref.EvaluateExpression(context, expression_idx, nullptr, chunk);
		cast_chunk_scan_ref.collection->Append(append_state, chunk);
	}
	return chunk_scan_ref;
}

} // namespace duckdb
