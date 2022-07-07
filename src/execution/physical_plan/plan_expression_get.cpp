#include "duckdb/execution/operator/scan/physical_expression_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalExpressionGet &op) {
	D_ASSERT(op.children.size() == 1);
	auto plan = CreatePlan(*op.children[0]);

	auto expr_scan = make_unique<PhysicalExpressionScan>(op.types, move(op.expressions), op.estimated_cardinality);
	expr_scan->children.push_back(move(plan));
	if (!expr_scan->IsFoldable()) {
		return move(expr_scan);
	}
	auto &allocator = Allocator::Get(context);
	// simple expression scan (i.e. no subqueries to evaluate and no prepared statement parameters)
	// we can evaluate all the expressions right now and turn this into a chunk collection scan
	auto chunk_scan =
	    make_unique<PhysicalChunkScan>(op.types, PhysicalOperatorType::CHUNK_SCAN, expr_scan->expressions.size());
	chunk_scan->owned_collection = make_unique<ChunkCollection>(allocator);
	chunk_scan->collection = chunk_scan->owned_collection.get();

	DataChunk chunk;
	chunk.Initialize(allocator, op.types);
	for (idx_t expression_idx = 0; expression_idx < expr_scan->expressions.size(); expression_idx++) {
		chunk.Reset();
		expr_scan->EvaluateExpression(allocator, expression_idx, nullptr, chunk);
		chunk_scan->owned_collection->Append(chunk);
	}
	return move(chunk_scan);
}

} // namespace duckdb
