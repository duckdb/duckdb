#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalExplain &op) {
	assert(op.children.size() == 1);
	auto logical_plan_opt = op.children[0]->ToString();
	auto plan = CreatePlan(*op.children[0]);

	op.physical_plan = plan->ToString();

	// the output of the explain
	vector<string> keys = {"logical_plan", "logical_opt", "physical_plan"};
	vector<string> values = {op.logical_plan_unopt, logical_plan_opt, op.physical_plan};
	// create a ChunkCollection from the output
	auto collection = make_unique<ChunkCollection>();

	DataChunk chunk;
	chunk.Initialize(op.types);
	for (idx_t i = 0; i < keys.size(); i++) {
		chunk.SetValue(0, chunk.size(), Value(keys[i]));
		chunk.SetValue(1, chunk.size(), Value(values[i]));
		chunk.SetCardinality(chunk.size() + 1);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			collection->Append(chunk);
			chunk.Reset();
		}
	}
	collection->Append(chunk);

	// create a chunk scan to output the result
	auto chunk_scan = make_unique<PhysicalChunkScan>(op.types, PhysicalOperatorType::CHUNK_SCAN);
	chunk_scan->owned_collection = move(collection);
	chunk_scan->collection = chunk_scan->owned_collection.get();
	return move(chunk_scan);
}
