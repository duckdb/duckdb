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
	chunk.SetCardinality(keys.size());
	for (index_t i = 0; i < keys.size(); i++) {
		chunk.SetValue(0, i, Value(keys[i]));
		chunk.SetValue(1, i, Value(values[i]));
	}
	collection->Append(chunk);

	// create a chunk scan to output the result
	auto chunk_scan = make_unique<PhysicalChunkScan>(op.types, PhysicalOperatorType::CHUNK_SCAN);
	chunk_scan->owned_collection = move(collection);
	chunk_scan->collection = chunk_scan->owned_collection.get();
	return move(chunk_scan);
}
