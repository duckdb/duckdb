#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parser/parsed_data/show_select_info.hpp"
#include "duckdb/planner/operator/logical_show.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalShow &op) {
	DataChunk output;
	output.Initialize(op.types);

	auto collection = make_unique<ChunkCollection>();
	for (idx_t column_idx = 0; column_idx < op.types_select.size(); column_idx++) {
		auto type = op.types_select[column_idx];
		auto &name = op.aliases[column_idx];

		// "name", TypeId::VARCHAR
		output.SetValue(0, output.size(), Value(name));
		// "type", TypeId::VARCHAR
		output.SetValue(1, output.size(), Value(type.ToString()));
		// "null", TypeId::VARCHAR
		output.SetValue(2, output.size(), Value("YES"));
		// "pk", TypeId::BOOL
		output.SetValue(3, output.size(), Value());
		// "dflt_value", TypeId::VARCHAR
		output.SetValue(4, output.size(), Value());
		// "extra", TypeId::VARCHAR
		output.SetValue(5, output.size(), Value());

		output.SetCardinality(output.size() + 1);
		if (output.size() == STANDARD_VECTOR_SIZE) {
			collection->Append(output);
			output.Reset();
		}
	}

	collection->Append(output);

	// create a chunk scan to output the result
	auto chunk_scan =
	    make_unique<PhysicalChunkScan>(op.types, PhysicalOperatorType::CHUNK_SCAN, op.estimated_cardinality);
	chunk_scan->owned_collection = move(collection);
	chunk_scan->collection = chunk_scan->owned_collection.get();
	return chunk_scan;
}

} // namespace duckdb
