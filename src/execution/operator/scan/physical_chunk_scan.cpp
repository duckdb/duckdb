#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"

namespace duckdb {

class PhysicalChunkScanState : public GlobalSourceState {
public:
	explicit PhysicalChunkScanState() : chunk_index(0) {
	}

	//! The current position in the scan
	idx_t chunk_index;
};

unique_ptr<GlobalSourceState> PhysicalChunkScan::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<PhysicalChunkScanState>();
}

void PhysicalChunkScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                LocalSourceState &lstate) const {
	auto &state = (PhysicalChunkScanState &)gstate;
	D_ASSERT(collection);
	if (collection->Count() == 0) {
		return;
	}
	D_ASSERT(chunk.GetTypes() == collection->Types());
	if (state.chunk_index >= collection->ChunkCount()) {
		return;
	}
	auto &collection_chunk = collection->GetChunk(state.chunk_index);
	chunk.Reference(collection_chunk);
	state.chunk_index++;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalChunkScan::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	// check if there is any additional action we need to do depending on the type
	switch (type) {
	case PhysicalOperatorType::DELIM_SCAN: {
		auto entry = state.delim_join_dependencies.find(this);
		D_ASSERT(entry != state.delim_join_dependencies.end());
		// this chunk scan introduces a dependency to the current pipeline
		// namely a dependency on the duplicate elimination pipeline to finish
		auto delim_dependency = entry->second->shared_from_this();
		auto delim_sink = state.GetPipelineSink(*delim_dependency);
		D_ASSERT(delim_sink);
		D_ASSERT(delim_sink->type == PhysicalOperatorType::DELIM_JOIN);
		auto &delim_join = (PhysicalDelimJoin &)*delim_sink;
		current.AddDependency(delim_dependency);
		state.SetPipelineSource(current, (PhysicalOperator *)delim_join.distinct.get());
		return;
	}
	case PhysicalOperatorType::RECURSIVE_CTE_SCAN:
		if (!state.recursive_cte) {
			throw InternalException("Recursive CTE scan found without recursive CTE node");
		}
		break;
	default:
		break;
	}
	D_ASSERT(children.empty());
	state.SetPipelineSource(current, this);
}

} // namespace duckdb
