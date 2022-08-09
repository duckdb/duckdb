#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"

namespace duckdb {

class PhysicalColumnDataScanState : public GlobalSourceState {
public:
	explicit PhysicalColumnDataScanState() : initialized(false) {
	}

	//! The current position in the scan
	ColumnDataScanState scan_state;
	bool initialized;
};

unique_ptr<GlobalSourceState> PhysicalColumnDataScan::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<PhysicalColumnDataScanState>();
}

void PhysicalColumnDataScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                     LocalSourceState &lstate) const {
	auto &state = (PhysicalColumnDataScanState &)gstate;
	D_ASSERT(collection);
	if (collection->Count() == 0) {
		return;
	}
	if (!state.initialized) {
		collection->InitializeScan(state.scan_state);
		state.initialized = true;
	}
	collection->Scan(state.scan_state, chunk);
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalColumnDataScan::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
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
