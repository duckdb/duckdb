#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"

#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalColumnDataScan::PhysicalColumnDataScan(vector<LogicalType> types, PhysicalOperatorType op_type,
                                               idx_t estimated_cardinality,
                                               unique_ptr<ColumnDataCollection> owned_collection_p)
    : PhysicalOperator(op_type, std::move(types), estimated_cardinality), collection(owned_collection_p.get()),
      owned_collection(std::move(owned_collection_p)) {
}

class PhysicalColumnDataScanState : public GlobalSourceState {
public:
	explicit PhysicalColumnDataScanState() : initialized(false) {
	}

	//! The current position in the scan
	ColumnDataScanState scan_state;
	bool initialized;
};

unique_ptr<GlobalSourceState> PhysicalColumnDataScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PhysicalColumnDataScanState>();
}

void PhysicalColumnDataScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                     LocalSourceState &lstate) const {
	auto &state = gstate.Cast<PhysicalColumnDataScanState>();
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
void PhysicalColumnDataScan::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	// check if there is any additional action we need to do depending on the type
	auto &state = meta_pipeline.GetState();
	switch (physical_type) {
	case PhysicalOperatorType::DELIM_SCAN: {
		auto entry = state.delim_join_dependencies.find(*this);
		D_ASSERT(entry != state.delim_join_dependencies.end());
		// this chunk scan introduces a dependency to the current pipeline
		// namely a dependency on the duplicate elimination pipeline to finish
		auto delim_dependency = entry->second.get().shared_from_this();
		auto delim_sink = state.GetPipelineSink(*delim_dependency);
		D_ASSERT(delim_sink);
		D_ASSERT(delim_sink->physical_type == PhysicalOperatorType::DELIM_JOIN);
		auto &delim_join = delim_sink->Cast<PhysicalDelimJoin>();
		current.AddDependency(delim_dependency);
		state.SetPipelineSource(current, (PhysicalOperator &)*delim_join.distinct);
		return;
	}
	case PhysicalOperatorType::RECURSIVE_CTE_SCAN:
		if (!meta_pipeline.HasRecursiveCTE()) {
			throw InternalException("Recursive CTE scan found without recursive CTE node");
		}
		break;
	default:
		break;
	}
	D_ASSERT(children.empty());
	state.SetPipelineSource(current, *this);
}

} // namespace duckdb
