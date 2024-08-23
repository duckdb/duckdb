#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalColumnDataScan::PhysicalColumnDataScan(vector<LogicalType> types, PhysicalOperatorType op_type,
                                               idx_t estimated_cardinality,
                                               optionally_owned_ptr<ColumnDataCollection> collection_p)
    : PhysicalOperator(op_type, std::move(types), estimated_cardinality), collection(std::move(collection_p)),
      cte_index(DConstants::INVALID_INDEX) {
}

PhysicalColumnDataScan::PhysicalColumnDataScan(vector<LogicalType> types, PhysicalOperatorType op_type,
                                               idx_t estimated_cardinality, idx_t cte_index)
    : PhysicalOperator(op_type, std::move(types), estimated_cardinality), collection(nullptr), cte_index(cte_index) {
}

class PhysicalColumnDataGlobalScanState : public GlobalSourceState {
public:
	explicit PhysicalColumnDataGlobalScanState(const ColumnDataCollection &collection)
	    : max_threads(MaxValue<idx_t>(collection.ChunkCount(), 1)) {
		collection.InitializeScan(global_scan_state);
	}

	idx_t MaxThreads() override {
		return max_threads;
	}

public:
	ColumnDataParallelScanState global_scan_state;

	const idx_t max_threads;
};

class PhysicalColumnDataLocalScanState : public LocalSourceState {
public:
	ColumnDataLocalScanState local_scan_state;
};

unique_ptr<GlobalSourceState> PhysicalColumnDataScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PhysicalColumnDataGlobalScanState>(*collection);
}

unique_ptr<LocalSourceState> PhysicalColumnDataScan::GetLocalSourceState(ExecutionContext &,
                                                                         GlobalSourceState &) const {
	return make_uniq<PhysicalColumnDataLocalScanState>();
}

SourceResultType PhysicalColumnDataScan::GetData(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<PhysicalColumnDataGlobalScanState>();
	auto &lstate = input.local_state.Cast<PhysicalColumnDataLocalScanState>();
	collection->Scan(gstate.global_scan_state, lstate.local_scan_state, chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalColumnDataScan::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	// check if there is any additional action we need to do depending on the type
	auto &state = meta_pipeline.GetState();
	switch (type) {
	case PhysicalOperatorType::DELIM_SCAN: {
		auto entry = state.delim_join_dependencies.find(*this);
		D_ASSERT(entry != state.delim_join_dependencies.end());
		// this chunk scan introduces a dependency to the current pipeline
		// namely a dependency on the duplicate elimination pipeline to finish
		auto delim_dependency = entry->second.get().shared_from_this();
		auto delim_sink = state.GetPipelineSink(*delim_dependency);
		D_ASSERT(delim_sink);
		D_ASSERT(delim_sink->type == PhysicalOperatorType::LEFT_DELIM_JOIN ||
		         delim_sink->type == PhysicalOperatorType::RIGHT_DELIM_JOIN);
		auto &delim_join = delim_sink->Cast<PhysicalDelimJoin>();
		current.AddDependency(delim_dependency);
		state.SetPipelineSource(current, delim_join.distinct->Cast<PhysicalOperator>());
		return;
	}
	case PhysicalOperatorType::CTE_SCAN: {
		auto entry = state.cte_dependencies.find(*this);
		D_ASSERT(entry != state.cte_dependencies.end());
		// this chunk scan introduces a dependency to the current pipeline
		// namely a dependency on the CTE pipeline to finish
		auto cte_dependency = entry->second.get().shared_from_this();
		auto cte_sink = state.GetPipelineSink(*cte_dependency);
		(void)cte_sink;
		D_ASSERT(cte_sink);
		D_ASSERT(cte_sink->type == PhysicalOperatorType::CTE);
		current.AddDependency(cte_dependency);
		state.SetPipelineSource(current, *this);
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

InsertionOrderPreservingMap<string> PhysicalColumnDataScan::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	switch (type) {
	case PhysicalOperatorType::DELIM_SCAN:
		if (delim_index.IsValid()) {
			result["Delim Index"] = StringUtil::Format("%llu", delim_index.GetIndex());
		}
		break;
	case PhysicalOperatorType::CTE_SCAN:
	case PhysicalOperatorType::RECURSIVE_CTE_SCAN: {
		result["CTE Index"] = StringUtil::Format("%llu", cte_index);
		break;
	}
	default:
		break;
	}
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
