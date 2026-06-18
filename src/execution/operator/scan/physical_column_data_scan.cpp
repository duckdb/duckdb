#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"

#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/logger.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/set/physical_cte.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

static constexpr idx_t COLUMN_DATA_SCAN_BATCH_SIZE = STANDARD_VECTOR_SIZE * 50ULL;

static idx_t ColumnDataScanBatchCount(idx_t count, idx_t batch_size) {
	return MaxValue<idx_t>(count / batch_size + (count % batch_size != 0), 1);
}

PhysicalColumnDataScan::PhysicalColumnDataScan(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                               PhysicalOperatorType op_type, idx_t estimated_cardinality,
                                               optionally_owned_ptr<ColumnDataCollection> collection_p)
    : PhysicalOperator(physical_plan, op_type, std::move(types), estimated_cardinality),
      collection(std::move(collection_p)) {
}

PhysicalColumnDataScan::PhysicalColumnDataScan(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                               PhysicalOperatorType op_type, idx_t estimated_cardinality,
                                               TableIndex cte_index)
    : PhysicalOperator(physical_plan, op_type, std::move(types), estimated_cardinality), collection(nullptr),
      cte_index(cte_index) {
}

class PhysicalColumnDataGlobalScanState : public GlobalSourceState {
public:
	PhysicalColumnDataGlobalScanState(ClientContext &context, const ColumnDataCollection &collection)
	    : batch_size(ClientConfig::GetConfig(context).verify_parallelism ? STANDARD_VECTOR_SIZE
	                                                                     : COLUMN_DATA_SCAN_BATCH_SIZE),
	      max_threads(ColumnDataScanBatchCount(collection.Count(), batch_size)) {
		collection.InitializeScan(global_scan_state);
	}

	idx_t MaxThreads() override {
		return max_threads;
	}

public:
	ColumnDataParallelScanState global_scan_state;

	const idx_t batch_size;
	const idx_t max_threads;
	idx_t next_batch_index = 0;
};

struct ColumnDataScanEntry {
	ColumnDataScanEntry(idx_t chunk_index_p, idx_t segment_index_p, idx_t row_index_p)
	    : chunk_index(chunk_index_p), segment_index(segment_index_p), row_index(row_index_p) {
	}

	idx_t chunk_index;
	idx_t segment_index;
	idx_t row_index;
};

class PhysicalColumnDataLocalScanState : public LocalSourceState {
public:
	bool AssignTask(const ColumnDataCollection &collection, PhysicalColumnDataGlobalScanState &gstate) {
		entries.clear();
		entry_index = 0;
		batch_index = DConstants::INVALID_INDEX;

		idx_t task_count = 0;
		lock_guard<mutex> l(gstate.global_scan_state.lock);
		while (task_count < gstate.batch_size) {
			idx_t chunk_index;
			idx_t segment_index;
			idx_t row_index;
			if (!collection.NextScanIndex(gstate.global_scan_state.scan_state, chunk_index, segment_index, row_index)) {
				break;
			}
			entries.emplace_back(chunk_index, segment_index, row_index);
			task_count += collection.GetSegments()[segment_index]->chunk_data[chunk_index].count;
		}

		if (entries.empty()) {
			return false;
		}
		batch_index = gstate.next_batch_index++;
		return true;
	}

	ColumnDataLocalScanState local_scan_state;
	vector<ColumnDataScanEntry> entries;
	idx_t entry_index = 0;
	idx_t batch_index = DConstants::INVALID_INDEX;
};

unique_ptr<GlobalSourceState> PhysicalColumnDataScan::GetGlobalSourceState(ClientContext &context) const {
	if (!collection) {
		return make_uniq<GlobalSourceState>();
	}
	return make_uniq<PhysicalColumnDataGlobalScanState>(context, *collection);
}

unique_ptr<LocalSourceState> PhysicalColumnDataScan::GetLocalSourceState(ExecutionContext &,
                                                                         GlobalSourceState &) const {
	return make_uniq<PhysicalColumnDataLocalScanState>();
}

SourceResultType PhysicalColumnDataScan::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                         OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<PhysicalColumnDataGlobalScanState>();
	auto &lstate = input.local_state.Cast<PhysicalColumnDataLocalScanState>();
	if (lstate.entry_index >= lstate.entries.size() && !lstate.AssignTask(*collection, gstate)) {
		chunk.Reset();
		return SourceResultType::FINISHED;
	}

	auto &entry = lstate.entries[lstate.entry_index++];
	collection->ScanAtIndex(gstate.global_scan_state, lstate.local_scan_state, chunk, entry.chunk_index,
	                        entry.segment_index, entry.row_index);
	return SourceResultType::HAVE_MORE_OUTPUT;
}

bool PhysicalColumnDataScan::SupportsPartitioning(const OperatorPartitionInfo &partition_info) const {
	if (partition_info.RequiresPartitionColumns()) {
		return false;
	}
	if (!partition_info.RequiresBatchIndex()) {
		return false;
	}
	return type == PhysicalOperatorType::COLUMN_DATA_SCAN || type == PhysicalOperatorType::CTE_SCAN;
}

OperatorPartitionData PhysicalColumnDataScan::GetPartitionData(ExecutionContext &context, DataChunk &chunk,
                                                               GlobalSourceState &gstate, LocalSourceState &lstate_p,
                                                               const OperatorPartitionInfo &partition_info) const {
	D_ASSERT(SupportsPartitioning(partition_info));
	auto &lstate = lstate_p.Cast<PhysicalColumnDataLocalScanState>();
	return OperatorPartitionData(lstate.batch_index);
}

ProgressData PhysicalColumnDataScan::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	if (!collection) {
		ProgressData progress;
		progress.SetInvalid();
		return progress;
	}
	auto &state = gstate.Cast<PhysicalColumnDataGlobalScanState>();
	lock_guard<mutex> guard(state.global_scan_state.lock);
	auto total = MaxValue<idx_t>(collection->Count(), 1);
	ProgressData progress;
	progress.done = double(MinValue<idx_t>(state.global_scan_state.scan_state.next_row_index, total));
	progress.total = double(total);
	return progress;
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
		state.SetPipelineSource(current, delim_join.distinct.Cast<PhysicalOperator>());
		return;
	}
	case PhysicalOperatorType::CTE_SCAN: {
		if (cte_source) {
			auto entry = state.cte_dependencies.find(*this);
			D_ASSERT(entry != state.cte_dependencies.end());
			auto cte_dependency = entry->second.get().shared_from_this();
			auto cte_sink = state.GetPipelineSink(*cte_dependency);
			D_ASSERT(cte_sink);
			D_ASSERT(cte_sink->type == PhysicalOperatorType::CTE);
			auto &cte = cte_sink->Cast<PhysicalCTE>();
			auto &source = cte_source->Cast<PhysicalCTEConsumerSource>();
			// Prefer direct fanout. Buffered exchange is only used when it can avoid full materialization or
			// when the consumer can stop early; otherwise this scan reads the materialized working table.
			if (cte.TryRegisterDirectConsumer(current, source.consumer_idx)) {
				auto current_pipeline = current.shared_from_this();
				current.SetExternalInput();
				current.AddExternalFinishDependency(cte_dependency);
				cte_dependency->AddDataflowDependency(current_pipeline);
				DUCKDB_LOG(current.GetClientContext(), PhysicalOperatorLogType, cte, "PhysicalCTE", "SelectConsumer",
				           {{"consumer", to_string(source.consumer_idx)}, {"mode", "DIRECT"}});
				state.SetPipelineSource(current, *cte_source);
				return;
			}
			if (cte.ShouldUseBufferedConsumer(current)) {
				cte.RegisterBufferedConsumer(source.consumer_idx);
				current.AddDataflowDependency(cte_dependency);
				DUCKDB_LOG(current.GetClientContext(), PhysicalOperatorLogType, cte, "PhysicalCTE", "SelectConsumer",
				           {{"consumer", to_string(source.consumer_idx)}, {"mode", "BUFFERED"}});
				state.SetPipelineSource(current, *cte_source);
				return;
			}
			D_ASSERT(collection);
			cte.RegisterMaterializedConsumer(source.consumer_idx);
			current.AddDependency(cte_dependency);
			DUCKDB_LOG(current.GetClientContext(), PhysicalOperatorLogType, cte, "PhysicalCTE", "SelectConsumer",
			           {{"consumer", to_string(source.consumer_idx)}, {"mode", "MATERIALIZED"}});
			state.SetPipelineSource(current, *this);
			return;
		}
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
	case PhysicalOperatorType::RECURSIVE_RECURRING_CTE_SCAN:
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
	case PhysicalOperatorType::RECURSIVE_RECURRING_CTE_SCAN:
	case PhysicalOperatorType::CTE_SCAN:
	case PhysicalOperatorType::RECURSIVE_CTE_SCAN: {
		result["CTE Index"] = StringUtil::Format("%llu", cte_index.index);
		break;
	}
	default:
		break;
	}
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
