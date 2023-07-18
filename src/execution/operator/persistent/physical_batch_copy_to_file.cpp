#include "duckdb/execution/operator/persistent/physical_batch_copy_to_file.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"

#include <algorithm>

namespace duckdb {

PhysicalBatchCopyToFile::PhysicalBatchCopyToFile(vector<LogicalType> types, CopyFunction function_p,
                                                 unique_ptr<FunctionData> bind_data_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::BATCH_COPY_TO_FILE, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data_p)) {
	if (!function.flush_batch || !function.prepare_batch) {
		throw InternalException(
		    "PhysicalBatchCopyToFile created for copy function that does not have prepare_batch/flush_batch defined");
	}
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class BatchCopyToGlobalState : public GlobalSinkState {
public:
	explicit BatchCopyToGlobalState(unique_ptr<GlobalFunctionData> global_state)
	    : rows_copied(0), global_state(std::move(global_state)), any_flushing(false) {
	}

	mutex lock;
	//! The total number of rows copied to the file
	atomic<idx_t> rows_copied;
	//! Global copy state
	unique_ptr<GlobalFunctionData> global_state;
	//! The prepared batch data by batch index - ready to flush
	map<idx_t, unique_ptr<PreparedBatchData>> batch_data;
	//! Lock for flushing to disk
	mutex flush_lock;
	//! Whether or not any threads are flushing (only one thread can flush at a time)
	atomic<bool> any_flushing;

	void AddBatchData(idx_t batch_index, unique_ptr<PreparedBatchData> new_batch) {
		// move the batch data to the set of prepared batch data
		lock_guard<mutex> l(lock);
		auto entry = batch_data.insert(make_pair(batch_index, std::move(new_batch)));
		if (!entry.second) {
			throw InternalException("Duplicate batch index %llu encountered in PhysicalBatchCopyToFile", batch_index);
		}
	}
};

class BatchCopyToLocalState : public LocalSinkState {
public:
	explicit BatchCopyToLocalState(unique_ptr<LocalFunctionData> local_state_p)
	    : local_state(std::move(local_state_p)), rows_copied(0) {
	}

	//! Local copy state
	unique_ptr<LocalFunctionData> local_state;
	//! The current collection we are appending to
	unique_ptr<ColumnDataCollection> collection;
	//! The append state of the collection
	ColumnDataAppendState append_state;
	//! How many rows have been copied in total
	idx_t rows_copied;
	//! The current batch index
	optional_idx batch_index;

	void InitializeCollection(ClientContext &context, const PhysicalOperator &op) {
		collection = make_uniq<ColumnDataCollection>(BufferAllocator::Get(context), op.children[0]->types);
		collection->InitializeAppend(append_state);
	}
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalBatchCopyToFile::Sink(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSinkInput &input) const {
	auto &state = input.local_state.Cast<BatchCopyToLocalState>();
	if (!state.collection) {
		state.InitializeCollection(context.client, *this);
		state.batch_index = state.partition_info.batch_index.GetIndex();
	}
	state.rows_copied += chunk.size();
	state.collection->Append(state.append_state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalBatchCopyToFile::Combine(ExecutionContext &context,
                                                       OperatorSinkCombineInput &input) const {
	auto &state = input.local_state.Cast<BatchCopyToLocalState>();
	auto &gstate = input.global_state.Cast<BatchCopyToGlobalState>();
	gstate.rows_copied += state.rows_copied;

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalBatchCopyToFile::FinalFlush(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();
	idx_t min_batch_index = idx_t(NumericLimits<int64_t>::Maximum());
	FlushBatchData(context, gstate_p, min_batch_index);
	if (function.copy_to_finalize) {
		function.copy_to_finalize(context, *bind_data, *gstate.global_state);

		if (use_tmp_file) {
			PhysicalCopyToFile::MoveTmpFile(context, file_path);
		}
	}
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalBatchCopyToFile::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                   OperatorSinkFinalizeInput &input) const {
	FinalFlush(context, input.global_state);
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Batch Data Handling
//===--------------------------------------------------------------------===//
void PhysicalBatchCopyToFile::PrepareBatchData(ClientContext &context, GlobalSinkState &gstate_p, idx_t batch_index,
                                               unique_ptr<ColumnDataCollection> collection) const {
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();

	// prepare the batch
	auto batch_data = function.prepare_batch(context, *bind_data, *gstate.global_state, std::move(collection));
	gstate.AddBatchData(batch_index, std::move(batch_data));
}

void PhysicalBatchCopyToFile::FlushBatchData(ClientContext &context, GlobalSinkState &gstate_p, idx_t min_index) const {
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();

	// flush batch data to disk (if there are any to flush)
	// grab the flush lock - we can only call flush_batch with this lock
	// otherwise the data might end up in the wrong order
	{
		lock_guard<mutex> l(gstate.flush_lock);
		if (gstate.any_flushing) {
			return;
		}
		gstate.any_flushing = true;
	}
	ActiveFlushGuard active_flush(gstate.any_flushing);
	while (true) {
		unique_ptr<PreparedBatchData> batch_data;
		{
			// fetch the next batch to flush (if any)
			lock_guard<mutex> l(gstate.lock);
			if (gstate.batch_data.empty()) {
				// no batch data left to flush
				break;
			}
			auto entry = gstate.batch_data.begin();
			if (entry->first >= min_index) {
				// this data is past the min_index - we cannot write it yet
				break;
			}
			if (!entry->second) {
				// this batch is in process of being prepared but is not ready yet
				break;
			}
			batch_data = std::move(entry->second);
			gstate.batch_data.erase(entry);
		}
		function.flush_batch(context, *bind_data, *gstate.global_state, *batch_data);
	}
}

//===--------------------------------------------------------------------===//
// Next Batch
//===--------------------------------------------------------------------===//
void PhysicalBatchCopyToFile::NextBatch(ExecutionContext &context, GlobalSinkState &gstate_p,
                                        LocalSinkState &lstate) const {
	auto &state = lstate.Cast<BatchCopyToLocalState>();
	if (state.collection && state.collection->Count() > 0) {
		// we finished processing this batch
		// start flushing data
		auto min_batch_index = lstate.partition_info.min_batch_index.GetIndex();
		PrepareBatchData(context.client, gstate_p, state.batch_index.GetIndex(), std::move(state.collection));
		FlushBatchData(context.client, gstate_p, min_batch_index);
	}
	state.batch_index = lstate.partition_info.batch_index.GetIndex();

	state.InitializeCollection(context.client, *this);
}

unique_ptr<LocalSinkState> PhysicalBatchCopyToFile::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<BatchCopyToLocalState>(function.copy_to_initialize_local(context, *bind_data));
}

unique_ptr<GlobalSinkState> PhysicalBatchCopyToFile::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<BatchCopyToGlobalState>(function.copy_to_initialize_global(context, *bind_data, file_path));
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalBatchCopyToFile::GetData(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	auto &g = sink_state->Cast<BatchCopyToGlobalState>();

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(g.rows_copied));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
