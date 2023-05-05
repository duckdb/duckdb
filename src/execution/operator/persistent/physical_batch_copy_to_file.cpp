#include "duckdb/execution/operator/persistent/physical_batch_copy_to_file.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/allocator.hpp"
#include <algorithm>

namespace duckdb {

PhysicalBatchCopyToFile::PhysicalBatchCopyToFile(vector<LogicalType> types, CopyFunction function_p,
                                                 unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::BATCH_COPY_TO_FILE, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data)) {
	if (!function.flush_batch || !function.prepare_batch) {
		throw InternalException(
		    "PhysicalBatchCopyToFile created for copy function that does not have prepare_batch/flush_batch defined");
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BatchCopyToGlobalState : public GlobalSinkState {
public:
	explicit BatchCopyToGlobalState(unique_ptr<GlobalFunctionData> global_state)
	    : rows_copied(0), global_state(std::move(global_state)) {
	}

	mutex lock;
	mutex flush_lock;
	atomic<idx_t> rows_copied;
	unique_ptr<GlobalFunctionData> global_state;
	map<idx_t, unique_ptr<PreparedBatchData>> batch_data;
};

class BatchCopyToLocalState : public LocalSinkState {
public:
	explicit BatchCopyToLocalState(unique_ptr<LocalFunctionData> local_state_p)
	    : local_state(std::move(local_state_p)), rows_copied(0), batch_index(0) {
	}

	unique_ptr<LocalFunctionData> local_state;
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
	idx_t rows_copied;
	idx_t batch_index;

	void InitializeCollection(ClientContext &context, const PhysicalOperator &op) {
		collection = make_uniq<ColumnDataCollection>(Allocator::Get(context), op.children[0]->types);
		collection->InitializeAppend(append_state);
	}
};

SinkResultType PhysicalBatchCopyToFile::Sink(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSinkInput &input) const {
	auto &state = input.local_state.Cast<BatchCopyToLocalState>();
	if (!state.collection) {
		state.InitializeCollection(context.client, *this);
	}
	state.rows_copied += chunk.size();
	state.collection->Append(state.append_state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalBatchCopyToFile::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                      LocalSinkState &lstate) const {
	auto &state = lstate.Cast<BatchCopyToLocalState>();
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();
	gstate.rows_copied += state.rows_copied;
}

SinkFinalizeType PhysicalBatchCopyToFile::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                   GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();
	FlushBatchData(context, gstate_p, NumericLimits<int64_t>::Maximum());
	if (function.copy_to_finalize) {
		function.copy_to_finalize(context, *bind_data, *gstate.global_state);

		if (use_tmp_file) {
			PhysicalCopyToFile::MoveTmpFile(context, file_path);
		}
	}
	return SinkFinalizeType::READY;
}

void PhysicalBatchCopyToFile::PrepareBatchData(ClientContext &context, GlobalSinkState &gstate_p, idx_t batch_index,
                                               unique_ptr<ColumnDataCollection> collection) const {
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();

	// prepare the batch
	auto batch_data = function.prepare_batch(context, *bind_data, *gstate.global_state, std::move(collection));
	// move the batch data to the set of prepared batch data
	lock_guard<mutex> l(gstate.lock);
	gstate.batch_data[batch_index] = std::move(batch_data);
}

void PhysicalBatchCopyToFile::FlushBatchData(ClientContext &context, GlobalSinkState &gstate_p, idx_t min_index) const {
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();

	// flush batch data to disk (if there are any to flush)
	while (true) {
		// grab the flush lock - we can only call flush_batch with this lock
		// otherwise the data might end up in the wrong order
		lock_guard<mutex> l(gstate.flush_lock);
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

void PhysicalBatchCopyToFile::NextBatch(ExecutionContext &context, GlobalSinkState &gstate_p,
                                        LocalSinkState &lstate) const {
	auto &state = lstate.Cast<BatchCopyToLocalState>();
	if (state.collection) {
		// we finished processing this batch
		// start flushing data
		PrepareBatchData(context.client, gstate_p, state.batch_index, std::move(state.collection));
		FlushBatchData(context.client, gstate_p, lstate.partition_info.min_batch_index.GetIndex());
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
