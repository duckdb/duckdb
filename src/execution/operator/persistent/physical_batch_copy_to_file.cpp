#include "duckdb/execution/operator/persistent/physical_batch_copy_to_file.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/queue.hpp"
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
// Sink
//===--------------------------------------------------------------------===//
class BatchCopyTask {
public:
	virtual ~BatchCopyTask() {
	}

	virtual void Execute(const PhysicalBatchCopyToFile &op, ClientContext &context, GlobalSinkState &gstate_p) = 0;
};

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class BatchCopyToGlobalState : public GlobalSinkState {
public:
	explicit BatchCopyToGlobalState(unique_ptr<GlobalFunctionData> global_state)
	    : rows_copied(0), global_state(std::move(global_state)), batch_size(0), scheduled_batch_index(0),
	      flushed_batch_index(0) {
	}

	mutex lock;
	mutex flush_lock;
	//! The total number of rows copied to the file
	atomic<idx_t> rows_copied;
	//! Global copy state
	unique_ptr<GlobalFunctionData> global_state;
	//! The desired batch size (if any)
	idx_t batch_size;
	//! Unpartitioned batches - only used in case batch_size is required
	map<idx_t, unique_ptr<ColumnDataCollection>> raw_batches;
	//! The prepared batch data by batch index - ready to flush
	map<idx_t, unique_ptr<PreparedBatchData>> batch_data;
	//! The index of the latest batch index that has been scheduled
	atomic<idx_t> scheduled_batch_index;
	//! The index of the latest batch index that has been flushed
	atomic<idx_t> flushed_batch_index;

	//! Whether or not we need to repartition the batches
	bool RequiresRepartition() {
		return batch_size != 0;
	}

	void AddTask(unique_ptr<BatchCopyTask> task) {
		lock_guard<mutex> l(task_lock);
		task_queue.push(std::move(task));
	}

	unique_ptr<BatchCopyTask> GetTask() {
		lock_guard<mutex> l(task_lock);
		if (task_queue.empty()) {
			return nullptr;
		}
		auto entry = std::move(task_queue.front());
		task_queue.pop();
		return entry;
	}

	void AddBatchData(idx_t batch_index, unique_ptr<PreparedBatchData> new_batch) {
		// move the batch data to the set of prepared batch data
		lock_guard<mutex> l(lock);
		auto entry = batch_data.insert(make_pair(batch_index, std::move(new_batch)));
		if (!entry.second) {
			throw InternalException("Duplicate batch index %llu encountered in PhysicalBatchCopyToFile", batch_index);
		}
	}

private:
	mutex task_lock;
	//! The task queue for the batch copy to file
	queue<unique_ptr<BatchCopyTask>> task_queue;
};

class BatchCopyToLocalState : public LocalSinkState {
public:
	explicit BatchCopyToLocalState(unique_ptr<LocalFunctionData> local_state_p)
	    : local_state(std::move(local_state_p)), rows_copied(0), batch_index(0) {
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
	idx_t batch_index;

	void InitializeCollection(ClientContext &context, const PhysicalOperator &op) {
		collection = make_uniq<ColumnDataCollection>(Allocator::Get(context), op.children[0]->types);
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
	idx_t min_batch_index = idx_t(NumericLimits<int64_t>::Maximum());
	if (gstate.RequiresRepartition()) {
		// there are raw batches remaining - repartition
		RepartitionBatches(context, gstate_p, min_batch_index, true);
		ExecuteTasks(context, gstate_p);
	}
	FlushBatchData(context, gstate_p, min_batch_index);
	if (gstate.scheduled_batch_index != gstate.flushed_batch_index) {
		throw InternalException("Not all batches were flushed to disk - incomplete file?");
	}
	if (function.copy_to_finalize) {
		function.copy_to_finalize(context, *bind_data, *gstate.global_state);

		if (use_tmp_file) {
			PhysicalCopyToFile::MoveTmpFile(context, file_path);
		}
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Tasks
//===--------------------------------------------------------------------===//
class BatchCopyFlushTask : public BatchCopyTask {
public:
	explicit BatchCopyFlushTask(idx_t min_index) : min_index(min_index) {
	}

	idx_t min_index;

	void Execute(const PhysicalBatchCopyToFile &op, ClientContext &context, GlobalSinkState &gstate_p) override {
		op.FlushBatchData(context, gstate_p, min_index);
	}
};

class PrepareBatchTask : public BatchCopyTask {
public:
	PrepareBatchTask(idx_t batch_index, unique_ptr<ColumnDataCollection> collection_p)
	    : batch_index(batch_index), collection(std::move(collection_p)) {
	}

	idx_t batch_index;
	unique_ptr<ColumnDataCollection> collection;

	void Execute(const PhysicalBatchCopyToFile &op, ClientContext &context, GlobalSinkState &gstate_p) override {
		auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();
		auto batch_data =
		    op.function.prepare_batch(context, *op.bind_data, *gstate.global_state, std::move(collection));
		gstate.AddBatchData(batch_index, std::move(batch_data));
	}
};

//===--------------------------------------------------------------------===//
// Batch Data Handling
//===--------------------------------------------------------------------===//
void PhysicalBatchCopyToFile::AddRawBatchData(ClientContext &context, GlobalSinkState &gstate_p, idx_t batch_index,
                                              unique_ptr<ColumnDataCollection> collection) const {
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();

	// add the batch index to the set of raw batches
	lock_guard<mutex> l(gstate.lock);
	auto entry = gstate.raw_batches.insert(make_pair(batch_index, std::move(collection)));
	if (!entry.second) {
		throw InternalException("Duplicate batch index %llu encountered in PhysicalBatchCopyToFile", batch_index);
	}
}

static bool CorrectSizeForBatch(idx_t collection_size, idx_t desired_size) {
	return idx_t(AbsValue<int64_t>(int64_t(collection_size) - int64_t(desired_size))) < STANDARD_VECTOR_SIZE;
}

void PhysicalBatchCopyToFile::RepartitionBatches(ClientContext &context, GlobalSinkState &gstate_p, idx_t min_index,
                                                 bool final) const {
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();

	// repartition batches until the min index is reached
	lock_guard<mutex> l(gstate.lock);
	if (gstate.raw_batches.empty()) {
		return;
	}
	if (!final) {
		// if this is not the final flush we first check if we have enough data to merge past the batch threshold
		idx_t candidate_rows = 0;
		for (auto entry = gstate.raw_batches.begin(); entry != gstate.raw_batches.end(); entry++) {
			if (entry->first >= min_index) {
				// we have exceeded the minimum batch
				break;
			}
			candidate_rows += entry->second->Count();
		}
		if (candidate_rows < gstate.batch_size) {
			// not enough rows - cancel!
			return;
		}
	}
	// gather all collections we can repartition
	idx_t max_batch_index;
	vector<unique_ptr<ColumnDataCollection>> collections;
	for (auto entry = gstate.raw_batches.begin(); entry != gstate.raw_batches.end();) {
		if (entry->first >= min_index) {
			break;
		}
		max_batch_index = entry->first;
		collections.push_back(std::move(entry->second));
		entry = gstate.raw_batches.erase(entry);
	}
	unique_ptr<ColumnDataCollection> current_collection;
	ColumnDataAppendState append_state;
	// now perform the actual repartitioning
	for (auto &collection : collections) {
		if (!current_collection) {
			if (CorrectSizeForBatch(collection->Count(), gstate.batch_size)) {
				// the collection is ~approximately equal to the batch size (off by at most one vector)
				// use it directly
				gstate.AddTask(make_uniq<PrepareBatchTask>(gstate.scheduled_batch_index++, std::move(collection)));
				collection.reset();
			} else if (collection->Count() < gstate.batch_size) {
				// the collection is smaller than the batch size - use it as a starting point
				current_collection = std::move(collection);
				collection.reset();
			} else {
				// the collection is too large for a batch - we need to repartition
				// create an empty collection
				current_collection = make_uniq<ColumnDataCollection>(Allocator::Get(context), children[0]->types);
			}
			if (current_collection) {
				current_collection->InitializeAppend(append_state);
			}
		}
		if (!collection) {
			// we have consumed the collection already - no need to append
			continue;
		}
		// iterate the collection while appending
		for (auto &chunk : collection->Chunks()) {
			// append the chunk to the collection
			current_collection->Append(append_state, chunk);
			if (current_collection->Count() < gstate.batch_size) {
				// the collection is still under the batch size - continue
				continue;
			}
			// the collection is full - move it to the result and create a new one
			gstate.AddTask(make_uniq<PrepareBatchTask>(gstate.scheduled_batch_index++, std::move(current_collection)));
			current_collection = make_uniq<ColumnDataCollection>(Allocator::Get(context), children[0]->types);
			current_collection->InitializeAppend(append_state);
		}
	}
	if (current_collection->Count() > 0) {
		// if there are any remaining batches that are not filled up to the batch size
		// AND this is not the final collection
		// re-add it to the set of raw (to-be-merged) batches
		if (final || CorrectSizeForBatch(current_collection->Count(), gstate.batch_size)) {
			gstate.AddTask(make_uniq<PrepareBatchTask>(gstate.scheduled_batch_index++, std::move(current_collection)));
		} else {
			gstate.raw_batches[max_batch_index] = std::move(current_collection);
		}
	}
}

void PhysicalBatchCopyToFile::PrepareBatchData(ClientContext &context, GlobalSinkState &gstate_p, idx_t batch_index,
                                               unique_ptr<ColumnDataCollection> collection) const {
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();

	// prepare the batch
	auto batch_data = function.prepare_batch(context, *bind_data, *gstate.global_state, std::move(collection));
	gstate.AddBatchData(batch_index, std::move(batch_data));
}

void PhysicalBatchCopyToFile::FlushBatchDataRepartitioned(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();
	while (true) {
		unique_ptr<PreparedBatchData> batch_data;
		{
			lock_guard<mutex> l(gstate.lock);
			if (gstate.batch_data.empty()) {
				// no batch data left to flush
				break;
			}
			auto entry = gstate.batch_data.begin();
			if (entry->first != gstate.flushed_batch_index) {
				// this entry is not yet ready to be flushed
				break;
			}
			if (entry->first < gstate.flushed_batch_index) {
				throw InternalException("Batch index was out of order!?");
			}
			batch_data = std::move(entry->second);
			gstate.batch_data.erase(entry);
		}
		function.flush_batch(context, *bind_data, *gstate.global_state, *batch_data);
		gstate.flushed_batch_index++;
	}
}

void PhysicalBatchCopyToFile::FlushBatchDataSerial(ClientContext &context, GlobalSinkState &gstate_p,
                                                   idx_t min_index) const {
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();
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

void PhysicalBatchCopyToFile::FlushBatchData(ClientContext &context, GlobalSinkState &gstate_p, idx_t min_index) const {
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();

	// flush batch data to disk (if there are any to flush)
	// grab the flush lock - we can only call flush_batch with this lock
	// otherwise the data might end up in the wrong order
	lock_guard<mutex> l(gstate.flush_lock);
	if (gstate.RequiresRepartition()) {
		FlushBatchDataRepartitioned(context, gstate_p);
	} else {
		FlushBatchDataSerial(context, gstate_p, min_index);
	}
}

//===--------------------------------------------------------------------===//
// Tasks
//===--------------------------------------------------------------------===//
void PhysicalBatchCopyToFile::ExecuteTasks(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();
	while (true) {
		auto task = gstate.GetTask();
		if (!task) {
			break;
		}
		task->Execute(*this, context, gstate_p);
	}
}

//===--------------------------------------------------------------------===//
// Next Batch
//===--------------------------------------------------------------------===//
void PhysicalBatchCopyToFile::NextBatch(ExecutionContext &context, GlobalSinkState &gstate_p,
                                        LocalSinkState &lstate) const {
	auto &state = lstate.Cast<BatchCopyToLocalState>();
	auto &gstate = gstate_p.Cast<BatchCopyToGlobalState>();
	if (state.collection) {
		// we finished processing this batch
		// start flushing data
		auto min_batch_index = lstate.partition_info.min_batch_index.GetIndex();
		if (gstate.RequiresRepartition()) {
			// we have a desired batch size - we need to repartition to ensure `PrepareBatchData` is only called with
			AddRawBatchData(context.client, gstate_p, state.batch_index, std::move(state.collection));
			RepartitionBatches(context.client, gstate_p, min_batch_index);
		} else {
			// no desired batch size - we can directly prepare and flush the batch data for this batch
			PrepareBatchData(context.client, gstate_p, state.batch_index, std::move(state.collection));
		}
		gstate.AddTask(make_uniq<BatchCopyFlushTask>(min_batch_index));
		ExecuteTasks(context.client, gstate_p);
	}
	state.batch_index = lstate.partition_info.batch_index.GetIndex();

	state.InitializeCollection(context.client, *this);
}

unique_ptr<LocalSinkState> PhysicalBatchCopyToFile::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<BatchCopyToLocalState>(function.copy_to_initialize_local(context, *bind_data));
}

unique_ptr<GlobalSinkState> PhysicalBatchCopyToFile::GetGlobalSinkState(ClientContext &context) const {
	auto result = make_uniq<BatchCopyToGlobalState>(function.copy_to_initialize_global(context, *bind_data, file_path));
	if (function.desired_batch_size) {
		result->batch_size = function.desired_batch_size(context, *bind_data);
	} else {
		result->batch_size = 0;
	}
	return result;
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
