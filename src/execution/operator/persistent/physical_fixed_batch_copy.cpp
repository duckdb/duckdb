#include "duckdb/execution/operator/persistent/physical_fixed_batch_copy.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/execution/operator/persistent/physical_batch_copy_to_file.hpp"

#include <algorithm>

namespace duckdb {

PhysicalFixedBatchCopy::PhysicalFixedBatchCopy(vector<LogicalType> types, CopyFunction function_p,
                                               unique_ptr<FunctionData> bind_data_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::BATCH_COPY_TO_FILE, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data_p)) {
	if (!function.flush_batch || !function.prepare_batch || !function.desired_batch_size) {
		throw InternalException("PhysicalFixedBatchCopy created for copy function that does not have "
		                        "prepare_batch/flush_batch/desired_batch_size defined");
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BatchCopyTask {
public:
	virtual ~BatchCopyTask() {
	}

	virtual void Execute(const PhysicalFixedBatchCopy &op, ClientContext &context, GlobalSinkState &gstate_p) = 0;
};

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class FixedBatchCopyGlobalState : public GlobalSinkState {
public:
	explicit FixedBatchCopyGlobalState(unique_ptr<GlobalFunctionData> global_state)
	    : rows_copied(0), global_state(std::move(global_state)), batch_size(0), scheduled_batch_index(0),
	      flushed_batch_index(0), any_flushing(false), any_finished(false) {
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
	//! Whether or not any thread is flushing
	atomic<bool> any_flushing;
	//! Whether or not any threads are finished
	atomic<bool> any_finished;

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

	idx_t TaskCount() {
		lock_guard<mutex> l(task_lock);
		return task_queue.size();
	}

	void AddBatchData(idx_t batch_index, unique_ptr<PreparedBatchData> new_batch) {
		// move the batch data to the set of prepared batch data
		lock_guard<mutex> l(lock);
		auto entry = batch_data.insert(make_pair(batch_index, std::move(new_batch)));
		if (!entry.second) {
			throw InternalException("Duplicate batch index %llu encountered in PhysicalFixedBatchCopy", batch_index);
		}
	}

private:
	mutex task_lock;
	//! The task queue for the batch copy to file
	queue<unique_ptr<BatchCopyTask>> task_queue;
};

class FixedBatchCopyLocalState : public LocalSinkState {
public:
	explicit FixedBatchCopyLocalState(unique_ptr<LocalFunctionData> local_state_p)
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
SinkResultType PhysicalFixedBatchCopy::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {
	auto &state = input.local_state.Cast<FixedBatchCopyLocalState>();
	if (!state.collection) {
		state.InitializeCollection(context.client, *this);
		state.batch_index = state.partition_info.batch_index.GetIndex();
	}
	state.rows_copied += chunk.size();
	state.collection->Append(state.append_state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalFixedBatchCopy::Combine(ExecutionContext &context,
                                                      OperatorSinkCombineInput &input) const {
	auto &state = input.local_state.Cast<FixedBatchCopyLocalState>();
	auto &gstate = input.global_state.Cast<FixedBatchCopyGlobalState>();
	gstate.rows_copied += state.rows_copied;
	if (!gstate.any_finished) {
		// signal that this thread is finished processing batches and that we should move on to Finalize
		lock_guard<mutex> l(gstate.lock);
		gstate.any_finished = true;
	}
	ExecuteTasks(context.client, gstate);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// ProcessRemainingBatchesEvent
//===--------------------------------------------------------------------===//
class ProcessRemainingBatchesTask : public ExecutorTask {
public:
	ProcessRemainingBatchesTask(Executor &executor, shared_ptr<Event> event_p, FixedBatchCopyGlobalState &state_p,
	                            ClientContext &context, const PhysicalFixedBatchCopy &op)
	    : ExecutorTask(executor), event(std::move(event_p)), op(op), gstate(state_p), context(context) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		while (op.ExecuteTask(context, gstate)) {
			op.FlushBatchData(context, gstate, 0);
		}
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	const PhysicalFixedBatchCopy &op;
	FixedBatchCopyGlobalState &gstate;
	ClientContext &context;
};

class ProcessRemainingBatchesEvent : public BasePipelineEvent {
public:
	ProcessRemainingBatchesEvent(const PhysicalFixedBatchCopy &op_p, FixedBatchCopyGlobalState &gstate_p,
	                             Pipeline &pipeline_p, ClientContext &context)
	    : BasePipelineEvent(pipeline_p), op(op_p), gstate(gstate_p), context(context) {
	}
	const PhysicalFixedBatchCopy &op;
	FixedBatchCopyGlobalState &gstate;
	ClientContext &context;

public:
	void Schedule() override {
		vector<shared_ptr<Task>> tasks;
		for (idx_t i = 0; i < idx_t(TaskScheduler::GetScheduler(context).NumberOfThreads()); i++) {
			auto process_task =
			    make_uniq<ProcessRemainingBatchesTask>(pipeline->executor, shared_from_this(), gstate, context, op);
			tasks.push_back(std::move(process_task));
		}
		D_ASSERT(!tasks.empty());
		SetTasks(std::move(tasks));
	}

	void FinishEvent() override {
		//! Now that all batches are processed we finish flushing the file to disk
		op.FinalFlush(context, gstate);
	}
};
//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalFixedBatchCopy::FinalFlush(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<FixedBatchCopyGlobalState>();
	if (gstate.TaskCount() != 0) {
		throw InternalException("Unexecuted tasks are remaining in PhysicalFixedBatchCopy::FinalFlush!?");
	}
	idx_t min_batch_index = idx_t(NumericLimits<int64_t>::Maximum());
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

SinkFinalizeType PhysicalFixedBatchCopy::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<FixedBatchCopyGlobalState>();
	idx_t min_batch_index = idx_t(NumericLimits<int64_t>::Maximum());
	// repartition any remaining batches
	RepartitionBatches(context, input.global_state, min_batch_index, true);
	// check if we have multiple tasks to execute
	if (gstate.TaskCount() <= 1) {
		// we don't - just execute the remaining task and finish flushing to disk
		ExecuteTasks(context, input.global_state);
		FinalFlush(context, input.global_state);
		return SinkFinalizeType::READY;
	}
	// we have multiple tasks remaining - launch an event to execute the tasks in parallel
	auto new_event = make_shared<ProcessRemainingBatchesEvent>(*this, gstate, pipeline, context);
	event.InsertEvent(std::move(new_event));
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Tasks
//===--------------------------------------------------------------------===//
class RepartitionedFlushTask : public BatchCopyTask {
public:
	RepartitionedFlushTask() {
	}

	void Execute(const PhysicalFixedBatchCopy &op, ClientContext &context, GlobalSinkState &gstate_p) override {
		op.FlushBatchData(context, gstate_p, 0);
	}
};

class PrepareBatchTask : public BatchCopyTask {
public:
	PrepareBatchTask(idx_t batch_index, unique_ptr<ColumnDataCollection> collection_p)
	    : batch_index(batch_index), collection(std::move(collection_p)) {
	}

	idx_t batch_index;
	unique_ptr<ColumnDataCollection> collection;

	void Execute(const PhysicalFixedBatchCopy &op, ClientContext &context, GlobalSinkState &gstate_p) override {
		auto &gstate = gstate_p.Cast<FixedBatchCopyGlobalState>();
		auto batch_data =
		    op.function.prepare_batch(context, *op.bind_data, *gstate.global_state, std::move(collection));
		gstate.AddBatchData(batch_index, std::move(batch_data));
		if (batch_index == gstate.flushed_batch_index) {
			gstate.AddTask(make_uniq<RepartitionedFlushTask>());
		}
	}
};

//===--------------------------------------------------------------------===//
// Batch Data Handling
//===--------------------------------------------------------------------===//
void PhysicalFixedBatchCopy::AddRawBatchData(ClientContext &context, GlobalSinkState &gstate_p, idx_t batch_index,
                                             unique_ptr<ColumnDataCollection> collection) const {
	auto &gstate = gstate_p.Cast<FixedBatchCopyGlobalState>();

	// add the batch index to the set of raw batches
	lock_guard<mutex> l(gstate.lock);
	auto entry = gstate.raw_batches.insert(make_pair(batch_index, std::move(collection)));
	if (!entry.second) {
		throw InternalException("Duplicate batch index %llu encountered in PhysicalFixedBatchCopy", batch_index);
	}
}

static bool CorrectSizeForBatch(idx_t collection_size, idx_t desired_size) {
	return idx_t(AbsValue<int64_t>(int64_t(collection_size) - int64_t(desired_size))) < STANDARD_VECTOR_SIZE;
}

void PhysicalFixedBatchCopy::RepartitionBatches(ClientContext &context, GlobalSinkState &gstate_p, idx_t min_index,
                                                bool final) const {
	auto &gstate = gstate_p.Cast<FixedBatchCopyGlobalState>();

	// repartition batches until the min index is reached
	lock_guard<mutex> l(gstate.lock);
	if (gstate.raw_batches.empty()) {
		return;
	}
	if (!final) {
		if (gstate.any_finished) {
			// we only repartition in ::NextBatch if all threads are still busy processing batches
			// otherwise we might end up repartitioning a lot of data with only a few threads remaining
			// which causes erratic performance
			return;
		}
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
	idx_t max_batch_index = 0;
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
				current_collection = make_uniq<ColumnDataCollection>(BufferAllocator::Get(context), children[0]->types);
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
			current_collection = make_uniq<ColumnDataCollection>(BufferAllocator::Get(context), children[0]->types);
			current_collection->InitializeAppend(append_state);
		}
	}
	if (current_collection && current_collection->Count() > 0) {
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

void PhysicalFixedBatchCopy::FlushBatchData(ClientContext &context, GlobalSinkState &gstate_p, idx_t min_index) const {
	auto &gstate = gstate_p.Cast<FixedBatchCopyGlobalState>();

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

//===--------------------------------------------------------------------===//
// Tasks
//===--------------------------------------------------------------------===//
bool PhysicalFixedBatchCopy::ExecuteTask(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<FixedBatchCopyGlobalState>();
	auto task = gstate.GetTask();
	if (!task) {
		return false;
	}
	task->Execute(*this, context, gstate_p);
	return true;
}

void PhysicalFixedBatchCopy::ExecuteTasks(ClientContext &context, GlobalSinkState &gstate_p) const {
	while (ExecuteTask(context, gstate_p)) {
	}
}

//===--------------------------------------------------------------------===//
// Next Batch
//===--------------------------------------------------------------------===//
void PhysicalFixedBatchCopy::NextBatch(ExecutionContext &context, GlobalSinkState &gstate_p,
                                       LocalSinkState &lstate) const {
	auto &state = lstate.Cast<FixedBatchCopyLocalState>();
	if (state.collection && state.collection->Count() > 0) {
		// we finished processing this batch
		// start flushing data
		auto min_batch_index = lstate.partition_info.min_batch_index.GetIndex();
		// push the raw batch data into the set of unprocessed batches
		AddRawBatchData(context.client, gstate_p, state.batch_index.GetIndex(), std::move(state.collection));
		// attempt to repartition to our desired batch size
		RepartitionBatches(context.client, gstate_p, min_batch_index);
		// execute a single batch task
		ExecuteTask(context.client, gstate_p);
		FlushBatchData(context.client, gstate_p, min_batch_index);
	}
	state.batch_index = lstate.partition_info.batch_index.GetIndex();

	state.InitializeCollection(context.client, *this);
}

unique_ptr<LocalSinkState> PhysicalFixedBatchCopy::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<FixedBatchCopyLocalState>(function.copy_to_initialize_local(context, *bind_data));
}

unique_ptr<GlobalSinkState> PhysicalFixedBatchCopy::GetGlobalSinkState(ClientContext &context) const {
	auto result =
	    make_uniq<FixedBatchCopyGlobalState>(function.copy_to_initialize_global(context, *bind_data, file_path));
	result->batch_size = function.desired_batch_size(context, *bind_data);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalFixedBatchCopy::GetData(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &g = sink_state->Cast<FixedBatchCopyGlobalState>();

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(g.rows_copied));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
