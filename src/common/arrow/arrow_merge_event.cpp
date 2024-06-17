#include "duckdb/common/arrow/arrow_merge_event.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Arrow Batch Task
//===--------------------------------------------------------------------===//

ArrowBatchTask::ArrowBatchTask(ArrowQueryResult &result, vector<idx_t> record_batch_indices, Executor &executor,
                               shared_ptr<Event> event_p, BatchCollectionChunkScanState scan_state,
                               vector<string> names, idx_t batch_size)
    : ExecutorTask(executor, event_p), result(result), record_batch_indices(std::move(record_batch_indices)),
      event(std::move(event_p)), batch_size(batch_size), names(std::move(names)), scan_state(std::move(scan_state)) {
}

void ArrowBatchTask::ProduceRecordBatches() {
	auto &arrays = result.Arrays();
	auto arrow_options = executor.context.GetClientProperties();
	for (auto &index : record_batch_indices) {
		auto &array = arrays[index];
		D_ASSERT(array);
		idx_t count;
		count = ArrowUtil::FetchChunk(scan_state, arrow_options, batch_size, &array->arrow_array);
		(void)count;
		D_ASSERT(count != 0);
	}
}

TaskExecutionResult ArrowBatchTask::ExecuteTask(TaskExecutionMode mode) {
	ProduceRecordBatches();
	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

//===--------------------------------------------------------------------===//
// Arrow Merge Event
//===--------------------------------------------------------------------===//

ArrowMergeEvent::ArrowMergeEvent(ArrowQueryResult &result, BatchedDataCollection &batches, Pipeline &pipeline_p)
    : BasePipelineEvent(pipeline_p), result(result), batches(batches) {
	record_batch_size = result.BatchSize();
}

vector<idx_t> ArrowMergeEvent::ProduceRecordBatchIndices(idx_t index) {
	auto batch_index = batches.IndexToBatchIndex(index);
	auto tuples_in_batch = batches.BatchSize(batch_index);

	auto full_batches = tuples_in_batch / record_batch_size;
	auto remainder = tuples_in_batch % record_batch_size;
	auto total_batches = full_batches + !!remainder;
	vector<idx_t> record_batch_indices(total_batches);
	for (idx_t i = 0; i < total_batches; i++) {
		record_batch_indices[i] = record_batch_index++;
	}
	return record_batch_indices;
}

void ArrowMergeEvent::Schedule() {
	vector<shared_ptr<Task>> tasks;

	for (idx_t index = 0; index < batches.BatchCount(); index++) {
		auto record_batch_indices = ProduceRecordBatchIndices(index);

		auto batch_range = batches.BatchRange(index, index + 1);

		// Prepare the initial state for this scan state
		BatchCollectionChunkScanState scan_state(batches, batch_range, pipeline->executor.context);

		tasks.push_back(make_uniq<ArrowBatchTask>(result, std::move(record_batch_indices), pipeline->executor,
		                                          shared_from_this(), std::move(scan_state), result.names,
		                                          record_batch_size));
	}
	// Allocate the list of record batches inside the query result
	{
		vector<unique_ptr<ArrowArrayWrapper>> arrays;
		arrays.resize(record_batch_index);
		for (idx_t i = 0; i < record_batch_index; i++) {
			arrays[i] = make_uniq<ArrowArrayWrapper>();
		}
		result.SetArrowData(std::move(arrays));
	}
	D_ASSERT(!tasks.empty());
	SetTasks(std::move(tasks));
}

} // namespace duckdb
