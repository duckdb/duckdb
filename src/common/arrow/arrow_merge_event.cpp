#include "duckdb/common/arrow/arrow_merge_event.hpp"
#include "duckdb/common/arrow/arrow_util.hpp"
#include "duckdb/storage/storage_info.hpp"

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

namespace {

struct BatchesForTask {
	idx_t tuple_count;
	BatchedChunkIteratorRange batches;
};

struct BatchesToTaskTransformer {
public:
	explicit BatchesToTaskTransformer(BatchedDataCollection &batches) : batches(batches), batch_index(0) {
		batch_count = batches.BatchCount();
	}
	idx_t GetIndex() const {
		return batch_index;
	}
	bool TryGetNextBatchSize(idx_t &tuple_count) {
		if (batch_index >= batch_count) {
			return false;
		}
		auto internal_index = batches.IndexToBatchIndex(batch_index++);
		auto tuples_in_batch = batches.BatchSize(internal_index);
		tuple_count = tuples_in_batch;
		return true;
	}

public:
	BatchedDataCollection &batches;
	idx_t batch_index;
	idx_t batch_count;
};

} // namespace

void ArrowMergeEvent::Schedule() {
	vector<shared_ptr<Task>> tasks;

	BatchesToTaskTransformer transformer(batches);
	vector<BatchesForTask> task_data;
	bool finished = false;
	// First we convert our list of batches into units of Storage::ROW_GROUP_SIZE tuples each
	while (!finished) {
		idx_t tuples_for_task = 0;
		idx_t start_index = transformer.GetIndex();
		idx_t end_index = start_index;
		while (tuples_for_task < DEFAULT_ROW_GROUP_SIZE) {
			idx_t batch_size;
			if (!transformer.TryGetNextBatchSize(batch_size)) {
				finished = true;
				break;
			}
			end_index++;
			tuples_for_task += batch_size;
		}
		if (start_index == end_index) {
			break;
		}
		BatchesForTask batches_for_task;
		batches_for_task.tuple_count = tuples_for_task;
		batches_for_task.batches = batches.BatchRange(start_index, end_index);
		task_data.push_back(batches_for_task);
	}

	// Now we produce tasks from these units
	// Every task is given a scan_state created from the range of batches
	// and a vector of indices indicating the arrays (record batches) they should populate
	idx_t record_batch_index = 0;
	for (auto &data : task_data) {
		const auto tuples = data.tuple_count;

		auto full_batches = tuples / record_batch_size;
		auto remainder = tuples % record_batch_size;
		auto total_batches = full_batches + !!remainder;

		vector<idx_t> record_batch_indices(total_batches);
		for (idx_t i = 0; i < total_batches; i++) {
			record_batch_indices[i] = record_batch_index++;
		}

		BatchCollectionChunkScanState scan_state(batches, data.batches, pipeline->executor.context);
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
