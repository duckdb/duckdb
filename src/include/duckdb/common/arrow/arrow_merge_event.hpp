#pragma once

#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/task.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/main/chunk_scan_state/batched_data_collection.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/arrow/arrow_query_result.hpp"

namespace duckdb {

// Task to create one RecordBatch by (partially) scanning a BatchedDataCollection
class ArrowBatchTask : public ExecutorTask {
public:
	ArrowBatchTask(ArrowQueryResult &result, vector<idx_t> record_batch_indices, Executor &executor,
	               shared_ptr<Event> event_p, BatchCollectionChunkScanState scan_state, vector<string> names,
	               idx_t batch_size)
	    : ExecutorTask(executor, event_p), result(result), record_batch_indices(std::move(record_batch_indices)),
	      event(std::move(event_p)), batch_size(batch_size), names(std::move(names)),
	      scan_state(std::move(scan_state)) {
	}

	void ProduceRecordBatches() {
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

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		ProduceRecordBatches();
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	ArrowQueryResult &result;
	vector<idx_t> record_batch_indices;
	shared_ptr<Event> event;
	idx_t batch_size;
	vector<string> names;
	BatchCollectionChunkScanState scan_state;
};

class ArrowMergeEvent : public BasePipelineEvent {
public:
	ArrowMergeEvent(ArrowQueryResult &result, BatchedDataCollection &batches, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), result(result), batches(batches) {
		record_batch_size = result.BatchSize();
	}

	ArrowQueryResult &result;
	BatchedDataCollection &batches;

public:
	vector<idx_t> ProduceRecordBatchIndices(idx_t index) {
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

	void Schedule() override {
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

private:
	idx_t record_batch_index = 0;
	//! The max size of a record batch to output
	idx_t record_batch_size;
};

} // namespace duckdb
