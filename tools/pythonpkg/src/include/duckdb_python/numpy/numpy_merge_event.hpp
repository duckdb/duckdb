#pragma once

#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/task.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

// Task to append from a batched data collection into a NumpyResultConversion
class NumpyResultAppendTask : public ExecutorTask {
public:
	NumpyResultAppendTask(Executor &executor, shared_ptr<Event> event_p, BatchedChunkScanState scan_state,
	                      NumpyResultConversion &result, idx_t offset, BatchedDataCollection &source)
	    : ExecutorTask(executor), event(std::move(event_p)), offset(offset), result(result),
	      scan_state(std::move(scan_state)), source(source) {
	}

	void AppendToResult() {
		D_ASSERT(offset < result.Capacity());

		auto &allocator = BufferManager::GetBufferManager(executor.context).GetBufferAllocator();

		DataChunk intermediate;
		intermediate.Initialize(allocator, result.Types());
		while (scan_state.range.begin != scan_state.range.end) {
			source.Scan(scan_state, intermediate);
			if (intermediate.size() == 0) {
				break;
			}
			result.Append(intermediate, offset);
			offset += intermediate.size();
		}
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		AppendToResult();
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	idx_t offset;
	NumpyResultConversion &result;
	BatchedChunkScanState scan_state;
	BatchedDataCollection &source;
};

// Spawn tasks to populate a pre-allocated NumpyConversionResult
class NumpyMergeEvent : public BasePipelineEvent {
public:
	NumpyMergeEvent(NumpyResultConversion &result, BatchedDataCollection &batches, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), result(result), batches(batches) {
	}

	NumpyResultConversion &result;
	BatchedDataCollection &batches;

public:
	void Schedule() override {
		vector<shared_ptr<Task>> tasks;
		idx_t offset = 0;
		for (idx_t i = 0; i < batches.BatchCount(); i++) {
			// Create a range of 1 batch per task
			auto batch_range = batches.BatchRange(i, i + 1);
			BatchedChunkScanState batch_scan_state;
			batches.InitializeScan(batch_scan_state, batch_range);

			// Create a task to populate the numpy result with this batch at this offset
			tasks.push_back(make_uniq<NumpyResultAppendTask>(pipeline->executor, shared_from_this(),
			                                                 std::move(batch_scan_state), result, offset, batches));

			// Forward the offset
			auto batch_index = batches.IndexToBatchIndex(i);
			auto row_count = batches.BatchSize(batch_index);
			offset += row_count;
		}
		D_ASSERT(!tasks.empty());
		SetTasks(std::move(tasks));
	}
};

} // namespace duckdb
