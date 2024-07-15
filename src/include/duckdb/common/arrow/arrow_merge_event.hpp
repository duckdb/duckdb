//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/arrow_merge_event.hpp
//
//
//===----------------------------------------------------------------------===//

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
	               idx_t batch_size);
	void ProduceRecordBatches();
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

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
	ArrowMergeEvent(ArrowQueryResult &result, BatchedDataCollection &batches, Pipeline &pipeline_p);

public:
	void Schedule() override;

public:
	ArrowQueryResult &result;
	BatchedDataCollection &batches;

private:
	//! The max size of a record batch to output
	idx_t record_batch_size;
};

} // namespace duckdb
