//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/merge_action_queue.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/deque.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parallel/interrupt.hpp"

namespace duckdb {
class ClientContext;

//! BOUNDED queues hand the rows to the consumer as they are pushed, and block the producer while the consumer cannot
//! keep up. MATERIALIZED queues buffer all rows (spilling to disk if required) and only hand them out once the
//! producer is done - required for actions whose pipeline can only run after another action has completed.
enum class MergeActionQueueMode : uint8_t { BOUNDED, MATERIALIZED };

//! Scan state of a single consumer
class MergeActionQueueScanState {
public:
	//! BOUNDED - the chunk that is currently being scanned
	shared_ptr<DataChunk> current_chunk;
	//! MATERIALIZED - the scan state within the buffered data
	ColumnDataScanState scan_state;
	bool scan_initialized = false;
};

//! Connects the PhysicalMergeInto to the pipeline of a single merge action - the merge into pushes the rows of the
//! action into the queue, and the source of the action pipeline scans them. Producers block while the queue is full,
//! consumers block while it is empty.
class MergeActionQueue {
public:
	MergeActionQueue(ClientContext &context, vector<LogicalType> types, MergeActionQueueMode mode,
	                 idx_t max_buffered_chunks);
	~MergeActionQueue();

	MergeActionQueueMode Mode() const {
		return mode;
	}

	const vector<LogicalType> &Types() const {
		return types;
	}

	//! Push a chunk into the queue - the data is copied. Returns BLOCKED if the queue is full.
	SinkResultType Push(DataChunk &chunk, const InterruptState &interrupt_state);
	//! Signal that no more data will be pushed into the queue
	void Finish();
	//! Signal that the pipeline of the action no longer consumes any data - rows that are pushed are discarded
	void ConsumerFinished();
	//! Abandon the queue - unblocks all producers and consumers
	void Cancel();
	//! Scan the next chunk. Returns BLOCKED while the queue is empty and the producers are not done yet.
	SourceResultType Scan(DataChunk &chunk, MergeActionQueueScanState &scan_state,
	                      const InterruptState &interrupt_state);
	//! The number of rows that have been pushed into the queue
	idx_t RowsPushed() const;
	//! The number of rows that are currently buffered in the queue
	idx_t RowsBuffered() const;

private:
	struct ChunkPool;

	static void CallbackAll(const vector<InterruptState> &states);
	SinkResultType PushBounded(DataChunk &chunk, const InterruptState &interrupt_state);
	SinkResultType PushMaterialized(DataChunk &chunk);
	SourceResultType ScanBounded(DataChunk &chunk, MergeActionQueueScanState &scan_state,
	                             const InterruptState &interrupt_state);
	SourceResultType ScanMaterialized(DataChunk &chunk, MergeActionQueueScanState &scan_state,
	                                  const InterruptState &interrupt_state);

private:
	vector<LogicalType> types;
	MergeActionQueueMode mode;
	idx_t max_buffered_chunks;
	shared_ptr<ChunkPool> pool;
	//! MATERIALIZED - the buffered rows
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;

	mutable mutex lock;
	//! Chunks that have been pushed but not yet scanned
	deque<shared_ptr<DataChunk>> chunks;
	//! Chunk slots that have been reserved by a producer that is currently copying its data
	idx_t reserved_slots = 0;
	//! Consumers that are waiting for data
	vector<InterruptState> blocked_consumers;
	//! Producers that are waiting for space
	vector<InterruptState> blocked_producers;
	bool finished = false;
	bool cancelled = false;
	bool consumer_finished = false;
	idx_t rows_pushed = 0;
	idx_t rows_buffered = 0;
};

} // namespace duckdb
