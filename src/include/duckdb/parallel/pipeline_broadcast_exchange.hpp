//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline_broadcast_exchange.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/progress_data.hpp"
#include "duckdb/parallel/interrupt.hpp"

namespace duckdb {

class ClientContext;
class Pipeline;
class PipelineBroadcastExchange;
class PipelineExecutor;

class PipelineBroadcastExchangeLocalState {
public:
	PipelineBroadcastExchangeLocalState(ClientContext &context, const PipelineBroadcastExchange &exchange);
	~PipelineBroadcastExchangeLocalState();

private:
	friend class PipelineBroadcastExchange;

	vector<unique_ptr<PipelineExecutor>> direct_executors;
	bool waiting_for_direct = false;
	idx_t direct_idx = 0;
	idx_t direct_finalize_idx = 0;
	bool direct_done_for_chunk = false;
	bool direct_all_finished_for_chunk = false;
	bool direct_only = false;
};

class PipelineBroadcastExchange {
	friend class PipelineBroadcastExchangeLocalState;

public:
	PipelineBroadcastExchange(ClientContext &context, vector<LogicalType> types_p, bool run_to_completion_p);

	const vector<LogicalType> &Types() const {
		return types;
	}
	bool RunToCompletion() const {
		return run_to_completion;
	}

	unique_ptr<PipelineBroadcastExchangeLocalState> GetLocalState(ClientContext &context) const;

	idx_t RegisterConsumer();
	bool TryRegisterDirectConsumer(Pipeline &pipeline, idx_t consumer_idx);
	void Reset();

	SinkResultType Push(DataChunk &chunk, PipelineBroadcastExchangeLocalState &lstate,
	                    const InterruptState &interrupt_state);
	SinkCombineResultType FinishLocal(PipelineBroadcastExchangeLocalState &lstate,
	                                  const InterruptState &interrupt_state);
	void Finish();
	void FinishDirectConsumers();
	void Cancel();

	SourceResultType Scan(idx_t consumer_idx, DataChunk &chunk, shared_ptr<DataChunk> &current_chunk,
	                      const InterruptState &interrupt_state);
	void UnregisterConsumer(idx_t consumer_idx);

	ProgressData ScanProgress(idx_t consumer_idx, idx_t estimated_cardinality) const;
	ProgressData SinkProgress(const ProgressData &source_progress, idx_t estimated_cardinality) const;
	idx_t MaxThreads() const;
	idx_t ConsumerCount() const;
	idx_t DirectConsumerCount() const;
	bool HasBufferedConsumers() const;

private:
	struct ChunkPool;
	struct ConsumerSpool;
	struct ConsumerSpoolReader;

	struct BufferedChunk {
		shared_ptr<DataChunk> chunk;
		idx_t bytes;
	};

	struct ConsumerState {
		ConsumerState();
		~ConsumerState();
		ConsumerState(ConsumerState &&other) noexcept;
		ConsumerState &operator=(ConsumerState &&other) noexcept;
		ConsumerState(const ConsumerState &other) = delete;
		ConsumerState &operator=(const ConsumerState &other) = delete;

		idx_t position = 0;
		idx_t rows_read = 0;
		bool active = true;
		bool direct = false;
		bool detached = false;
		unique_ptr<ConsumerSpool> detached_buffer;
		unique_ptr<ConsumerSpoolReader> shared_reader;
		unique_ptr<ConsumerSpoolReader> detached_reader;
	};

public:
	~PipelineBroadcastExchange();

private:
	SinkResultType Append(DataChunk &chunk, const InterruptState &interrupt_state);
	void RecordDirectConsumerProgress();
	void RecordProducedRows(idx_t count);
	SinkResultType PushDirectConsumers(DataChunk &chunk, PipelineBroadcastExchangeLocalState &lstate,
	                                   const InterruptState &interrupt_state, bool &all_finished);
	SinkCombineResultType FinishDirectConsumers(PipelineBroadcastExchangeLocalState &lstate,
	                                            const InterruptState &interrupt_state);
	void ResetPushChunk(PipelineBroadcastExchangeLocalState &lstate);
	shared_ptr<DataChunk> CopyChunk(DataChunk &chunk);
	idx_t EstimateChunkSize(DataChunk &chunk) const;
	bool ShouldStopProducerLocked() const;
	bool ShouldThrottleProducerLocked() const;
	bool HasActiveSharedConsumersLocked() const;
	bool UseSharedSpoolLocked() const;
	idx_t ThrottledBufferedBytesLocked() const;
	void DetachLaggingConsumersLocked();
	void DetachConsumerLocked(ConsumerState &consumer);
	void RetireChunksLocked();
	void RetireDetachedBufferLocked(ConsumerState &consumer);
	void ClearDetachedBufferLocked(ConsumerState &consumer);
	void WakeReadersLocked(vector<InterruptState> &readers);
	void WakeWritersLocked(vector<InterruptState> &writers, bool force = false);
	static void CallbackAll(vector<InterruptState> &interrupts);

private:
	ClientContext &context;
	vector<LogicalType> types;
	bool run_to_completion;
	idx_t row_width;
	idx_t max_threads;
	idx_t high_watermark;
	idx_t low_watermark;
	shared_ptr<ChunkPool> chunk_pool;

	mutable mutex lock;
	deque<BufferedChunk> chunks;
	unique_ptr<ConsumerSpool> shared_spool;
	vector<ConsumerState> consumers;
	vector<reference<Pipeline>> direct_pipelines;
	vector<InterruptState> blocked_readers;
	vector<InterruptState> blocked_writers;
	idx_t base_position = 0;
	idx_t next_position = 0;
	idx_t active_consumers = 0;
	idx_t shared_buffered_bytes = 0;
	idx_t detached_buffered_bytes = 0;
	atomic<idx_t> produced_rows {0};
	atomic<bool> direct_consumer_progress {false};
	bool producer_finished = false;
	bool cancelled = false;
};

} // namespace duckdb
