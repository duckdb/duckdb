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
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/progress_data.hpp"
#include "duckdb/parallel/interrupt.hpp"

namespace duckdb {

class ClientContext;
class Pipeline;
class PipelineBroadcastExchange;
class PipelineExecutor;
class PhysicalOperator;

enum class PipelineBroadcastExchangeCompletionMode : uint8_t { STOP_WHEN_UNCONSUMED, RUN_TO_COMPLETION };
enum class PipelineBroadcastExchangeLocalMode : uint8_t { DIRECT_ONLY, BUFFERED };
enum class PipelineBroadcastExchangeDirectPushState : uint8_t { NOT_STARTED, RESUMING, ACTIVE, FINISHED };
enum class PipelineBroadcastExchangeConsumerMode : uint8_t { UNRESOLVED, BUFFERED, DIRECT, MATERIALIZED };

struct PipelineBroadcastExchangeConsumerSummary {
	idx_t unresolved = 0;
	idx_t buffered = 0;
	idx_t direct = 0;
	idx_t materialized = 0;

	idx_t ExchangeConsumerCount() const {
		return buffered + direct;
	}
};

class PipelineBroadcastExchangeLocalState {
public:
	PipelineBroadcastExchangeLocalState(ClientContext &context, const PipelineBroadcastExchange &exchange);
	~PipelineBroadcastExchangeLocalState();

private:
	friend class PipelineBroadcastExchange;
	SinkResultType Push(DataChunk &chunk, const InterruptState &interrupt_state);
	SinkCombineResultType Finish(const InterruptState &interrupt_state);
	bool HasDirectConsumers() const;
	bool DirectConsumersFinished() const;
	void ResetPush();

	vector<unique_ptr<PipelineExecutor>> direct_executors;
	idx_t direct_idx = 0;
	idx_t direct_finalize_idx = 0;
	PipelineBroadcastExchangeLocalMode mode = PipelineBroadcastExchangeLocalMode::BUFFERED;
	PipelineBroadcastExchangeDirectPushState direct_push_state = PipelineBroadcastExchangeDirectPushState::NOT_STARTED;
};

class PipelineBroadcastExchange {
	friend class PipelineBroadcastExchangeLocalState;

public:
	PipelineBroadcastExchange(ClientContext &context, vector<LogicalType> types_p,
	                          PipelineBroadcastExchangeCompletionMode completion_mode_p);

	const vector<LogicalType> &Types() const {
		return types;
	}
	bool RunToCompletion() const {
		return completion_mode == PipelineBroadcastExchangeCompletionMode::RUN_TO_COMPLETION;
	}

	unique_ptr<PipelineBroadcastExchangeLocalState> GetLocalState(ClientContext &context) const;

	idx_t RegisterConsumer();
	bool TryRegisterDirectConsumer(Pipeline &pipeline, idx_t consumer_idx);
	void SelectBufferedConsumer(idx_t consumer_idx);
	void SelectMaterializedConsumer(idx_t consumer_idx);
	void ResetConsumerRegistrations();
	void Reset();
	void SetLogOperator(const PhysicalOperator &op);

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
	idx_t RegisteredConsumerCount() const;
	PipelineBroadcastExchangeConsumerMode GetConsumerMode(idx_t consumer_idx) const;
	PipelineBroadcastExchangeConsumerSummary GetConsumerSummary() const;

private:
	struct ChunkPool;
	struct BroadcastSpool;
	struct BroadcastSpoolReader;
	struct AppendReservation;
	struct SpoolReadReservation;
	struct BufferedChunk;
	struct BufferState;

	enum class ConsumerLifecycle : uint8_t { ACTIVE, INACTIVE };
	enum class ConsumerReadState : uint8_t { IDLE, READING };
	enum class ProducerState : uint8_t { ACTIVE, FINISHED, CANCELLED };
	enum class AppendReservationState : uint8_t { IDLE, RESERVED };
	enum class WatermarkState : uint8_t { BELOW_HIGH_WATERMARK, ABOVE_HIGH_WATERMARK };
	enum class WriterWakeMode : uint8_t { LOW_WATERMARK, FORCE };
	enum class AppendAdmission : uint8_t { READY, BLOCKED, UNCONSUMED, CANCELLED };
	enum class BufferedPushState : uint8_t { NOT_REQUIRED, APPENDED, BLOCKED, UNCONSUMED, CANCELLED };
	enum class ExchangeLogEvent : uint8_t {
		SPOOL_CREATED,
		HIGH_WATERMARK_BLOCKED,
		LOW_WATERMARK_WAKE,
		CONSUMER_UNREGISTERED
	};

	struct ExchangeLogEntry {
		ExchangeLogEvent event;
		idx_t consumer_count;
		idx_t buffered_chunks;
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
		PipelineBroadcastExchangeConsumerMode mode = PipelineBroadcastExchangeConsumerMode::UNRESOLVED;
		ConsumerLifecycle lifecycle = ConsumerLifecycle::ACTIVE;
		ConsumerReadState read_state = ConsumerReadState::IDLE;
		idx_t read_position = 0;
		shared_ptr<BroadcastSpoolReader> shared_reader;
	};

public:
	~PipelineBroadcastExchange();

private:
	BufferedPushState Append(DataChunk &chunk, const InterruptState &interrupt_state);
	SinkResultType CompletePush(DataChunk &chunk, PipelineBroadcastExchangeLocalState &lstate,
	                            BufferedPushState buffered_state);
	void RecordProducedRows(idx_t count);
	void ResetExchangeStateLocked() DUCKDB_REQUIRES(lock);
	void ResetConsumerReadStateLocked(ConsumerState &consumer, idx_t position) DUCKDB_REQUIRES(lock);
	void ResetConsumerRegistrationLocked(ConsumerState &consumer) DUCKDB_REQUIRES(lock);
	void ResetConsumerExecutionLocked(ConsumerState &consumer) DUCKDB_REQUIRES(lock);
	void DeactivateConsumerLocked(ConsumerState &consumer, idx_t position) DUCKDB_REQUIRES(lock);
	PipelineBroadcastExchangeConsumerSummary GetConsumerSummaryLocked() const DUCKDB_REQUIRES(lock);
	AppendAdmission PrepareAppendLocked(const InterruptState &interrupt_state, vector<ExchangeLogEntry> &log_entries)
	    DUCKDB_REQUIRES(lock);
	AppendAdmission ReserveAppendLocked(const InterruptState &interrupt_state, AppendReservation &reservation,
	                                    vector<ExchangeLogEntry> &log_entries) DUCKDB_REQUIRES(lock);
	BufferedPushState CompleteAppendLocked(const AppendReservation &reservation, shared_ptr<DataChunk> copy,
	                                       idx_t row_count, vector<InterruptState> &readers,
	                                       vector<InterruptState> &appenders, vector<ExchangeLogEntry> &log_entries)
	    DUCKDB_REQUIRES(lock);
	void AbortAppendReservation(vector<InterruptState> &readers, vector<InterruptState> &writers,
	                            vector<InterruptState> &appenders) DUCKDB_REQUIRES(lock);
	SourceResultType ReserveScanLocked(idx_t consumer_idx, const InterruptState &interrupt_state,
	                                   shared_ptr<DataChunk> &next_chunk, SpoolReadReservation &spool_read,
	                                   vector<InterruptState> &writers, vector<ExchangeLogEntry> &log_entries)
	    DUCKDB_REQUIRES(lock);
	void CompleteSpoolReadLocked(idx_t consumer_idx, const SpoolReadReservation &spool_read, DataChunk &chunk,
	                             vector<InterruptState> &readers, vector<InterruptState> &writers,
	                             vector<ExchangeLogEntry> &log_entries) DUCKDB_REQUIRES(lock);
	bool ShouldStopProducerLocked() const DUCKDB_REQUIRES(lock);
	bool ShouldThrottleProducerLocked() const DUCKDB_REQUIRES(lock);
	bool ShouldCreateSharedSpoolLocked() const DUCKDB_REQUIRES(lock);
	void CreateSharedSpoolLocked(vector<ExchangeLogEntry> &log_entries) DUCKDB_REQUIRES(lock);
	void RetireChunksLocked() DUCKDB_REQUIRES(lock);
	void TryReleaseBufferedStorageLocked() DUCKDB_REQUIRES(lock);
	void DeactivateAllConsumersLocked() DUCKDB_REQUIRES(lock);
	void WakeReadersLocked(vector<InterruptState> &readers) DUCKDB_REQUIRES(lock);
	void WakeWritersLocked(vector<InterruptState> &writers, vector<ExchangeLogEntry> &log_entries,
	                       WriterWakeMode mode = WriterWakeMode::LOW_WATERMARK) DUCKDB_REQUIRES(lock);
	void WakeAppendersLocked(vector<InterruptState> &appenders) DUCKDB_REQUIRES(lock);
	void LogTransitions(const vector<ExchangeLogEntry> &log_entries) const;
	static void CallbackAll(vector<InterruptState> &interrupts);

private:
	ClientContext &context;
	vector<LogicalType> types;
	PipelineBroadcastExchangeCompletionMode completion_mode;
	idx_t max_threads;
	unique_ptr<BufferState> buffer;

	mutable annotated_mutex lock;
	vector<ConsumerState> consumers;
	vector<reference<Pipeline>> direct_pipelines;
	vector<InterruptState> blocked_readers;
	vector<InterruptState> blocked_writers;
	vector<InterruptState> blocked_appenders;
	AppendReservationState append_reservation_state DUCKDB_GUARDED_BY(lock) = AppendReservationState::IDLE;
	idx_t active_consumers = 0;
	atomic<idx_t> produced_rows {0};
	ProducerState producer_state = ProducerState::ACTIVE;
	WatermarkState watermark_state = WatermarkState::BELOW_HIGH_WATERMARK;
	optional_ptr<const PhysicalOperator> log_operator;
};

} // namespace duckdb
