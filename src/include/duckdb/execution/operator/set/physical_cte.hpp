//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/deque.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class Pipeline;
class RecursiveCTEState;

class CTEExchangeData {
public:
	CTEExchangeData(ClientContext &context, vector<LogicalType> types_p, bool run_to_completion_p);

	const vector<LogicalType> &Types() const {
		return types;
	}
	bool RunToCompletion() const {
		return run_to_completion;
	}

	idx_t RegisterConsumer();
	void MarkDirectConsumer(idx_t consumer_idx);
	void Reset();
	SinkResultType Append(DataChunk &chunk, const InterruptState &interrupt_state);
	void RecordDirectConsumerProgress();
	void RecordProducedRows(idx_t count);
	void Finish();
	void Cancel();
	SourceResultType Scan(idx_t consumer_idx, DataChunk &chunk, shared_ptr<DataChunk> &current_chunk,
	                      const InterruptState &interrupt_state);
	void UnregisterConsumer(idx_t consumer_idx);
	ProgressData ScanProgress(idx_t consumer_idx, idx_t estimated_cardinality) const;
	ProgressData SinkProgress(const ProgressData &source_progress, idx_t estimated_cardinality) const;
	idx_t MaxThreads() const;
	bool HasBufferedConsumers() const;

private:
	struct ChunkPool;

	struct BufferedChunk {
		shared_ptr<DataChunk> chunk;
		idx_t bytes;
	};

	struct ConsumerState {
		idx_t position = 0;
		idx_t rows_read = 0;
		bool active = true;
		bool direct = false;
		bool detached = false;
		deque<BufferedChunk> backlog;
		idx_t backlog_base_position = 0;
		idx_t backlog_next_position = 0;
		idx_t backlog_bytes = 0;
	};

private:
	shared_ptr<DataChunk> CopyChunk(DataChunk &chunk);
	idx_t EstimateChunkSize(DataChunk &chunk) const;
	bool ShouldStopProducerLocked() const;
	bool ShouldThrottleProducerLocked() const;
	bool HasActiveSharedConsumersLocked() const;
	void DetachLaggingConsumersLocked();
	void DetachConsumerLocked(ConsumerState &consumer);
	void RetireChunksLocked();
	void RetireBacklogLocked(ConsumerState &consumer);
	void WakeReadersLocked(vector<InterruptState> &readers);
	void WakeWritersLocked(vector<InterruptState> &writers, bool force = false);
	static void CallbackAll(vector<InterruptState> &interrupts);

private:
	vector<LogicalType> types;
	bool run_to_completion;
	idx_t row_width;
	idx_t max_threads;
	idx_t high_watermark;
	idx_t low_watermark;
	shared_ptr<ChunkPool> chunk_pool;

	mutable mutex lock;
	deque<BufferedChunk> chunks;
	vector<ConsumerState> consumers;
	vector<InterruptState> blocked_readers;
	vector<InterruptState> blocked_writers;
	idx_t base_position = 0;
	idx_t next_position = 0;
	idx_t active_consumers = 0;
	idx_t buffered_bytes = 0;
	idx_t produced_rows = 0;
	bool direct_consumer_progress = false;
	bool producer_finished = false;
	bool cancelled = false;
};

class PhysicalCTEConsumerSource : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::INVALID;

public:
	PhysicalCTEConsumerSource(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality,
	                          TableIndex cte_index, shared_ptr<CTEExchangeData> exchange, idx_t consumer_idx);

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	ProgressData GetProgress(ClientContext &context, GlobalSourceState &gstate) const override;
	void SourceFinished(ClientContext &context, GlobalSourceState &gstate) const override;

	bool IsSource() const override {
		return true;
	}

	bool ParallelSource() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;

public:
	TableIndex cte_index;
	shared_ptr<CTEExchangeData> exchange;
	idx_t consumer_idx;
	bool direct_fanout = false;
};

class PhysicalCTE : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CTE;

public:
	PhysicalCTE(PhysicalPlan &physical_plan, Identifier ctename, TableIndex table_index, vector<LogicalType> types,
	            PhysicalOperator &top, PhysicalOperator &bottom, idx_t estimated_cardinality);
	~PhysicalCTE() override;

	vector<const_reference<PhysicalOperator>> cte_scans;
	vector<reference<Pipeline>> fanout_pipelines;

	shared_ptr<ColumnDataCollection> working_table;
	shared_ptr<CTEExchangeData> exchange;

	TableIndex table_index;
	Identifier ctename;
	bool cte_body_is_dml = false;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

	bool SinkOrderDependent() const override {
		return false;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;

	ProgressData GetSinkProgress(ClientContext &context, GlobalSinkState &gstate,
	                             const ProgressData source_progress) const override;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	bool TryRegisterFanoutPipeline(Pipeline &pipeline, idx_t consumer_idx);

	vector<const_reference<PhysicalOperator>> GetSources() const override;
};

} // namespace duckdb
