#include "duckdb/execution/operator/set/physical_cte.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

static constexpr const idx_t CTE_EXCHANGE_HIGH_WATERMARK = 32ULL * 1024ULL * 1024ULL;
static constexpr const idx_t CTE_EXCHANGE_LOW_WATERMARK = CTE_EXCHANGE_HIGH_WATERMARK / 2;

static idx_t EstimateTypeWidth(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BIT:
	case PhysicalType::VARCHAR:
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
	case PhysicalType::ARRAY:
		return 16;
	case PhysicalType::INVALID:
		return 1;
	default:
		return GetTypeIdSize(type.InternalType());
	}
}

static idx_t EstimateRowWidth(const vector<LogicalType> &types) {
	idx_t result = 0;
	for (auto &type : types) {
		result += EstimateTypeWidth(type);
	}
	return MaxValue<idx_t>(result, 1);
}

static bool RequiresResetForReuse(const vector<LogicalType> &types) {
	for (auto &type : types) {
		if (!TypeIsConstantSize(type.InternalType())) {
			return true;
		}
	}
	return false;
}

struct CTEExchangeData::ChunkPool : public enable_shared_from_this<CTEExchangeData::ChunkPool> {
	ChunkPool(vector<LogicalType> types_p, idx_t row_width_p, idx_t max_threads_p, idx_t max_cached_bytes_p)
	    : types(std::move(types_p)), row_width(row_width_p), reset_on_reuse(RequiresResetForReuse(types)),
	      max_cached_chunks(MaxValue<idx_t>(
	          MaxValue<idx_t>(max_cached_bytes_p / MaxValue<idx_t>(row_width_p * STANDARD_VECTOR_SIZE, 1), 1),
	          MaxValue<idx_t>(max_threads_p * 4, 16))),
	      max_cached_bytes(max_cached_bytes_p) {
	}

	shared_ptr<DataChunk> Acquire(DataChunk &source) {
		unique_ptr<DataChunk> result;
		{
			lock_guard<mutex> guard(lock);
			if (!cached_chunks.empty()) {
				result = std::move(cached_chunks.back());
				cached_chunks.pop_back();
				cached_bytes -= MinValue<idx_t>(cached_bytes, EstimateChunkSize(*result));
			}
		}

		if (!result) {
			result = make_uniq<DataChunk>();
			result->Initialize(Allocator::DefaultAllocator(), types);
		} else if (reset_on_reuse) {
			result->Reset();
		} else {
			result->SetChildCardinality(0);
		}

		source.Copy(*result);

		auto self = shared_from_this();
		return shared_ptr<DataChunk>(result.release(), [self](DataChunk *chunk) {
			unique_ptr<DataChunk> owned(chunk);
			self->Release(std::move(owned));
		});
	}

	void Release(unique_ptr<DataChunk> chunk) {
		auto bytes = EstimateChunkSize(*chunk);
		lock_guard<mutex> guard(lock);
		if (cached_chunks.size() >= max_cached_chunks || cached_bytes + bytes > max_cached_bytes) {
			return;
		}
		cached_bytes += bytes;
		cached_chunks.push_back(std::move(chunk));
	}

	idx_t EstimateChunkSize(DataChunk &chunk) const {
		return MaxValue<idx_t>(row_width * MaxValue<idx_t>(chunk.size(), STANDARD_VECTOR_SIZE), 1);
	}

	vector<LogicalType> types;
	idx_t row_width;
	bool reset_on_reuse;
	idx_t max_cached_chunks;
	idx_t max_cached_bytes;

	mutex lock;
	vector<unique_ptr<DataChunk>> cached_chunks;
	idx_t cached_bytes = 0;
};

CTEExchangeData::CTEExchangeData(ClientContext &context, vector<LogicalType> types_p, bool run_to_completion_p)
    : types(std::move(types_p)), run_to_completion(run_to_completion_p), row_width(EstimateRowWidth(types)),
      max_threads(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads())),
      high_watermark(CTE_EXCHANGE_HIGH_WATERMARK), low_watermark(CTE_EXCHANGE_LOW_WATERMARK) {
	chunk_pool = make_shared_ptr<ChunkPool>(types, row_width, max_threads, high_watermark);
}

idx_t CTEExchangeData::RegisterConsumer() {
	lock_guard<mutex> guard(lock);
	ConsumerState state;
	state.position = base_position;
	consumers.push_back(std::move(state));
	active_consumers++;
	return consumers.size() - 1;
}

void CTEExchangeData::MarkDirectConsumer(idx_t consumer_idx) {
	lock_guard<mutex> guard(lock);
	D_ASSERT(consumer_idx < consumers.size());
	auto &consumer = consumers[consumer_idx];
	if (consumer.direct) {
		return;
	}
	consumer.direct = true;
	if (consumer.active) {
		D_ASSERT(active_consumers > 0);
		active_consumers--;
	}
	consumer.active = false;
	consumer.position = base_position;
	consumer.detached = false;
	consumer.backlog.clear();
	consumer.backlog_base_position = base_position;
	consumer.backlog_next_position = base_position;
	consumer.backlog_bytes = 0;
}

void CTEExchangeData::Reset() {
	vector<InterruptState> readers;
	vector<InterruptState> writers;
	{
		lock_guard<mutex> guard(lock);
		chunks.clear();
		base_position = 0;
		next_position = 0;
		buffered_bytes = 0;
		produced_rows = 0;
		direct_consumer_progress = false;
		producer_finished = false;
		cancelled = false;
		active_consumers = 0;
		for (auto &consumer : consumers) {
			consumer.position = base_position;
			consumer.rows_read = 0;
			consumer.active = !consumer.direct;
			if (consumer.active) {
				active_consumers++;
			}
			consumer.detached = false;
			consumer.backlog.clear();
			consumer.backlog_base_position = base_position;
			consumer.backlog_next_position = base_position;
			consumer.backlog_bytes = 0;
		}
		WakeReadersLocked(readers);
		WakeWritersLocked(writers);
	}
	CallbackAll(readers);
	CallbackAll(writers);
}

SinkResultType CTEExchangeData::Append(DataChunk &chunk, const InterruptState &interrupt_state) {
	{
		lock_guard<mutex> guard(lock);
		if (cancelled || ShouldStopProducerLocked()) {
			return SinkResultType::FINISHED;
		}
		DetachLaggingConsumersLocked();
		if (ShouldThrottleProducerLocked()) {
			blocked_writers.push_back(interrupt_state);
			return SinkResultType::BLOCKED;
		}
	}

	auto copy = CopyChunk(chunk);
	auto bytes = EstimateChunkSize(*copy);
	vector<InterruptState> readers;
	{
		lock_guard<mutex> guard(lock);
		if (cancelled || ShouldStopProducerLocked()) {
			return SinkResultType::FINISHED;
		}
		DetachLaggingConsumersLocked();
		if (ShouldThrottleProducerLocked()) {
			blocked_writers.push_back(interrupt_state);
			return SinkResultType::BLOCKED;
		}
		BufferedChunk buffered {copy, bytes};
		if (HasActiveSharedConsumersLocked()) {
			chunks.push_back(buffered);
			buffered_bytes += bytes;
		}
		for (auto &consumer : consumers) {
			if (!consumer.active || !consumer.detached) {
				continue;
			}
			consumer.backlog.push_back(buffered);
			consumer.backlog_next_position++;
			consumer.backlog_bytes += bytes;
		}
		if (active_consumers > 0) {
			next_position++;
			WakeReadersLocked(readers);
		}
		produced_rows += chunk.size();
	}
	CallbackAll(readers);
	return SinkResultType::NEED_MORE_INPUT;
}

void CTEExchangeData::RecordDirectConsumerProgress() {
	lock_guard<mutex> guard(lock);
	direct_consumer_progress = true;
}

void CTEExchangeData::RecordProducedRows(idx_t count) {
	lock_guard<mutex> guard(lock);
	produced_rows += count;
}

void CTEExchangeData::Finish() {
	vector<InterruptState> readers;
	vector<InterruptState> writers;
	{
		lock_guard<mutex> guard(lock);
		producer_finished = true;
		WakeReadersLocked(readers);
		WakeWritersLocked(writers);
	}
	CallbackAll(readers);
	CallbackAll(writers);
}

void CTEExchangeData::Cancel() {
	vector<InterruptState> readers;
	vector<InterruptState> writers;
	{
		lock_guard<mutex> guard(lock);
		cancelled = true;
		producer_finished = true;
		WakeReadersLocked(readers);
		WakeWritersLocked(writers);
	}
	CallbackAll(readers);
	CallbackAll(writers);
}

SourceResultType CTEExchangeData::Scan(idx_t consumer_idx, DataChunk &chunk, shared_ptr<DataChunk> &current_chunk,
                                       const InterruptState &interrupt_state) {
	vector<InterruptState> writers;
	auto result = SourceResultType::HAVE_MORE_OUTPUT;
	shared_ptr<DataChunk> next_chunk;
	{
		lock_guard<mutex> guard(lock);
		D_ASSERT(consumer_idx < consumers.size());
		auto &consumer = consumers[consumer_idx];
		if (!consumer.active || cancelled) {
			return SourceResultType::FINISHED;
		}
		if (consumer.detached && consumer.position < consumer.backlog_next_position) {
			D_ASSERT(consumer.position >= consumer.backlog_base_position);
			auto chunk_idx = consumer.position - consumer.backlog_base_position;
			D_ASSERT(chunk_idx < consumer.backlog.size());
			next_chunk = consumer.backlog[chunk_idx].chunk;
			consumer.position++;
			consumer.rows_read += next_chunk->size();
			RetireBacklogLocked(consumer);
		} else if (!consumer.detached && consumer.position < next_position) {
			D_ASSERT(consumer.position >= base_position);
			auto chunk_idx = consumer.position - base_position;
			D_ASSERT(chunk_idx < chunks.size());
			next_chunk = chunks[chunk_idx].chunk;
			consumer.position++;
			consumer.rows_read += next_chunk->size();
			RetireChunksLocked();
			WakeWritersLocked(writers);
		} else if (producer_finished) {
			consumer.active = false;
			D_ASSERT(active_consumers > 0);
			active_consumers--;
			RetireChunksLocked();
			WakeWritersLocked(writers, true);
			result = SourceResultType::FINISHED;
		} else {
			WakeWritersLocked(writers, true);
			blocked_readers.push_back(interrupt_state);
			result = SourceResultType::BLOCKED;
		}
	}
	CallbackAll(writers);
	if (next_chunk) {
		current_chunk = std::move(next_chunk);
		chunk.Reference(*current_chunk);
	}
	return result;
}

void CTEExchangeData::UnregisterConsumer(idx_t consumer_idx) {
	vector<InterruptState> readers;
	vector<InterruptState> writers;
	{
		lock_guard<mutex> guard(lock);
		if (consumer_idx >= consumers.size()) {
			return;
		}
		auto &consumer = consumers[consumer_idx];
		if (!consumer.active) {
			return;
		}
		consumer.active = false;
		consumer.position = next_position;
		consumer.detached = false;
		consumer.backlog.clear();
		consumer.backlog_base_position = next_position;
		consumer.backlog_next_position = next_position;
		consumer.backlog_bytes = 0;
		D_ASSERT(active_consumers > 0);
		active_consumers--;
		RetireChunksLocked();
		WakeReadersLocked(readers);
		WakeWritersLocked(writers, true);
	}
	CallbackAll(readers);
	CallbackAll(writers);
}

ProgressData CTEExchangeData::ScanProgress(idx_t consumer_idx, idx_t estimated_cardinality) const {
	lock_guard<mutex> guard(lock);
	ProgressData progress;
	if (consumer_idx >= consumers.size()) {
		progress.SetInvalid();
		return progress;
	}
	auto total = produced_rows;
	if (!producer_finished) {
		static constexpr const idx_t MAX_PROGRESS_CARDINALITY = 1ULL << 48ULL;
		if (estimated_cardinality > 0 && estimated_cardinality < MAX_PROGRESS_CARDINALITY) {
			total = MaxValue<idx_t>(total, estimated_cardinality);
		} else {
			total = MaxValue<idx_t>(total, consumers[consumer_idx].rows_read + 1);
		}
	}
	total = MaxValue<idx_t>(total, 1);
	progress.done = MinValue<double>(double(consumers[consumer_idx].rows_read), double(total));
	progress.total = double(total);
	return progress;
}

ProgressData CTEExchangeData::SinkProgress(const ProgressData &source_progress, idx_t estimated_cardinality) const {
	lock_guard<mutex> guard(lock);
	ProgressData progress;
	auto produced = double(produced_rows);
	if (producer_finished) {
		auto total = MaxValue<double>(produced, 1.0);
		progress.done = total;
		progress.total = total;
		return progress;
	}
	progress.done = produced;
	if (source_progress.IsValid()) {
		progress.total = produced + MaxValue<double>(source_progress.total - source_progress.done, 1.0);
	} else {
		static constexpr const idx_t MAX_PROGRESS_CARDINALITY = 1ULL << 48ULL;
		if (estimated_cardinality > 0 && estimated_cardinality < MAX_PROGRESS_CARDINALITY) {
			progress.total = double(MaxValue<idx_t>(estimated_cardinality, produced_rows + 1));
		} else {
			progress.total = produced + 1.0;
		}
	}
	if (progress.done > progress.total) {
		progress.total = progress.done;
	}
	return progress;
}

idx_t CTEExchangeData::MaxThreads() const {
	return MaxValue<idx_t>(max_threads, 1);
}

bool CTEExchangeData::HasBufferedConsumers() const {
	lock_guard<mutex> guard(lock);
	return active_consumers > 0;
}

shared_ptr<DataChunk> CTEExchangeData::CopyChunk(DataChunk &chunk) {
	return chunk_pool->Acquire(chunk);
}

idx_t CTEExchangeData::EstimateChunkSize(DataChunk &chunk) const {
	return MaxValue<idx_t>(row_width * chunk.size(), 1);
}

bool CTEExchangeData::ShouldStopProducerLocked() const {
	return !run_to_completion && active_consumers == 0;
}

bool CTEExchangeData::ShouldThrottleProducerLocked() const {
	return !run_to_completion && HasActiveSharedConsumersLocked() && buffered_bytes >= high_watermark;
}

bool CTEExchangeData::HasActiveSharedConsumersLocked() const {
	for (auto &consumer : consumers) {
		if (consumer.active && !consumer.detached) {
			return true;
		}
	}
	return false;
}

void CTEExchangeData::DetachLaggingConsumersLocked() {
	if (run_to_completion || buffered_bytes < high_watermark || chunks.empty()) {
		return;
	}

	bool has_progressed_consumer = direct_consumer_progress;
	bool has_lagging_consumer = false;
	for (auto &consumer : consumers) {
		if (!consumer.active || consumer.detached) {
			continue;
		}
		if (consumer.position > base_position) {
			has_progressed_consumer = true;
		} else if (consumer.position == base_position) {
			has_lagging_consumer = true;
		}
	}
	if (!has_progressed_consumer || !has_lagging_consumer) {
		return;
	}

	for (auto &consumer : consumers) {
		if (!consumer.active || consumer.detached || consumer.position != base_position) {
			continue;
		}
		DetachConsumerLocked(consumer);
	}
	RetireChunksLocked();
}

void CTEExchangeData::DetachConsumerLocked(ConsumerState &consumer) {
	D_ASSERT(!consumer.detached);
	D_ASSERT(consumer.position >= base_position);
	consumer.detached = true;
	consumer.backlog.clear();
	consumer.backlog_base_position = consumer.position;
	consumer.backlog_next_position = next_position;
	consumer.backlog_bytes = 0;

	auto start_offset = consumer.position - base_position;
	for (idx_t chunk_idx = start_offset; chunk_idx < chunks.size(); chunk_idx++) {
		consumer.backlog.push_back(chunks[chunk_idx]);
		consumer.backlog_bytes += chunks[chunk_idx].bytes;
	}
}

void CTEExchangeData::RetireChunksLocked() {
	if (chunks.empty()) {
		return;
	}
	idx_t min_position = next_position;
	bool found_active = false;
	for (auto &consumer : consumers) {
		if (!consumer.active || consumer.detached) {
			continue;
		}
		found_active = true;
		min_position = MinValue(min_position, consumer.position);
	}
	if (!found_active) {
		min_position = next_position;
	}
	while (!chunks.empty() && base_position < min_position) {
		buffered_bytes -= MinValue(buffered_bytes, chunks.front().bytes);
		chunks.pop_front();
		base_position++;
	}
}

void CTEExchangeData::RetireBacklogLocked(ConsumerState &consumer) {
	while (!consumer.backlog.empty() && consumer.backlog_base_position < consumer.position) {
		consumer.backlog_bytes -= MinValue(consumer.backlog_bytes, consumer.backlog.front().bytes);
		consumer.backlog.pop_front();
		consumer.backlog_base_position++;
	}
}

void CTEExchangeData::WakeReadersLocked(vector<InterruptState> &readers) {
	readers.insert(readers.end(), blocked_readers.begin(), blocked_readers.end());
	blocked_readers.clear();
}

void CTEExchangeData::WakeWritersLocked(vector<InterruptState> &writers, bool force) {
	if (!force && buffered_bytes > low_watermark && !cancelled && !ShouldStopProducerLocked() && !producer_finished) {
		return;
	}
	writers.insert(writers.end(), blocked_writers.begin(), blocked_writers.end());
	blocked_writers.clear();
}

void CTEExchangeData::CallbackAll(vector<InterruptState> &interrupts) {
	for (auto &interrupt : interrupts) {
		interrupt.Callback();
	}
}

class CTEConsumerGlobalSourceState : public GlobalSourceState {
public:
	CTEConsumerGlobalSourceState(shared_ptr<CTEExchangeData> exchange_p, idx_t consumer_idx_p)
	    : exchange(std::move(exchange_p)), consumer_idx(consumer_idx_p) {
	}

	~CTEConsumerGlobalSourceState() override {
		Unregister();
	}

	idx_t MaxThreads() override {
		return exchange->MaxThreads();
	}

	void Unregister() {
		if (unregistered) {
			return;
		}
		unregistered = true;
		exchange->UnregisterConsumer(consumer_idx);
	}

	shared_ptr<CTEExchangeData> exchange;
	idx_t consumer_idx;
	bool unregistered = false;
};

class CTEConsumerLocalSourceState : public LocalSourceState {
public:
	shared_ptr<DataChunk> current_chunk;
};

PhysicalCTEConsumerSource::PhysicalCTEConsumerSource(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                     idx_t estimated_cardinality, TableIndex cte_index,
                                                     shared_ptr<CTEExchangeData> exchange, idx_t consumer_idx)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::CTE_SCAN, std::move(types), estimated_cardinality),
      cte_index(cte_index), exchange(std::move(exchange)), consumer_idx(consumer_idx) {
}

unique_ptr<GlobalSourceState> PhysicalCTEConsumerSource::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<CTEConsumerGlobalSourceState>(exchange, consumer_idx);
}

unique_ptr<LocalSourceState> PhysicalCTEConsumerSource::GetLocalSourceState(ExecutionContext &context,
                                                                            GlobalSourceState &gstate) const {
	return make_uniq<CTEConsumerLocalSourceState>();
}

SourceResultType PhysicalCTEConsumerSource::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                            OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<CTEConsumerGlobalSourceState>();
	auto &lstate = input.local_state.Cast<CTEConsumerLocalSourceState>();
	return gstate.exchange->Scan(gstate.consumer_idx, chunk, lstate.current_chunk, input.interrupt_state);
}

ProgressData PhysicalCTEConsumerSource::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	auto &state = gstate.Cast<CTEConsumerGlobalSourceState>();
	return state.exchange->ScanProgress(state.consumer_idx, estimated_cardinality);
}

void PhysicalCTEConsumerSource::SourceFinished(ClientContext &context, GlobalSourceState &gstate) const {
	gstate.Cast<CTEConsumerGlobalSourceState>().Unregister();
}

InsertionOrderPreservingMap<string> PhysicalCTEConsumerSource::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Index"] = StringUtil::Format("%llu", cte_index.index);
	result["CTE Mode"] = direct_fanout ? "DIRECT_FANOUT" : "STREAMING_FANOUT";
	result["Consumer"] = StringUtil::Format("%llu", consumer_idx);
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

PhysicalCTE::PhysicalCTE(PhysicalPlan &physical_plan, Identifier ctename, TableIndex table_index,
                         vector<LogicalType> types, PhysicalOperator &top, PhysicalOperator &bottom,
                         idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::CTE, std::move(types), estimated_cardinality),
      table_index(table_index), ctename(std::move(ctename)) {
	children.push_back(top);
	children.push_back(bottom);
}

PhysicalCTE::~PhysicalCTE() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CTEGlobalState : public GlobalSinkState {
public:
	explicit CTEGlobalState(ClientContext &context, const PhysicalCTE &op)
	    : op(op), working_table_ref(op.working_table.get()), exchange(op.exchange) {
		ResetState(context);
	}
	const PhysicalCTE &op;
	optional_ptr<ColumnDataCollection> working_table_ref;
	shared_ptr<CTEExchangeData> exchange;

	mutex lhs_lock;

private:
	void ResetState(ClientContext &context) {
		if (exchange) {
			exchange->Reset();
			working_table_ref = nullptr;
		} else {
			op.working_table->Reset();
			working_table_ref = op.working_table.get();
		}
		GlobalSinkState::Reset(context);
	}

public:
	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ClientContext &context) override {
		ResetState(context);
	}

	void MergeIT(ColumnDataCollection &input) {
		lock_guard<mutex> guard(lhs_lock);
		working_table_ref->Combine(input);
	}
};

class CTELocalState : public LocalSinkState {
public:
	explicit CTELocalState(ClientContext &context, const PhysicalCTE &op)
	    : lhs_data(context, op.working_table ? op.working_table->Types() : op.exchange->Types()),
	      materialized_mode(!op.exchange) {
		if (materialized_mode) {
			lhs_data.InitializeAppend(append_state);
		}
		for (auto &pipeline_ref : op.fanout_pipelines) {
			auto &pipeline = pipeline_ref.get();
			pipeline.PrepareExternalInput();
			fanout_executors.push_back(make_uniq<PipelineExecutor>(context, pipeline));
		}
	}

	unique_ptr<LocalSinkState> distinct_state;
	ColumnDataCollection lhs_data;
	ColumnDataAppendState append_state;
	bool materialized_mode;
	vector<unique_ptr<PipelineExecutor>> fanout_executors;
	bool waiting_for_fanout = false;
	idx_t fanout_idx = 0;
	idx_t fanout_finalize_idx = 0;
	bool fanout_done_for_chunk = false;
	bool fanout_all_finished_for_chunk = false;

	void Append(DataChunk &input) {
		D_ASSERT(materialized_mode);
		lhs_data.Append(append_state, input);
	}

	void ResetFanoutChunk() {
		fanout_done_for_chunk = false;
		fanout_all_finished_for_chunk = false;
	}
};

unique_ptr<GlobalSinkState> PhysicalCTE::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<CTEGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalCTE::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<CTELocalState>(context.client, *this);
	return std::move(state);
}

static SinkResultType SinkFanout(DataChunk &chunk, CTELocalState &lstate, const InterruptState &interrupt_state,
                                 bool &all_finished) {
	if (!lstate.waiting_for_fanout) {
		lstate.fanout_idx = 0;
	}
	lstate.waiting_for_fanout = false;
	for (; lstate.fanout_idx < lstate.fanout_executors.size(); lstate.fanout_idx++) {
		auto &executor = *lstate.fanout_executors[lstate.fanout_idx];
		executor.SetInterruptState(interrupt_state);
		if (executor.IsFinishedProcessing()) {
			continue;
		}
		auto result = executor.PushExternal(chunk);
		if (result == PipelineExecuteResult::INTERRUPTED) {
			lstate.waiting_for_fanout = true;
			all_finished = false;
			return SinkResultType::BLOCKED;
		}
	}

	all_finished = true;
	for (auto &executor : lstate.fanout_executors) {
		if (!executor->IsFinishedProcessing()) {
			all_finished = false;
			break;
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

static SinkCombineResultType CombineFanout(CTELocalState &lstate, const InterruptState &interrupt_state) {
	for (; lstate.fanout_finalize_idx < lstate.fanout_executors.size(); lstate.fanout_finalize_idx++) {
		auto &executor = *lstate.fanout_executors[lstate.fanout_finalize_idx];
		executor.SetInterruptState(interrupt_state);
		auto result = PipelineExecuteResult::NOT_FINISHED;
		while (result == PipelineExecuteResult::NOT_FINISHED) {
			result = executor.FinishExternal();
		}
		if (result == PipelineExecuteResult::INTERRUPTED) {
			return SinkCombineResultType::BLOCKED;
		}
	}
	return SinkCombineResultType::FINISHED;
}

SinkResultType PhysicalCTE::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CTEGlobalState>();
	auto &lstate = input.local_state.Cast<CTELocalState>();
	if (exchange) {
		D_ASSERT(gstate.exchange);
		bool all_direct_finished = lstate.fanout_all_finished_for_chunk;
		bool rows_recorded = false;
		if (!lstate.fanout_done_for_chunk && !lstate.fanout_executors.empty()) {
			auto fanout_result = SinkFanout(chunk, lstate, input.interrupt_state, all_direct_finished);
			if (fanout_result == SinkResultType::BLOCKED) {
				return SinkResultType::BLOCKED;
			}
			lstate.fanout_done_for_chunk = true;
			lstate.fanout_all_finished_for_chunk = all_direct_finished;
			gstate.exchange->RecordDirectConsumerProgress();
		}

		auto has_buffered_consumers = gstate.exchange->HasBufferedConsumers();
		if (has_buffered_consumers) {
			auto append_result = gstate.exchange->Append(chunk, input.interrupt_state);
			if (append_result == SinkResultType::BLOCKED) {
				return SinkResultType::BLOCKED;
			}
			if (append_result == SinkResultType::FINISHED) {
				if (!rows_recorded) {
					gstate.exchange->RecordProducedRows(chunk.size());
					rows_recorded = true;
				}
				lstate.ResetFanoutChunk();
				if (!cte_body_is_dml && (lstate.fanout_executors.empty() || all_direct_finished)) {
					return SinkResultType::FINISHED;
				}
				return SinkResultType::NEED_MORE_INPUT;
			}
			rows_recorded = true;
		}

		if (!rows_recorded) {
			gstate.exchange->RecordProducedRows(chunk.size());
		}
		lstate.ResetFanoutChunk();
		if (!cte_body_is_dml && !has_buffered_consumers && (lstate.fanout_executors.empty() || all_direct_finished)) {
			return SinkResultType::FINISHED;
		}
		return SinkResultType::NEED_MORE_INPUT;
	}
	lstate.lhs_data.Append(lstate.append_state, chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCTE::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<CTELocalState>();
	if (!lstate.fanout_executors.empty()) {
		auto fanout_result = CombineFanout(lstate, input.interrupt_state);
		if (fanout_result == SinkCombineResultType::BLOCKED) {
			return SinkCombineResultType::BLOCKED;
		}
	}
	if (exchange) {
		return SinkCombineResultType::FINISHED;
	}
	auto &gstate = input.global_state.Cast<CTEGlobalState>();
	gstate.MergeIT(lstate.lhs_data);

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCTE::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	if (exchange) {
		auto &gstate = input.global_state.Cast<CTEGlobalState>();
		gstate.exchange->Finish();
		for (auto &pipeline_ref : fanout_pipelines) {
			pipeline_ref.get().CompleteExternalInput();
		}
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalCTE::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	D_ASSERT(children.size() == 2);
	op_state.reset();
	sink_state.reset();

	auto &state = meta_pipeline.GetState();

	auto &child_meta_pipeline =
	    meta_pipeline.CreateChildMetaPipeline(current, *this, MetaPipelineType::REGULAR, !exchange);
	child_meta_pipeline.Build(children[0]);

	for (auto &cte_scan : cte_scans) {
		state.cte_dependencies.insert(make_pair(cte_scan, reference<Pipeline>(*child_meta_pipeline.GetBasePipeline())));
	}

	// If the CTE body is a DML statement (INSERT/UPDATE/DELETE/MERGE INTO), all MetaPipelines
	// created while building children[1] (the query side) must run after the DML completes.
	// We follow the same pattern as PhysicalJoin::BuildJoinPipelines: capture the DML pipelines
	// and the current last child before building children[1], then call AddRecursiveDependencies
	// with force=true so that ordering is always enforced (not just when pipelines exceed the
	// thread count, as is the case for join build dependencies).
	vector<shared_ptr<Pipeline>> dml_pipelines;
	optional_ptr<MetaPipeline> last_child_ptr;
	if (cte_body_is_dml) {
		child_meta_pipeline.GetPipelines(dml_pipelines, false);
		last_child_ptr = meta_pipeline.GetLastChild();
	}

	children[1].get().BuildPipelines(current, meta_pipeline);

	if (exchange && last_child_ptr && !current.HasDataflowDependencies()) {
		for (auto &dml_pipeline : dml_pipelines) {
			current.AddDependency(dml_pipeline);
		}
	}
	if (last_child_ptr) {
		meta_pipeline.AddRecursiveDependencies(dml_pipelines, *last_child_ptr, true, !!exchange);
	}
}

bool PhysicalCTE::TryRegisterFanoutPipeline(Pipeline &pipeline, idx_t consumer_idx) {
	if (!exchange || !pipeline.CanUseExternalInput()) {
		return false;
	}
	exchange->MarkDirectConsumer(consumer_idx);
	fanout_pipelines.push_back(pipeline);
	return true;
}

vector<const_reference<PhysicalOperator>> PhysicalCTE::GetSources() const {
	return children[1].get().GetSources();
}

InsertionOrderPreservingMap<string> PhysicalCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Name"] = ctename.GetIdentifierName();
	result["Table Index"] = StringUtil::Format("%llu", table_index.index);
	result["Execution Mode"] = exchange ? "STREAMING_FANOUT" : "MATERIALIZED";
	if (exchange && exchange->RunToCompletion()) {
		result["Run To Completion"] = "true";
	}
	if (exchange) {
		if (fanout_pipelines.empty()) {
			result["Fanout Mode"] = "BUFFERED";
		} else if (fanout_pipelines.size() == cte_scans.size()) {
			result["Fanout Mode"] = "DIRECT";
		} else {
			result["Fanout Mode"] = "MIXED";
		}
		result["Chunk Storage"] = "POOLED";
	}
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

ProgressData PhysicalCTE::GetSinkProgress(ClientContext &context, GlobalSinkState &gstate,
                                          const ProgressData source_progress) const {
	auto &state = gstate.Cast<CTEGlobalState>();
	if (state.exchange) {
		return state.exchange->SinkProgress(source_progress, estimated_cardinality);
	}
	lock_guard<mutex> guard(state.lhs_lock);
	if (!state.working_table_ref) {
		return ProgressData {0, 1, true};
	}
	auto &working_table = *state.working_table_ref;
	auto count = double(working_table.Count());
	ProgressData progress;
	progress.done = count;
	progress.total = count + source_progress.total;
	return progress;
}

} // namespace duckdb
