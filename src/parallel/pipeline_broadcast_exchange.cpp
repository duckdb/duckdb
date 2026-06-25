#include "duckdb/parallel/pipeline_broadcast_exchange.hpp"

#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

static constexpr const idx_t PIPELINE_BROADCAST_HIGH_WATERMARK = 32ULL * 1024ULL * 1024ULL;
static constexpr const idx_t PIPELINE_BROADCAST_LOW_WATERMARK = PIPELINE_BROADCAST_HIGH_WATERMARK / 2;

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

struct PipelineBroadcastExchange::ChunkPool : public enable_shared_from_this<PipelineBroadcastExchange::ChunkPool> {
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

PipelineBroadcastExchangeLocalState::PipelineBroadcastExchangeLocalState(ClientContext &context,
                                                                         const PipelineBroadcastExchange &exchange) {
	vector<reference<Pipeline>> direct_pipeline_refs;
	{
		lock_guard<mutex> guard(exchange.lock);
		direct_only = !exchange.consumers.empty() && exchange.active_consumers == 0 &&
		              exchange.direct_pipelines.size() == exchange.consumers.size();
		direct_pipeline_refs = exchange.direct_pipelines;
	}

	for (auto &pipeline_ref : direct_pipeline_refs) {
		auto &pipeline = pipeline_ref.get();
		pipeline.PrepareExternalInput();
		direct_executors.push_back(make_uniq<PipelineExecutor>(context, pipeline));
	}
}

PipelineBroadcastExchangeLocalState::~PipelineBroadcastExchangeLocalState() = default;

PipelineBroadcastExchange::PipelineBroadcastExchange(ClientContext &context, vector<LogicalType> types_p,
                                                     bool run_to_completion_p)
    : types(std::move(types_p)), run_to_completion(run_to_completion_p), row_width(EstimateRowWidth(types)),
      max_threads(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads())),
      high_watermark(PIPELINE_BROADCAST_HIGH_WATERMARK), low_watermark(PIPELINE_BROADCAST_LOW_WATERMARK) {
	chunk_pool = make_shared_ptr<ChunkPool>(types, row_width, max_threads, high_watermark);
}

unique_ptr<PipelineBroadcastExchangeLocalState> PipelineBroadcastExchange::GetLocalState(ClientContext &context) const {
	return make_uniq<PipelineBroadcastExchangeLocalState>(context, *this);
}

idx_t PipelineBroadcastExchange::RegisterConsumer() {
	lock_guard<mutex> guard(lock);
	ConsumerState state;
	state.position = base_position;
	consumers.push_back(std::move(state));
	active_consumers++;
	return consumers.size() - 1;
}

bool PipelineBroadcastExchange::TryRegisterDirectConsumer(Pipeline &pipeline, idx_t consumer_idx) {
	if (!pipeline.CanUseExternalInput()) {
		return false;
	}
	lock_guard<mutex> guard(lock);
	D_ASSERT(consumer_idx < consumers.size());
	auto &consumer = consumers[consumer_idx];
	if (consumer.direct) {
		return true;
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
	direct_pipelines.push_back(pipeline);
	return true;
}

void PipelineBroadcastExchange::Reset() {
	vector<InterruptState> readers;
	vector<InterruptState> writers;
	{
		lock_guard<mutex> guard(lock);
		chunks.clear();
		base_position = 0;
		next_position = 0;
		buffered_bytes = 0;
		produced_rows.store(0, std::memory_order_relaxed);
		direct_consumer_progress.store(false, std::memory_order_relaxed);
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

SinkResultType PipelineBroadcastExchange::Push(DataChunk &chunk, PipelineBroadcastExchangeLocalState &lstate,
                                               const InterruptState &interrupt_state) {
	bool all_direct_finished = lstate.direct_all_finished_for_chunk;
	if (lstate.direct_only) {
		D_ASSERT(!lstate.direct_executors.empty());
		if (!lstate.direct_done_for_chunk) {
			auto direct_result = PushDirectConsumers(chunk, lstate, interrupt_state, all_direct_finished);
			if (direct_result == SinkResultType::BLOCKED) {
				return SinkResultType::BLOCKED;
			}
			lstate.direct_done_for_chunk = true;
			lstate.direct_all_finished_for_chunk = all_direct_finished;
		}
		RecordProducedRows(chunk.size());
		ResetPushChunk(lstate);
		if (!RunToCompletion() && all_direct_finished) {
			return SinkResultType::FINISHED;
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	bool rows_recorded = false;
	if (!lstate.direct_done_for_chunk && !lstate.direct_executors.empty()) {
		auto direct_result = PushDirectConsumers(chunk, lstate, interrupt_state, all_direct_finished);
		if (direct_result == SinkResultType::BLOCKED) {
			return SinkResultType::BLOCKED;
		}
		lstate.direct_done_for_chunk = true;
		lstate.direct_all_finished_for_chunk = all_direct_finished;
		RecordDirectConsumerProgress();
	}

	auto has_buffered_consumers = HasBufferedConsumers();
	if (has_buffered_consumers) {
		auto append_result = Append(chunk, interrupt_state);
		if (append_result == SinkResultType::BLOCKED) {
			return SinkResultType::BLOCKED;
		}
		if (append_result == SinkResultType::FINISHED) {
			if (!rows_recorded) {
				RecordProducedRows(chunk.size());
				rows_recorded = true;
			}
			ResetPushChunk(lstate);
			if (!RunToCompletion() && (lstate.direct_executors.empty() || all_direct_finished)) {
				return SinkResultType::FINISHED;
			}
			return SinkResultType::NEED_MORE_INPUT;
		}
		rows_recorded = true;
	}

	if (!rows_recorded) {
		RecordProducedRows(chunk.size());
	}
	ResetPushChunk(lstate);
	if (!RunToCompletion() && !has_buffered_consumers && (lstate.direct_executors.empty() || all_direct_finished)) {
		return SinkResultType::FINISHED;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PipelineBroadcastExchange::FinishLocal(PipelineBroadcastExchangeLocalState &lstate,
                                                             const InterruptState &interrupt_state) {
	return FinishDirectConsumers(lstate, interrupt_state);
}

SinkResultType PipelineBroadcastExchange::PushDirectConsumers(DataChunk &chunk,
                                                              PipelineBroadcastExchangeLocalState &lstate,
                                                              const InterruptState &interrupt_state,
                                                              bool &all_finished) {
	if (!lstate.waiting_for_direct) {
		lstate.direct_idx = 0;
	}
	lstate.waiting_for_direct = false;
	for (; lstate.direct_idx < lstate.direct_executors.size(); lstate.direct_idx++) {
		auto &executor = *lstate.direct_executors[lstate.direct_idx];
		executor.SetInterruptState(interrupt_state);
		if (executor.IsFinishedProcessing()) {
			continue;
		}
		auto result = executor.PushExternal(chunk);
		if (result == PipelineExecuteResult::INTERRUPTED) {
			lstate.waiting_for_direct = true;
			all_finished = false;
			return SinkResultType::BLOCKED;
		}
	}

	all_finished = true;
	for (auto &executor : lstate.direct_executors) {
		if (!executor->IsFinishedProcessing()) {
			all_finished = false;
			break;
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PipelineBroadcastExchange::FinishDirectConsumers(PipelineBroadcastExchangeLocalState &lstate,
                                                                       const InterruptState &interrupt_state) {
	for (; lstate.direct_finalize_idx < lstate.direct_executors.size(); lstate.direct_finalize_idx++) {
		auto &executor = *lstate.direct_executors[lstate.direct_finalize_idx];
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

void PipelineBroadcastExchange::ResetPushChunk(PipelineBroadcastExchangeLocalState &lstate) {
	lstate.direct_done_for_chunk = false;
	lstate.direct_all_finished_for_chunk = false;
}

SinkResultType PipelineBroadcastExchange::Append(DataChunk &chunk, const InterruptState &interrupt_state) {
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
		RecordProducedRows(chunk.size());
	}
	CallbackAll(readers);
	return SinkResultType::NEED_MORE_INPUT;
}

void PipelineBroadcastExchange::RecordDirectConsumerProgress() {
	direct_consumer_progress.store(true, std::memory_order_relaxed);
}

void PipelineBroadcastExchange::RecordProducedRows(idx_t count) {
	produced_rows.fetch_add(count, std::memory_order_relaxed);
}

void PipelineBroadcastExchange::Finish() {
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

void PipelineBroadcastExchange::FinishDirectConsumers() {
	for (auto &pipeline_ref : direct_pipelines) {
		pipeline_ref.get().CompleteExternalInput();
	}
}

void PipelineBroadcastExchange::Cancel() {
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

SourceResultType PipelineBroadcastExchange::Scan(idx_t consumer_idx, DataChunk &chunk,
                                                 shared_ptr<DataChunk> &current_chunk,
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

void PipelineBroadcastExchange::UnregisterConsumer(idx_t consumer_idx) {
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

ProgressData PipelineBroadcastExchange::ScanProgress(idx_t consumer_idx, idx_t estimated_cardinality) const {
	lock_guard<mutex> guard(lock);
	ProgressData progress;
	if (consumer_idx >= consumers.size()) {
		progress.SetInvalid();
		return progress;
	}
	auto total = produced_rows.load(std::memory_order_relaxed);
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

ProgressData PipelineBroadcastExchange::SinkProgress(const ProgressData &source_progress,
                                                     idx_t estimated_cardinality) const {
	lock_guard<mutex> guard(lock);
	ProgressData progress;
	auto produced_count = produced_rows.load(std::memory_order_relaxed);
	auto produced = double(produced_count);
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
			progress.total = double(MaxValue<idx_t>(estimated_cardinality, produced_count + 1));
		} else {
			progress.total = produced + 1.0;
		}
	}
	if (progress.done > progress.total) {
		progress.total = progress.done;
	}
	return progress;
}

idx_t PipelineBroadcastExchange::MaxThreads() const {
	return MaxValue<idx_t>(max_threads, 1);
}

idx_t PipelineBroadcastExchange::ConsumerCount() const {
	lock_guard<mutex> guard(lock);
	return consumers.size();
}

idx_t PipelineBroadcastExchange::DirectConsumerCount() const {
	lock_guard<mutex> guard(lock);
	return direct_pipelines.size();
}

bool PipelineBroadcastExchange::HasBufferedConsumers() const {
	lock_guard<mutex> guard(lock);
	return active_consumers > 0;
}

shared_ptr<DataChunk> PipelineBroadcastExchange::CopyChunk(DataChunk &chunk) {
	return chunk_pool->Acquire(chunk);
}

idx_t PipelineBroadcastExchange::EstimateChunkSize(DataChunk &chunk) const {
	return MaxValue<idx_t>(row_width * chunk.size(), 1);
}

bool PipelineBroadcastExchange::ShouldStopProducerLocked() const {
	return !run_to_completion && active_consumers == 0;
}

bool PipelineBroadcastExchange::ShouldThrottleProducerLocked() const {
	return !run_to_completion && HasActiveSharedConsumersLocked() && buffered_bytes >= high_watermark;
}

bool PipelineBroadcastExchange::HasActiveSharedConsumersLocked() const {
	for (auto &consumer : consumers) {
		if (consumer.active && !consumer.detached) {
			return true;
		}
	}
	return false;
}

void PipelineBroadcastExchange::DetachLaggingConsumersLocked() {
	if (run_to_completion || buffered_bytes < high_watermark || chunks.empty()) {
		return;
	}

	bool has_progressed_consumer = direct_consumer_progress.load(std::memory_order_relaxed);
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

void PipelineBroadcastExchange::DetachConsumerLocked(ConsumerState &consumer) {
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

void PipelineBroadcastExchange::RetireChunksLocked() {
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

void PipelineBroadcastExchange::RetireBacklogLocked(ConsumerState &consumer) {
	while (!consumer.backlog.empty() && consumer.backlog_base_position < consumer.position) {
		consumer.backlog_bytes -= MinValue(consumer.backlog_bytes, consumer.backlog.front().bytes);
		consumer.backlog.pop_front();
		consumer.backlog_base_position++;
	}
}

void PipelineBroadcastExchange::WakeReadersLocked(vector<InterruptState> &readers) {
	readers.insert(readers.end(), blocked_readers.begin(), blocked_readers.end());
	blocked_readers.clear();
}

void PipelineBroadcastExchange::WakeWritersLocked(vector<InterruptState> &writers, bool force) {
	if (!force && buffered_bytes > low_watermark && !cancelled && !ShouldStopProducerLocked() && !producer_finished) {
		return;
	}
	writers.insert(writers.end(), blocked_writers.begin(), blocked_writers.end());
	blocked_writers.clear();
}

void PipelineBroadcastExchange::CallbackAll(vector<InterruptState> &interrupts) {
	for (auto &interrupt : interrupts) {
		interrupt.Callback();
	}
}

} // namespace duckdb
