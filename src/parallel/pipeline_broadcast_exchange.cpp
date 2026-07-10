#include "duckdb/parallel/pipeline_broadcast_exchange.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

static constexpr const idx_t PIPELINE_BROADCAST_HIGH_WATERMARK_CHUNKS = 32;
static constexpr const idx_t PIPELINE_BROADCAST_LOW_WATERMARK_CHUNKS = PIPELINE_BROADCAST_HIGH_WATERMARK_CHUNKS / 2;

static bool RequiresResetForReuse(const vector<LogicalType> &types) {
	for (auto &type : types) {
		if (!TypeIsConstantSize(type.InternalType())) {
			return true;
		}
	}
	return false;
}

struct PipelineBroadcastExchange::ChunkPool : public enable_shared_from_this<PipelineBroadcastExchange::ChunkPool> {
	enum class ResetMode : uint8_t { RESET, CLEAR_CARDINALITY };

	ChunkPool(vector<LogicalType> types_p, idx_t max_threads_p)
	    : types(std::move(types_p)),
	      reset_mode(RequiresResetForReuse(types) ? ResetMode::RESET : ResetMode::CLEAR_CARDINALITY),
	      max_cached_chunks(MaxValue<idx_t>(max_threads_p * 4, 16)) {
	}

	shared_ptr<DataChunk> Acquire(DataChunk &source) {
		unique_ptr<DataChunk> result;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			if (!cached_chunks.empty()) {
				result = std::move(cached_chunks.back());
				cached_chunks.pop_back();
			}
		}

		if (!result) {
			result = make_uniq<DataChunk>();
			result->Initialize(Allocator::DefaultAllocator(), types);
		} else if (reset_mode == ResetMode::RESET) {
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
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (cached_chunks.size() >= max_cached_chunks) {
			return;
		}
		cached_chunks.push_back(std::move(chunk));
	}

	vector<LogicalType> types;
	ResetMode reset_mode;
	idx_t max_cached_chunks;

	annotated_mutex lock;
	vector<unique_ptr<DataChunk>> cached_chunks;
};

struct PipelineBroadcastExchange::BroadcastSpoolReader {
	explicit BroadcastSpoolReader(BroadcastSpool &spool);

	bool Contains(idx_t row_idx) const {
		return scan_state.current_row_index <= row_idx && row_idx < scan_state.next_row_index;
	}

	ColumnDataScanState scan_state;
	DataChunk scan_chunk;
	idx_t observed_generation = 0;
};

struct PipelineBroadcastExchange::BroadcastSpool {
	struct ChunkEntry {
		idx_t row_offset;
		idx_t row_count;
	};

	BroadcastSpool(ClientContext &context, const vector<LogicalType> &types, idx_t base_position_p)
	    : collection(context, types, ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR), base_position(base_position_p),
	      next_position(base_position_p) {
		collection.InitializeAppend(append_state);
	}

	void Append(DataChunk &chunk) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		D_ASSERT(chunk.size() > 0);
		auto row_offset = collection.Count();
		collection.Append(append_state, chunk);
		chunks.push_back(ChunkEntry {row_offset, chunk.size()});
		next_position++;
		append_generation++;
	}

	void Read(idx_t position, DataChunk &result, BroadcastSpoolReader &reader) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (position < base_position || position >= next_position) {
			throw InternalException("Attempted to read retired pipeline broadcast spool chunk");
		}
		auto chunk_idx = position - base_position;
		D_ASSERT(chunk_idx < chunks.size());
		auto &entry = chunks[chunk_idx];
		result.Reset();

		auto row_idx = entry.row_offset;
		auto offset = SeekLocked(position, entry, row_idx, reader);
		auto available = reader.scan_chunk.size() - offset;
		if (available >= entry.row_count) {
			if (offset == 0 && entry.row_count == reader.scan_chunk.size()) {
				result.Reference(reader.scan_chunk);
			} else {
				result.Slice(reader.scan_chunk, offset, offset + entry.row_count);
			}
			return;
		}

		idx_t rows_read = 0;
		while (rows_read < entry.row_count) {
			row_idx = entry.row_offset + rows_read;
			offset = SeekLocked(position, entry, row_idx, reader);
			auto append_count = MinValue<idx_t>(reader.scan_chunk.size() - offset, entry.row_count - rows_read);
			auto sel = SelectionVector::Incremental(offset, append_count);
			result.Append(reader.scan_chunk, sel, append_count);
			rows_read += append_count;
		}
	}

	idx_t RetireBefore(idx_t position) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		idx_t retired_chunks = 0;
		while (!chunks.empty() && base_position < position) {
			chunks.pop_front();
			base_position++;
			retired_chunks++;
		}
		return retired_chunks;
	}

	bool HasPosition(idx_t position) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return position >= base_position && position < next_position;
	}

	void InitializeReader(BroadcastSpoolReader &reader) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		ResetReaderLocked(reader);
		collection.InitializeScanChunk(reader.scan_state, reader.scan_chunk);
	}

	void ResetReaderLocked(BroadcastSpoolReader &reader) {
		collection.InitializeScan(reader.scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);
		reader.observed_generation = append_generation;
	}

	idx_t SeekLocked(idx_t position, const ChunkEntry &entry, idx_t row_idx, BroadcastSpoolReader &reader) {
		if (reader.observed_generation != append_generation && !reader.Contains(row_idx)) {
			ResetReaderLocked(reader);
		}
		if (!collection.Seek(row_idx, reader.scan_state, reader.scan_chunk)) {
			throw InternalException(
			    "Failed to read pipeline broadcast spool chunk (position=%llu, base=%llu, next=%llu, row=%llu, "
			    "row_offset=%llu, row_count=%llu, current=%llu, next_row=%llu, generation=%llu, observed=%llu)",
			    position, base_position, next_position, row_idx, entry.row_offset, entry.row_count,
			    reader.scan_state.current_row_index, reader.scan_state.next_row_index, append_generation,
			    reader.observed_generation);
		}
		D_ASSERT(reader.scan_state.current_row_index <= row_idx);
		auto offset = row_idx - reader.scan_state.current_row_index;
		D_ASSERT(offset < reader.scan_chunk.size());
		return offset;
	}

	annotated_mutex lock;
	ColumnDataCollection collection;
	ColumnDataAppendState append_state;
	deque<ChunkEntry> chunks;
	idx_t base_position;
	idx_t next_position;
	idx_t append_generation = 0;
};

PipelineBroadcastExchange::BroadcastSpoolReader::BroadcastSpoolReader(BroadcastSpool &spool) {
	spool.InitializeReader(*this);
}

struct PipelineBroadcastExchange::AppendReservation {
	shared_ptr<BroadcastSpool> shared_spool;
	vector<shared_ptr<BroadcastSpool>> detached_spools;
	idx_t position = 0;
};

struct PipelineBroadcastExchange::SpoolReadReservation {
	shared_ptr<BroadcastSpool> spool;
	shared_ptr<BroadcastSpoolReader> reader;
	idx_t position = 0;
	ConsumerBufferMode buffer_mode = ConsumerBufferMode::SHARED;

	bool IsSet() const {
		return spool != nullptr;
	}
};

PipelineBroadcastExchange::ConsumerState::ConsumerState() = default;
PipelineBroadcastExchange::ConsumerState::~ConsumerState() = default;
PipelineBroadcastExchange::ConsumerState::ConsumerState(ConsumerState &&other) noexcept = default;
PipelineBroadcastExchange::ConsumerState &
PipelineBroadcastExchange::ConsumerState::operator=(ConsumerState &&other) noexcept = default;

PipelineBroadcastExchange::~PipelineBroadcastExchange() = default;

PipelineBroadcastExchangeLocalState::PipelineBroadcastExchangeLocalState(ClientContext &context,
                                                                         const PipelineBroadcastExchange &exchange) {
	vector<reference<Pipeline>> direct_pipeline_refs;
	{
		annotated_lock_guard<annotated_mutex> guard(exchange.lock);
		idx_t exchange_consumers = 0;
		for (auto &consumer : exchange.consumers) {
			if (consumer.mode != PipelineBroadcastExchange::ConsumerMode::MATERIALIZED) {
				exchange_consumers++;
			}
		}
		mode = exchange_consumers > 0 && exchange.active_consumers == 0 &&
		               exchange.direct_pipelines.size() == exchange_consumers
		           ? PipelineBroadcastExchangeLocalMode::DIRECT_ONLY
		           : PipelineBroadcastExchangeLocalMode::BUFFERED;
		direct_pipeline_refs = exchange.direct_pipelines;
	}

	for (auto &pipeline_ref : direct_pipeline_refs) {
		auto &pipeline = pipeline_ref.get();
		pipeline.PrepareExternalInput();
		direct_executors.push_back(make_uniq<PipelineExecutor>(context, pipeline));
	}
}

void PipelineBroadcastExchange::SetLogOperator(const PhysicalOperator &op) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	log_operator = &op;
}

PipelineBroadcastExchangeLocalState::~PipelineBroadcastExchangeLocalState() = default;

PipelineBroadcastExchange::PipelineBroadcastExchange(ClientContext &context, vector<LogicalType> types_p,
                                                     PipelineBroadcastExchangeCompletionMode completion_mode_p)
    : context(context), types(std::move(types_p)), completion_mode(completion_mode_p),
      max_threads(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads())) {
	chunk_pool = make_shared_ptr<ChunkPool>(types, max_threads);
}

unique_ptr<PipelineBroadcastExchangeLocalState> PipelineBroadcastExchange::GetLocalState(ClientContext &context) const {
	return make_uniq<PipelineBroadcastExchangeLocalState>(context, *this);
}

idx_t PipelineBroadcastExchange::RegisterConsumer() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	ConsumerState state;
	state.position = base_position;
	consumers.push_back(std::move(state));
	active_consumers++;
	return consumers.size() - 1;
}

bool PipelineBroadcastExchange::DisableConsumer(idx_t consumer_idx) {
	// Disabled consumers are served from materialized CTE storage instead of this exchange.
	annotated_lock_guard<annotated_mutex> guard(lock);
	D_ASSERT(consumer_idx < consumers.size());
	auto &consumer = consumers[consumer_idx];
	if (consumer.mode == ConsumerMode::MATERIALIZED) {
		return false;
	}
	D_ASSERT(consumer.mode == ConsumerMode::BUFFERED);
	consumer.mode = ConsumerMode::MATERIALIZED;
	DeactivateConsumerLocked(consumer, next_position);
	return true;
}

bool PipelineBroadcastExchange::TryRegisterDirectConsumer(Pipeline &pipeline, idx_t consumer_idx) {
	if (!pipeline.CanUseExternalInput()) {
		return false;
	}
	annotated_lock_guard<annotated_mutex> guard(lock);
	D_ASSERT(consumer_idx < consumers.size());
	auto &consumer = consumers[consumer_idx];
	D_ASSERT(consumer.mode != ConsumerMode::MATERIALIZED);
	if (consumer.mode == ConsumerMode::DIRECT) {
		return true;
	}
	consumer.mode = ConsumerMode::DIRECT;
	DeactivateConsumerLocked(consumer, base_position);
	direct_pipelines.push_back(pipeline);
	return true;
}

void PipelineBroadcastExchange::ResetConsumerRegistrations() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	ResetExchangeStateLocked();
	direct_pipelines.clear();
	blocked_readers.clear();
	blocked_writers.clear();
	blocked_appenders.clear();
	for (auto &consumer : consumers) {
		ResetConsumerRegistrationLocked(consumer);
	}
}

void PipelineBroadcastExchange::Reset() {
	vector<InterruptState> readers;
	vector<InterruptState> writers;
	vector<InterruptState> appenders;
	vector<ExchangeLogEntry> log_entries;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		ResetExchangeStateLocked();
		for (auto &consumer : consumers) {
			ResetConsumerExecutionLocked(consumer);
		}
		WakeReadersLocked(readers);
		WakeWritersLocked(writers, log_entries, WriterWakeMode::FORCE);
		WakeAppendersLocked(appenders);
	}
	CallbackAll(readers);
	CallbackAll(writers);
	CallbackAll(appenders);
	LogTransitions(log_entries);
}

void PipelineBroadcastExchange::ResetExchangeStateLocked() {
	chunks.clear();
	shared_spool.reset();
	base_position = 0;
	next_position = 0;
	shared_buffered_chunks = 0;
	produced_rows.store(0, std::memory_order_relaxed);
	direct_consumer_progress.store(false, std::memory_order_relaxed);
	producer_state = ProducerState::ACTIVE;
	watermark_state = WatermarkState::BELOW_HIGH_WATERMARK;
	append_reservation_state = AppendReservationState::IDLE;
	active_consumers = 0;
}

void PipelineBroadcastExchange::ResetConsumerReadStateLocked(ConsumerState &consumer, idx_t position) {
	consumer.position = position;
	consumer.buffer_mode = ConsumerBufferMode::SHARED;
	consumer.read_state = ConsumerReadState::IDLE;
	consumer.read_position = position;
	consumer.shared_reader.reset();
	ClearDetachedBufferLocked(consumer);
}

void PipelineBroadcastExchange::ResetConsumerRegistrationLocked(ConsumerState &consumer) {
	consumer.rows_read = 0;
	consumer.lifecycle = ConsumerLifecycle::ACTIVE;
	consumer.mode = ConsumerMode::BUFFERED;
	ResetConsumerReadStateLocked(consumer, base_position);
	active_consumers++;
}

void PipelineBroadcastExchange::ResetConsumerExecutionLocked(ConsumerState &consumer) {
	consumer.rows_read = 0;
	consumer.lifecycle =
	    consumer.mode == ConsumerMode::BUFFERED ? ConsumerLifecycle::ACTIVE : ConsumerLifecycle::INACTIVE;
	ResetConsumerReadStateLocked(consumer, base_position);
	if (consumer.lifecycle == ConsumerLifecycle::ACTIVE) {
		active_consumers++;
	}
}

void PipelineBroadcastExchange::DeactivateConsumerLocked(ConsumerState &consumer, idx_t position) {
	if (consumer.lifecycle == ConsumerLifecycle::ACTIVE) {
		D_ASSERT(active_consumers > 0);
		active_consumers--;
	}
	consumer.lifecycle = ConsumerLifecycle::INACTIVE;
	ResetConsumerReadStateLocked(consumer, position);
}

SinkResultType PipelineBroadcastExchange::Push(DataChunk &chunk, PipelineBroadcastExchangeLocalState &lstate,
                                               const InterruptState &interrupt_state) {
	if (!lstate.direct_executors.empty() &&
	    (lstate.direct_push_state == PipelineBroadcastExchangeDirectPushState::NOT_STARTED ||
	     lstate.direct_push_state == PipelineBroadcastExchangeDirectPushState::RESUMING)) {
		auto direct_result = PushDirectConsumers(chunk, lstate, interrupt_state);
		if (direct_result == SinkResultType::BLOCKED) {
			return SinkResultType::BLOCKED;
		}
		if (lstate.mode == PipelineBroadcastExchangeLocalMode::BUFFERED) {
			RecordDirectConsumerProgress();
		}
	}

	if (lstate.mode == PipelineBroadcastExchangeLocalMode::BUFFERED && HasBufferedConsumers()) {
		auto append_result = Append(chunk, interrupt_state);
		if (append_result == SinkResultType::BLOCKED) {
			return SinkResultType::BLOCKED;
		}
		return CompletePush(chunk, lstate,
		                    append_result == SinkResultType::FINISHED ? BufferedPushState::FINISHED
		                                                              : BufferedPushState::APPENDED);
	}
	return CompletePush(chunk, lstate, BufferedPushState::NONE);
}

SinkResultType PipelineBroadcastExchange::CompletePush(DataChunk &chunk, PipelineBroadcastExchangeLocalState &lstate,
                                                       BufferedPushState buffered_state) {
	if (buffered_state != BufferedPushState::APPENDED) {
		RecordProducedRows(chunk.size());
	}
	const auto direct_consumers_finished =
	    lstate.direct_executors.empty() ||
	    lstate.direct_push_state == PipelineBroadcastExchangeDirectPushState::FINISHED;
	ResetPushChunk(lstate);
	if (!RunToCompletion() && (buffered_state == BufferedPushState::FINISHED ||
	                           (buffered_state == BufferedPushState::NONE && direct_consumers_finished))) {
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
                                                              const InterruptState &interrupt_state) {
	if (lstate.direct_push_state != PipelineBroadcastExchangeDirectPushState::RESUMING) {
		lstate.direct_idx = 0;
	}
	for (; lstate.direct_idx < lstate.direct_executors.size(); lstate.direct_idx++) {
		auto &executor = *lstate.direct_executors[lstate.direct_idx];
		executor.SetInterruptState(interrupt_state);
		if (executor.IsFinishedProcessing()) {
			continue;
		}
		auto result = executor.PushExternal(chunk);
		if (result == PipelineExecuteResult::INTERRUPTED) {
			lstate.direct_push_state = PipelineBroadcastExchangeDirectPushState::RESUMING;
			return SinkResultType::BLOCKED;
		}
	}

	lstate.direct_push_state = PipelineBroadcastExchangeDirectPushState::FINISHED;
	for (auto &executor : lstate.direct_executors) {
		if (!executor->IsFinishedProcessing()) {
			lstate.direct_push_state = PipelineBroadcastExchangeDirectPushState::ACTIVE;
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
	lstate.direct_push_state = PipelineBroadcastExchangeDirectPushState::NOT_STARTED;
}

PipelineBroadcastExchange::AppendAdmission
PipelineBroadcastExchange::PrepareAppendLocked(const InterruptState &interrupt_state,
                                               vector<ExchangeLogEntry> &log_entries) {
	if (producer_state == ProducerState::CANCELLED || ShouldStopProducerLocked()) {
		return AppendAdmission::FINISHED;
	}
	if (append_reservation_state == AppendReservationState::RESERVED) {
		blocked_appenders.push_back(interrupt_state);
		return AppendAdmission::BLOCKED;
	}
	if (ShouldCreateSharedSpoolLocked()) {
		CreateSharedSpoolLocked(log_entries);
	}
	DetachLaggingConsumersLocked(log_entries);
	if (ShouldThrottleProducerLocked()) {
		if (watermark_state == WatermarkState::BELOW_HIGH_WATERMARK) {
			watermark_state = WatermarkState::ABOVE_HIGH_WATERMARK;
			log_entries.push_back({ExchangeLogEvent::HIGH_WATERMARK_BLOCKED, active_consumers, shared_buffered_chunks});
		}
		blocked_writers.push_back(interrupt_state);
		return AppendAdmission::BLOCKED;
	}
	return AppendAdmission::READY;
}

PipelineBroadcastExchange::AppendAdmission
PipelineBroadcastExchange::ReserveAppendLocked(const InterruptState &interrupt_state, AppendReservation &reservation,
                                               vector<ExchangeLogEntry> &log_entries) {
	auto admission = PrepareAppendLocked(interrupt_state, log_entries);
	if (admission != AppendAdmission::READY) {
		return admission;
	}
	append_reservation_state = AppendReservationState::RESERVED;
	reservation.shared_spool = shared_spool;
	reservation.position = next_position;
	for (auto &consumer : consumers) {
		if (consumer.lifecycle != ConsumerLifecycle::ACTIVE || consumer.buffer_mode != ConsumerBufferMode::DETACHED) {
			continue;
		}
		D_ASSERT(consumer.detached_spool);
		reservation.detached_spools.push_back(consumer.detached_spool);
	}
	return AppendAdmission::READY;
}

void PipelineBroadcastExchange::CompleteAppendLocked(const AppendReservation &reservation, shared_ptr<DataChunk> copy,
                                                     idx_t row_count, vector<InterruptState> &readers,
                                                     vector<InterruptState> &appenders,
                                                     vector<ExchangeLogEntry> &log_entries) {
	D_ASSERT(append_reservation_state == AppendReservationState::RESERVED);
	append_reservation_state = AppendReservationState::IDLE;
	if (reservation.shared_spool) {
		shared_buffered_chunks++;
	} else if (HasActiveSharedConsumersLocked()) {
		chunks.push_back({std::move(copy)});
		shared_buffered_chunks++;
	}
	if (active_consumers > 0 || reservation.shared_spool) {
		D_ASSERT(next_position == reservation.position);
		next_position++;
		WakeReadersLocked(readers);
		if (active_consumers == 0) {
			RetireChunksLocked();
		}
	}
	RecordProducedRows(row_count);
	WakeAppendersLocked(appenders);
}

void PipelineBroadcastExchange::AbortAppendReservation(vector<InterruptState> &readers, vector<InterruptState> &writers,
                                                       vector<InterruptState> &appenders) {
	D_ASSERT(append_reservation_state == AppendReservationState::RESERVED);
	append_reservation_state = AppendReservationState::IDLE;
	producer_state = ProducerState::CANCELLED;
	WakeReadersLocked(readers);
	vector<ExchangeLogEntry> ignored_log_entries;
	WakeWritersLocked(writers, ignored_log_entries, WriterWakeMode::FORCE);
	WakeAppendersLocked(appenders);
}

SinkResultType PipelineBroadcastExchange::Append(DataChunk &chunk, const InterruptState &interrupt_state) {
	vector<ExchangeLogEntry> log_entries;
	AppendAdmission admission;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		admission = PrepareAppendLocked(interrupt_state, log_entries);
	}
	LogTransitions(log_entries);
	if (admission == AppendAdmission::BLOCKED) {
		return SinkResultType::BLOCKED;
	}
	if (admission == AppendAdmission::FINISHED) {
		return SinkResultType::FINISHED;
	}

	auto copy = CopyChunk(chunk);
	AppendReservation reservation;
	log_entries.clear();
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		admission = ReserveAppendLocked(interrupt_state, reservation, log_entries);
	}
	LogTransitions(log_entries);
	if (admission == AppendAdmission::BLOCKED) {
		return SinkResultType::BLOCKED;
	}
	if (admission == AppendAdmission::FINISHED) {
		return SinkResultType::FINISHED;
	}

	try {
		if (reservation.shared_spool) {
			reservation.shared_spool->Append(*copy);
		}
		for (auto &spool : reservation.detached_spools) {
			spool->Append(*copy);
		}
	} catch (...) {
		vector<InterruptState> readers;
		vector<InterruptState> writers;
		vector<InterruptState> appenders;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			AbortAppendReservation(readers, writers, appenders);
		}
		CallbackAll(readers);
		CallbackAll(writers);
		CallbackAll(appenders);
		throw;
	}

	vector<InterruptState> readers;
	vector<InterruptState> appenders;
	log_entries.clear();
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		CompleteAppendLocked(reservation, std::move(copy), chunk.size(), readers, appenders, log_entries);
	}
	CallbackAll(readers);
	CallbackAll(appenders);
	LogTransitions(log_entries);
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
	vector<ExchangeLogEntry> log_entries;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		producer_state = ProducerState::FINISHED;
		WakeReadersLocked(readers);
		WakeWritersLocked(writers, log_entries);
	}
	CallbackAll(readers);
	CallbackAll(writers);
	LogTransitions(log_entries);
}

void PipelineBroadcastExchange::FinishDirectConsumers() {
	for (auto &pipeline_ref : direct_pipelines) {
		pipeline_ref.get().CompleteExternalInput();
	}
}

void PipelineBroadcastExchange::Cancel() {
	vector<InterruptState> readers;
	vector<InterruptState> writers;
	vector<InterruptState> appenders;
	vector<ExchangeLogEntry> log_entries;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		producer_state = ProducerState::CANCELLED;
		WakeReadersLocked(readers);
		WakeWritersLocked(writers, log_entries, WriterWakeMode::FORCE);
		WakeAppendersLocked(appenders);
	}
	CallbackAll(readers);
	CallbackAll(writers);
	CallbackAll(appenders);
	LogTransitions(log_entries);
}

SourceResultType PipelineBroadcastExchange::Scan(idx_t consumer_idx, DataChunk &chunk,
                                                 shared_ptr<DataChunk> &current_chunk,
                                                 const InterruptState &interrupt_state) {
	vector<InterruptState> writers;
	vector<InterruptState> readers;
	vector<ExchangeLogEntry> log_entries;
	shared_ptr<DataChunk> next_chunk;
	SpoolReadReservation spool_read;
	SourceResultType result;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		result = ReserveScanLocked(consumer_idx, interrupt_state, next_chunk, spool_read, writers, log_entries);
	}

	if (spool_read.IsSet()) {
		try {
			spool_read.spool->Read(spool_read.position, chunk, *spool_read.reader);
		} catch (...) {
			vector<InterruptState> failed_readers;
			vector<InterruptState> failed_writers;
			vector<InterruptState> appenders;
			{
				annotated_lock_guard<annotated_mutex> guard(lock);
				auto &consumer = consumers[consumer_idx];
				consumer.read_state = ConsumerReadState::IDLE;
				producer_state = ProducerState::CANCELLED;
				WakeReadersLocked(failed_readers);
				vector<ExchangeLogEntry> ignored_log_entries;
				WakeWritersLocked(failed_writers, ignored_log_entries, WriterWakeMode::FORCE);
				WakeAppendersLocked(appenders);
			}
			CallbackAll(failed_readers);
			CallbackAll(failed_writers);
			CallbackAll(appenders);
			throw;
		}
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			CompleteSpoolReadLocked(consumer_idx, spool_read, chunk, readers, writers, log_entries);
		}
	}

	CallbackAll(readers);
	CallbackAll(writers);
	LogTransitions(log_entries);
	if (next_chunk) {
		current_chunk = std::move(next_chunk);
		chunk.Reference(*current_chunk);
	}
	return result;
}

SourceResultType PipelineBroadcastExchange::ReserveScanLocked(idx_t consumer_idx, const InterruptState &interrupt_state,
                                                              shared_ptr<DataChunk> &next_chunk,
                                                              SpoolReadReservation &spool_read,
                                                              vector<InterruptState> &writers,
                                                              vector<ExchangeLogEntry> &log_entries) {
	D_ASSERT(consumer_idx < consumers.size());
	auto &consumer = consumers[consumer_idx];
	if (consumer.lifecycle != ConsumerLifecycle::ACTIVE || producer_state == ProducerState::CANCELLED) {
		return SourceResultType::FINISHED;
	}
	if (consumer.read_state == ConsumerReadState::READING) {
		blocked_readers.push_back(interrupt_state);
		return SourceResultType::BLOCKED;
	}
	if (consumer.buffer_mode == ConsumerBufferMode::DETACHED && consumer.detached_spool &&
	    consumer.detached_spool->HasPosition(consumer.position)) {
		if (!consumer.detached_reader) {
			consumer.detached_reader = make_shared_ptr<BroadcastSpoolReader>(*consumer.detached_spool);
		}
		consumer.read_state = ConsumerReadState::READING;
		consumer.read_position = consumer.position;
		spool_read.spool = consumer.detached_spool;
		spool_read.reader = consumer.detached_reader;
		spool_read.position = consumer.position;
		spool_read.buffer_mode = ConsumerBufferMode::DETACHED;
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	if (consumer.buffer_mode == ConsumerBufferMode::SHARED && consumer.position < next_position) {
		if (shared_spool) {
			if (!shared_spool->HasPosition(consumer.position)) {
				throw InternalException("Pipeline broadcast shared spool chunk was retired before it was read");
			}
			if (!consumer.shared_reader) {
				consumer.shared_reader = make_shared_ptr<BroadcastSpoolReader>(*shared_spool);
			}
			consumer.read_state = ConsumerReadState::READING;
			consumer.read_position = consumer.position;
			spool_read.spool = shared_spool;
			spool_read.reader = consumer.shared_reader;
			spool_read.position = consumer.position;
			spool_read.buffer_mode = ConsumerBufferMode::SHARED;
			return SourceResultType::HAVE_MORE_OUTPUT;
		}
		D_ASSERT(consumer.position >= base_position);
		auto chunk_idx = consumer.position - base_position;
		D_ASSERT(chunk_idx < chunks.size());
		next_chunk = chunks[chunk_idx].chunk;
		consumer.position++;
		consumer.rows_read += next_chunk->size();
		RetireChunksLocked();
		WakeWritersLocked(writers, log_entries);
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	if (producer_state == ProducerState::FINISHED) {
		consumer.lifecycle = ConsumerLifecycle::INACTIVE;
		D_ASSERT(active_consumers > 0);
		active_consumers--;
		RetireChunksLocked();
		ClearDetachedBufferLocked(consumer);
		WakeWritersLocked(writers, log_entries, WriterWakeMode::FORCE);
		return SourceResultType::FINISHED;
	}
	WakeWritersLocked(writers, log_entries, WriterWakeMode::FORCE);
	blocked_readers.push_back(interrupt_state);
	return SourceResultType::BLOCKED;
}

void PipelineBroadcastExchange::CompleteSpoolReadLocked(idx_t consumer_idx, const SpoolReadReservation &spool_read,
                                                        DataChunk &chunk, vector<InterruptState> &readers,
                                                        vector<InterruptState> &writers,
                                                        vector<ExchangeLogEntry> &log_entries) {
	D_ASSERT(consumer_idx < consumers.size());
	auto &consumer = consumers[consumer_idx];
	D_ASSERT(consumer.read_state == ConsumerReadState::READING);
	D_ASSERT(consumer.read_position == spool_read.position);
	consumer.read_state = ConsumerReadState::IDLE;
	if (consumer.lifecycle != ConsumerLifecycle::ACTIVE || producer_state == ProducerState::CANCELLED) {
		chunk.Reset();
		consumer.shared_reader.reset();
		RetireChunksLocked();
		ClearDetachedBufferLocked(consumer);
		consumer.buffer_mode = ConsumerBufferMode::SHARED;
		WakeWritersLocked(writers, log_entries, WriterWakeMode::FORCE);
		WakeReadersLocked(readers);
		return;
	}
	D_ASSERT(consumer.position == spool_read.position);
	consumer.position++;
	consumer.rows_read += chunk.size();
	if (spool_read.buffer_mode == ConsumerBufferMode::DETACHED) {
		RetireDetachedBufferLocked(consumer);
	} else {
		RetireChunksLocked();
	}
	WakeWritersLocked(writers, log_entries);
	WakeReadersLocked(readers);
}

void PipelineBroadcastExchange::UnregisterConsumer(idx_t consumer_idx) {
	vector<InterruptState> readers;
	vector<InterruptState> writers;
	vector<ExchangeLogEntry> log_entries;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (consumer_idx >= consumers.size()) {
			return;
		}
		auto &consumer = consumers[consumer_idx];
		if (consumer.lifecycle != ConsumerLifecycle::ACTIVE) {
			return;
		}
		consumer.lifecycle = ConsumerLifecycle::INACTIVE;
		consumer.position = next_position;
		if (consumer.read_state != ConsumerReadState::READING) {
			consumer.buffer_mode = ConsumerBufferMode::SHARED;
			consumer.shared_reader.reset();
			ClearDetachedBufferLocked(consumer);
		}
		D_ASSERT(active_consumers > 0);
		active_consumers--;
		RetireChunksLocked();
		WakeReadersLocked(readers);
		WakeWritersLocked(writers, log_entries, WriterWakeMode::FORCE);
		log_entries.push_back({ExchangeLogEvent::CONSUMER_UNREGISTERED, active_consumers, shared_buffered_chunks});
	}
	CallbackAll(readers);
	CallbackAll(writers);
	LogTransitions(log_entries);
}

ProgressData PipelineBroadcastExchange::ScanProgress(idx_t consumer_idx, idx_t estimated_cardinality) const {
	annotated_lock_guard<annotated_mutex> guard(lock);
	ProgressData progress;
	if (consumer_idx >= consumers.size()) {
		progress.SetInvalid();
		return progress;
	}
	auto total = produced_rows.load(std::memory_order_relaxed);
	if (producer_state == ProducerState::ACTIVE) {
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
	annotated_lock_guard<annotated_mutex> guard(lock);
	ProgressData progress;
	auto produced_count = produced_rows.load(std::memory_order_relaxed);
	auto produced = double(produced_count);
	if (producer_state != ProducerState::ACTIVE) {
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

idx_t PipelineBroadcastExchange::RegisteredConsumerCount() const {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return consumers.size();
}

idx_t PipelineBroadcastExchange::ConsumerCount() const {
	annotated_lock_guard<annotated_mutex> guard(lock);
	idx_t count = 0;
	for (auto &consumer : consumers) {
		if (consumer.mode != ConsumerMode::MATERIALIZED) {
			count++;
		}
	}
	return count;
}

idx_t PipelineBroadcastExchange::DirectConsumerCount() const {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return direct_pipelines.size();
}

bool PipelineBroadcastExchange::HasBufferedConsumers() const {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return active_consumers > 0;
}

shared_ptr<DataChunk> PipelineBroadcastExchange::CopyChunk(DataChunk &chunk) {
	return chunk_pool->Acquire(chunk);
}

bool PipelineBroadcastExchange::ShouldStopProducerLocked() const {
	return completion_mode == PipelineBroadcastExchangeCompletionMode::STOP_WHEN_UNCONSUMED && active_consumers == 0;
}

bool PipelineBroadcastExchange::ShouldThrottleProducerLocked() const {
	if (shared_spool) {
		return false;
	}
	return active_consumers > 0 && shared_buffered_chunks >= PIPELINE_BROADCAST_HIGH_WATERMARK_CHUNKS;
}

bool PipelineBroadcastExchange::HasActiveSharedConsumersLocked() const {
	for (auto &consumer : consumers) {
		if (consumer.lifecycle == ConsumerLifecycle::ACTIVE && consumer.buffer_mode == ConsumerBufferMode::SHARED) {
			return true;
		}
	}
	return false;
}

bool PipelineBroadcastExchange::ShouldCreateSharedSpoolLocked() const {
	return !shared_spool && direct_pipelines.empty() && consumers.size() > 1 &&
	       shared_buffered_chunks >= PIPELINE_BROADCAST_HIGH_WATERMARK_CHUNKS;
}

void PipelineBroadcastExchange::CreateSharedSpoolLocked(vector<ExchangeLogEntry> &log_entries) {
	D_ASSERT(!shared_spool);
	shared_spool = make_shared_ptr<BroadcastSpool>(context, types, base_position);
	for (auto &chunk : chunks) {
		shared_spool->Append(*chunk.chunk);
	}
	chunks.clear();
	log_entries.push_back({ExchangeLogEvent::SPOOL_CREATED, active_consumers, shared_buffered_chunks});
}

void PipelineBroadcastExchange::DetachLaggingConsumersLocked(vector<ExchangeLogEntry> &log_entries) {
	if (shared_spool) {
		return;
	}
	if (shared_buffered_chunks < PIPELINE_BROADCAST_HIGH_WATERMARK_CHUNKS || chunks.empty()) {
		return;
	}

	bool has_progressed_consumer = direct_consumer_progress.load(std::memory_order_relaxed);
	if (!has_progressed_consumer) {
		return;
	}
	bool has_lagging_consumer = false;
	for (auto &consumer : consumers) {
		if (consumer.lifecycle != ConsumerLifecycle::ACTIVE || consumer.buffer_mode == ConsumerBufferMode::DETACHED) {
			continue;
		}
		if (consumer.position == base_position) {
			has_lagging_consumer = true;
		}
	}
	if (!has_lagging_consumer) {
		return;
	}

	for (auto &consumer : consumers) {
		if (consumer.lifecycle != ConsumerLifecycle::ACTIVE || consumer.buffer_mode == ConsumerBufferMode::DETACHED ||
		    consumer.position != base_position) {
			continue;
		}
		DetachConsumerLocked(consumer);
	}
	RetireChunksLocked();
	log_entries.push_back({ExchangeLogEvent::CONSUMERS_DETACHED, active_consumers, shared_buffered_chunks});
}

void PipelineBroadcastExchange::DetachConsumerLocked(ConsumerState &consumer) {
	D_ASSERT(consumer.buffer_mode == ConsumerBufferMode::SHARED);
	D_ASSERT(consumer.position >= base_position);
	consumer.buffer_mode = ConsumerBufferMode::DETACHED;
	consumer.detached_spool = make_shared_ptr<BroadcastSpool>(context, types, consumer.position);

	auto start_offset = consumer.position - base_position;
	for (idx_t chunk_idx = start_offset; chunk_idx < chunks.size(); chunk_idx++) {
		consumer.detached_spool->Append(*chunks[chunk_idx].chunk);
	}
}

void PipelineBroadcastExchange::RetireChunksLocked() {
	if (shared_spool) {
		idx_t min_position = next_position;
		bool found_reader = false;
		for (auto &consumer : consumers) {
			if (consumer.read_state == ConsumerReadState::READING &&
			    consumer.buffer_mode == ConsumerBufferMode::SHARED) {
				found_reader = true;
				min_position = MinValue(min_position, consumer.read_position);
			}
			if (consumer.lifecycle == ConsumerLifecycle::ACTIVE && consumer.buffer_mode == ConsumerBufferMode::SHARED) {
				found_reader = true;
				min_position = MinValue(min_position, consumer.position);
			}
		}
		if (!found_reader) {
			min_position = next_position;
		}
		auto retired_chunks = shared_spool->RetireBefore(min_position);
		shared_buffered_chunks -= MinValue(shared_buffered_chunks, retired_chunks);
		base_position = MaxValue(base_position, min_position);
		return;
	}
	if (chunks.empty()) {
		return;
	}
	idx_t min_position = next_position;
	bool found_active = false;
	for (auto &consumer : consumers) {
		if (consumer.lifecycle != ConsumerLifecycle::ACTIVE || consumer.buffer_mode == ConsumerBufferMode::DETACHED) {
			continue;
		}
		found_active = true;
		min_position = MinValue(min_position, consumer.position);
	}
	if (!found_active) {
		min_position = next_position;
	}
	while (!chunks.empty() && base_position < min_position) {
		shared_buffered_chunks -= MinValue<idx_t>(shared_buffered_chunks, 1);
		chunks.pop_front();
		base_position++;
	}
}

void PipelineBroadcastExchange::RetireDetachedBufferLocked(ConsumerState &consumer) {
	if (!consumer.detached_spool || consumer.read_state == ConsumerReadState::READING) {
		return;
	}
	consumer.detached_spool->RetireBefore(consumer.position);
}

void PipelineBroadcastExchange::ClearDetachedBufferLocked(ConsumerState &consumer) {
	if (consumer.read_state == ConsumerReadState::READING) {
		return;
	}
	consumer.detached_reader.reset();
	if (!consumer.detached_spool) {
		return;
	}
	consumer.detached_spool.reset();
}

void PipelineBroadcastExchange::WakeReadersLocked(vector<InterruptState> &readers) {
	readers.insert(readers.end(), blocked_readers.begin(), blocked_readers.end());
	blocked_readers.clear();
}

void PipelineBroadcastExchange::WakeWritersLocked(vector<InterruptState> &writers,
                                                  vector<ExchangeLogEntry> &log_entries, WriterWakeMode mode) {
	if (mode == WriterWakeMode::LOW_WATERMARK && shared_buffered_chunks > PIPELINE_BROADCAST_LOW_WATERMARK_CHUNKS &&
	    producer_state == ProducerState::ACTIVE && !ShouldStopProducerLocked()) {
		return;
	}
	if (mode == WriterWakeMode::LOW_WATERMARK && watermark_state == WatermarkState::ABOVE_HIGH_WATERMARK) {
		watermark_state = WatermarkState::BELOW_HIGH_WATERMARK;
		log_entries.push_back({ExchangeLogEvent::LOW_WATERMARK_WAKE, active_consumers, shared_buffered_chunks});
	}
	writers.insert(writers.end(), blocked_writers.begin(), blocked_writers.end());
	blocked_writers.clear();
}

void PipelineBroadcastExchange::WakeAppendersLocked(vector<InterruptState> &appenders) {
	appenders.insert(appenders.end(), blocked_appenders.begin(), blocked_appenders.end());
	blocked_appenders.clear();
}

void PipelineBroadcastExchange::LogTransitions(const vector<ExchangeLogEntry> &log_entries) const {
	if (!log_operator) {
		return;
	}
	for (auto &entry : log_entries) {
		const char *event;
		switch (entry.event) {
		case ExchangeLogEvent::SPOOL_CREATED:
			event = "SpoolCreated";
			break;
		case ExchangeLogEvent::CONSUMERS_DETACHED:
			event = "ConsumersDetached";
			break;
		case ExchangeLogEvent::HIGH_WATERMARK_BLOCKED:
			event = "HighWatermarkBlocked";
			break;
		case ExchangeLogEvent::LOW_WATERMARK_WAKE:
			event = "LowWatermarkWake";
			break;
		case ExchangeLogEvent::CONSUMER_UNREGISTERED:
			event = "ConsumerUnregistered";
			break;
		default:
			throw InternalException("Unknown pipeline broadcast exchange log event");
		}
		DUCKDB_LOG(context, PhysicalOperatorLogType, *log_operator, "PipelineBroadcastExchange", event,
		           {{"active_consumers", to_string(entry.consumer_count)},
		            {"buffered_chunks", to_string(entry.buffered_chunks)}});
	}
}

void PipelineBroadcastExchange::CallbackAll(vector<InterruptState> &interrupts) {
	for (auto &interrupt : interrupts) {
		interrupt.Callback();
	}
}

} // namespace duckdb
