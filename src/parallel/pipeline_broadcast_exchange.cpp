#include "duckdb/parallel/pipeline_broadcast_exchange.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"

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

	ChunkPool(Allocator &allocator_p, vector<LogicalType> types_p, idx_t max_threads_p)
	    : allocator(allocator_p), types(std::move(types_p)),
	      reset_mode(RequiresResetForReuse(types) ? ResetMode::RESET : ResetMode::CLEAR_CARDINALITY),
	      max_cached_chunks(MaxValue<idx_t>(max_threads_p * 4, 16)) {
	}

	shared_ptr<DataChunk> Acquire(DataChunk &source) {
		unique_ptr<DataChunk> result;
		idx_t acquire_generation;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			acquire_generation = generation;
			if (caching_enabled && !cached_chunks.empty()) {
				result = std::move(cached_chunks.back());
				cached_chunks.pop_back();
			}
		}

		if (!result) {
			result = make_uniq<DataChunk>();
			result->Initialize(allocator, types);
		} else if (reset_mode == ResetMode::RESET) {
			result->Reset();
		} else {
			result->SetChildCardinality(0);
		}

		source.Copy(*result);

		auto self = shared_from_this();
		return shared_ptr<DataChunk>(result.release(), [self, acquire_generation](DataChunk *chunk) {
			unique_ptr<DataChunk> owned(chunk);
			self->Release(std::move(owned), acquire_generation);
		});
	}

	void BeginExecution() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		cached_chunks.clear();
		generation++;
		caching_enabled = true;
	}

	void EndExecution() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		caching_enabled = false;
		cached_chunks.clear();
	}

	void Release(unique_ptr<DataChunk> chunk, idx_t release_generation) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (!caching_enabled || release_generation != generation || cached_chunks.size() >= max_cached_chunks) {
			return;
		}
		cached_chunks.push_back(std::move(chunk));
	}

	Allocator &allocator;
	vector<LogicalType> types;
	ResetMode reset_mode;
	idx_t max_cached_chunks;
	idx_t generation = 0;
	bool caching_enabled = false;

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
		idx_t batch_index;
	};

	BroadcastSpool(ClientContext &context, const vector<LogicalType> &types, idx_t base_position_p)
	    : collection(context, types, ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR), base_position(base_position_p),
	      next_position(base_position_p) {
		collection.InitializeAppend(append_state);
	}

	void Append(DataChunk &chunk, idx_t batch_index) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		D_ASSERT(chunk.size() > 0);
		auto row_offset = collection.Count();
		collection.Append(append_state, chunk);
		chunks.push_back(ChunkEntry {row_offset, chunk.size(), batch_index});
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

	idx_t BatchIndex(idx_t position) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (position < base_position || position >= next_position) {
			throw InternalException("Attempted to inspect retired pipeline broadcast spool chunk");
		}
		return chunks[position - base_position].batch_index;
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
	idx_t position = 0;
	idx_t producer_batch_index = DConstants::INVALID_INDEX;
	idx_t exchange_batch_index = DConstants::INVALID_INDEX;
	bool pending = false;
};

struct PipelineBroadcastExchange::SpoolReadReservation {
	shared_ptr<BroadcastSpool> spool;
	shared_ptr<BroadcastSpoolReader> reader;
	idx_t position = 0;

	bool IsSet() const {
		return spool != nullptr;
	}
};

struct PipelineBroadcastExchange::BufferedChunk {
	shared_ptr<DataChunk> chunk;
	idx_t batch_index;
};

class PipelineBroadcastExchangeScanState {
	friend class PipelineBroadcastExchange;

public:
	PipelineBroadcastExchangeScanState() = default;

private:
	shared_ptr<DataChunk> current_chunk;
	shared_ptr<PipelineBroadcastExchange::BroadcastSpoolReader> spool_reader;
};

struct PipelineBroadcastExchange::BufferState {
	BufferState(ClientContext &context_p, const vector<LogicalType> &types_p, idx_t max_threads)
	    : context(context_p), types(types_p),
	      chunk_pool(make_shared_ptr<ChunkPool>(BufferAllocator::Get(context), types, max_threads)) {
	}

	void BeginExecution() {
		chunk_pool->BeginExecution();
	}

	void EndExecution() {
		chunk_pool->EndExecution();
	}

	void Reset() {
		chunks.clear();
		pending_batches.clear();
		active_batches.clear();
		shared_spool.reset();
		base_position = 0;
		next_position = 0;
		buffered_chunks = 0;
		pending_chunks = 0;
		min_batch_index = 0;
		producer_pipeline_index = DConstants::INVALID_INDEX;
		producer_pipeline_offset = 0;
		max_local_batch_index = 0;
	}

	shared_ptr<DataChunk> Copy(DataChunk &chunk) {
		return chunk_pool->Acquire(chunk);
	}

	idx_t BasePosition() const {
		return base_position;
	}

	idx_t NextPosition() const {
		return next_position;
	}

	idx_t BufferedCount() const {
		return buffered_chunks;
	}

	idx_t PendingCount() const {
		return pending_chunks;
	}

	idx_t MinBatchIndex() const {
		return min_batch_index;
	}

	void UpdateMinBatchIndex(idx_t new_min_batch_index) {
		min_batch_index = MaxValue(min_batch_index, new_min_batch_index);
	}

	bool HasReadyBatch() const {
		return !pending_batches.empty() && pending_batches.begin()->first <= min_batch_index &&
		       (active_batches.empty() || pending_batches.begin()->first <= *active_batches.begin());
	}

	void RegisterActiveBatch(idx_t batch_index) {
		active_batches.insert(batch_index);
	}

	void UnregisterActiveBatch(idx_t batch_index) {
		auto entry = active_batches.find(batch_index);
		D_ASSERT(entry != active_batches.end());
		active_batches.erase(entry);
	}

	bool HasEarlierActiveBatch(idx_t batch_index) const {
		return !active_batches.empty() && *active_batches.begin() < batch_index;
	}

	bool HasSharedSpool() const {
		return shared_spool != nullptr;
	}

	bool Empty() const {
		return chunks.empty() && !shared_spool;
	}

	idx_t ExchangeBatchIndex(idx_t producer_batch_index) {
		if (producer_batch_index == DConstants::INVALID_INDEX) {
			return DConstants::INVALID_INDEX;
		}
		auto pipeline_index = producer_batch_index / PipelineBuildState::BATCH_INCREMENT;
		auto local_batch_index = producer_batch_index % PipelineBuildState::BATCH_INCREMENT;
		if (local_batch_index == 0) {
			throw InternalException("Pipeline broadcast exchange received an uninitialized batch index");
		}
		if (producer_pipeline_index == DConstants::INVALID_INDEX) {
			producer_pipeline_index = pipeline_index;
		} else if (pipeline_index != producer_pipeline_index) {
			if (pipeline_index < producer_pipeline_index) {
				throw InternalException("Pipeline broadcast producer index decreased from %llu to %llu",
				                        producer_pipeline_index, pipeline_index);
			}
			producer_pipeline_offset += max_local_batch_index;
			producer_pipeline_index = pipeline_index;
			max_local_batch_index = 0;
		}
		if (local_batch_index < max_local_batch_index) {
			throw InternalException("Pipeline broadcast exchange emitted batch %llu after batch %llu",
			                        local_batch_index, max_local_batch_index);
		}
		max_local_batch_index = MaxValue(max_local_batch_index, local_batch_index);
		auto result = producer_pipeline_offset + local_batch_index - 1;
		if (result >= PipelineBuildState::BATCH_INCREMENT - 1) {
			throw InternalException("Pipeline broadcast exchange exceeded the batch index range");
		}
		return result;
	}

	void ReserveAppend(AppendReservation &reservation, idx_t producer_batch_index) {
		reservation.shared_spool = shared_spool;
		reservation.position = next_position;
		reservation.producer_batch_index = producer_batch_index;
		reservation.exchange_batch_index = ExchangeBatchIndex(producer_batch_index);
		reservation.pending = false;
	}

	shared_ptr<DataChunk> ReservePendingAppend(AppendReservation &reservation) {
		D_ASSERT(HasReadyBatch());
		auto &pending = pending_batches.begin()->second.front();
		reservation.shared_spool = shared_spool;
		reservation.position = next_position;
		reservation.producer_batch_index = pending.batch_index;
		reservation.exchange_batch_index = ExchangeBatchIndex(pending.batch_index);
		reservation.pending = true;
		return pending.chunk;
	}

	void Stage(shared_ptr<DataChunk> copy, idx_t batch_index) {
		pending_batches[batch_index].push_back({std::move(copy), batch_index});
		pending_chunks++;
	}

	void CompleteAppend(const AppendReservation &reservation, shared_ptr<DataChunk> copy) {
		if (reservation.pending) {
			D_ASSERT(HasReadyBatch());
			auto pending_entry = pending_batches.begin();
			D_ASSERT(pending_entry->first == reservation.producer_batch_index);
			D_ASSERT(pending_entry->second.front().chunk == copy);
			pending_entry->second.pop_front();
			pending_chunks--;
			if (pending_entry->second.empty()) {
				pending_batches.erase(pending_entry);
			}
		}
		if (!reservation.shared_spool) {
			chunks.push_back({std::move(copy), reservation.exchange_batch_index});
		}
		buffered_chunks++;
		D_ASSERT(next_position == reservation.position);
		next_position++;
	}

	void ReserveRead(idx_t position, shared_ptr<BroadcastSpoolReader> &reader, shared_ptr<DataChunk> &next_chunk,
	                 optional_idx &batch_index, SpoolReadReservation &spool_read) const {
		D_ASSERT(position < next_position);
		if (shared_spool) {
			if (!shared_spool->HasPosition(position)) {
				throw InternalException("Pipeline broadcast shared spool chunk was retired before it was read");
			}
			if (!reader) {
				reader = make_shared_ptr<BroadcastSpoolReader>(*shared_spool);
			}
			spool_read.spool = shared_spool;
			spool_read.reader = reader;
			spool_read.position = position;
			auto stored_batch_index = shared_spool->BatchIndex(position);
			if (stored_batch_index != DConstants::INVALID_INDEX) {
				batch_index = stored_batch_index;
			}
			return;
		}
		D_ASSERT(position >= base_position);
		auto chunk_idx = position - base_position;
		D_ASSERT(chunk_idx < chunks.size());
		next_chunk = chunks[chunk_idx].chunk;
		if (chunks[chunk_idx].batch_index != DConstants::INVALID_INDEX) {
			batch_index = chunks[chunk_idx].batch_index;
		}
	}

	void CreateSharedSpool() {
		D_ASSERT(!shared_spool);
		shared_spool = make_shared_ptr<BroadcastSpool>(context, types, base_position);
		for (auto &chunk : chunks) {
			shared_spool->Append(*chunk.chunk, chunk.batch_index);
		}
		chunks.clear();
	}

	void RetireBefore(idx_t position) {
		if (shared_spool) {
			auto retired_chunks = shared_spool->RetireBefore(position);
			buffered_chunks -= MinValue(buffered_chunks, retired_chunks);
			base_position = MaxValue(base_position, position);
			return;
		}
		while (!chunks.empty() && base_position < position) {
			buffered_chunks -= MinValue<idx_t>(buffered_chunks, 1);
			chunks.pop_front();
			base_position++;
		}
	}

	void Release() {
		chunks.clear();
		pending_batches.clear();
		shared_spool.reset();
		buffered_chunks = 0;
		pending_chunks = 0;
		base_position = next_position;
	}

	ClientContext &context;
	const vector<LogicalType> &types;
	shared_ptr<ChunkPool> chunk_pool;
	deque<BufferedChunk> chunks;
	map<idx_t, deque<BufferedChunk>> pending_batches;
	multiset<idx_t> active_batches;
	shared_ptr<BroadcastSpool> shared_spool;
	idx_t base_position = 0;
	idx_t next_position = 0;
	idx_t buffered_chunks = 0;
	idx_t pending_chunks = 0;
	idx_t min_batch_index = 0;
	idx_t producer_pipeline_index = DConstants::INVALID_INDEX;
	idx_t producer_pipeline_offset = 0;
	idx_t max_local_batch_index = 0;
};

PipelineBroadcastExchange::ConsumerState::ConsumerState() = default;
PipelineBroadcastExchange::ConsumerState::~ConsumerState() = default;
PipelineBroadcastExchange::ConsumerState::ConsumerState(ConsumerState &&other) noexcept = default;
PipelineBroadcastExchange::ConsumerState &
PipelineBroadcastExchange::ConsumerState::operator=(ConsumerState &&other) noexcept = default;

PipelineBroadcastExchange::~PipelineBroadcastExchange() = default;

PipelineBroadcastExchangeLocalState::PipelineBroadcastExchangeLocalState(ClientContext &context,
                                                                         const PipelineBroadcastExchange &exchange,
                                                                         idx_t producer_base_batch_index_p)
    : producer_base_batch_index(producer_base_batch_index_p), supports_batch_index(exchange.SupportsBatchIndex()) {
	vector<reference<Pipeline>> direct_pipeline_refs;
	{
		annotated_lock_guard<annotated_mutex> guard(exchange.lock);
		auto summary = exchange.GetConsumerSummaryLocked();
		D_ASSERT(summary.unresolved == 0);
		mode = summary.ExchangeConsumerCount() > 0 && summary.buffered == 0 &&
		               exchange.direct_pipelines.size() == summary.direct
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
                                                     PipelineBroadcastExchangeCompletionMode completion_mode_p,
                                                     OrderPreservationType source_order_p, bool use_batch_index_p)
    : context(context), types(std::move(types_p)), completion_mode(completion_mode_p),
      order_mode(use_batch_index_p                                   ? PipelineBroadcastExchangeOrderMode::BATCH_INDEX
                 : source_order_p == OrderPreservationType::NO_ORDER ? PipelineBroadcastExchangeOrderMode::UNORDERED
                                                                     : PipelineBroadcastExchangeOrderMode::SEQUENTIAL),
      source_order(source_order_p),
      max_threads(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads())) {
	D_ASSERT(!use_batch_index_p || source_order_p != OrderPreservationType::NO_ORDER);
	buffer = make_uniq<BufferState>(context, types, max_threads);
}

unique_ptr<PipelineBroadcastExchangeLocalState>
PipelineBroadcastExchange::GetLocalState(ClientContext &context, idx_t producer_base_batch_index) const {
	return make_uniq<PipelineBroadcastExchangeLocalState>(context, *this, producer_base_batch_index);
}

shared_ptr<PipelineBroadcastExchangeScanState> PipelineBroadcastExchange::GetScanState() const {
	return make_shared_ptr<PipelineBroadcastExchangeScanState>();
}

void PipelineBroadcastExchange::SetProducerPipelines(const vector<shared_ptr<Pipeline>> &pipelines) {
	D_ASSERT(!pipelines.empty());
	annotated_lock_guard<annotated_mutex> guard(lock);
	producer_pipelines.clear();
	for (auto &pipeline : pipelines) {
		producer_pipelines.push_back(*pipeline);
	}
}

idx_t PipelineBroadcastExchange::RegisterConsumer() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	ConsumerState state;
	state.position = buffer->BasePosition();
	consumers.push_back(std::move(state));
	active_consumers++;
	return consumers.size() - 1;
}

void PipelineBroadcastExchange::SelectMaterializedConsumer(idx_t consumer_idx) {
	// Materialized consumers are served from CTE storage instead of this exchange.
	annotated_lock_guard<annotated_mutex> guard(lock);
	D_ASSERT(consumer_idx < consumers.size());
	auto &consumer = consumers[consumer_idx];
	D_ASSERT(consumer.mode == PipelineBroadcastExchangeConsumerMode::UNRESOLVED);
	consumer.mode = PipelineBroadcastExchangeConsumerMode::MATERIALIZED;
	DeactivateConsumerLocked(consumer, buffer->NextPosition());
}

bool PipelineBroadcastExchange::TryRegisterDirectConsumer(Pipeline &pipeline, idx_t consumer_idx) {
	auto source_partition_info =
	    SupportsBatchIndex() ? OperatorPartitionInfo::BatchIndex() : OperatorPartitionInfo::NoPartitionInfo();
	if (!pipeline.CanUseExternalInput(source_partition_info)) {
		return false;
	}
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto required_partition_info = pipeline.GetSink()->RequiredPartitionInfo();
	if (required_partition_info.RequiresBatchIndex() && producer_pipelines.size() != 1) {
		return false;
	}
	D_ASSERT(consumer_idx < consumers.size());
	auto &consumer = consumers[consumer_idx];
	if (consumer.mode == PipelineBroadcastExchangeConsumerMode::DIRECT) {
		return true;
	}
	D_ASSERT(consumer.mode == PipelineBroadcastExchangeConsumerMode::UNRESOLVED);
	consumer.mode = PipelineBroadcastExchangeConsumerMode::DIRECT;
	DeactivateConsumerLocked(consumer, buffer->BasePosition());
	direct_pipelines.push_back(pipeline);
	if (pipeline.IsStreamingResultPipeline()) {
		for (auto &producer_pipeline : producer_pipelines) {
			producer_pipeline.get().SetExternalStreamingResultProducer();
		}
	}
	return true;
}

void PipelineBroadcastExchange::SelectBufferedConsumer(idx_t consumer_idx) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	D_ASSERT(consumer_idx < consumers.size());
	auto &consumer = consumers[consumer_idx];
	D_ASSERT(consumer.mode == PipelineBroadcastExchangeConsumerMode::UNRESOLVED);
	consumer.mode = PipelineBroadcastExchangeConsumerMode::BUFFERED;
}

void PipelineBroadcastExchange::ResetConsumerRegistrations() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	buffer->EndExecution();
	ResetExchangeStateLocked();
	direct_pipelines.clear();
	producer_pipelines.clear();
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
		buffer->BeginExecution();
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
	buffer->Reset();
	produced_rows.store(0, std::memory_order_relaxed);
	producer_state = ProducerState::ACTIVE;
	watermark_state = WatermarkState::BELOW_HIGH_WATERMARK;
	append_reservation_state = AppendReservationState::IDLE;
	active_consumers = 0;
}

void PipelineBroadcastExchange::ResetConsumerReadStateLocked(ConsumerState &consumer, idx_t position) {
	consumer.position = position;
	consumer.exhausted = false;
	consumer.in_flight_reads.clear();
}

void PipelineBroadcastExchange::ResetConsumerRegistrationLocked(ConsumerState &consumer) {
	consumer.rows_read = 0;
	consumer.lifecycle = ConsumerLifecycle::ACTIVE;
	consumer.mode = PipelineBroadcastExchangeConsumerMode::UNRESOLVED;
	ResetConsumerReadStateLocked(consumer, buffer->BasePosition());
	active_consumers++;
}

void PipelineBroadcastExchange::ResetConsumerExecutionLocked(ConsumerState &consumer) {
	consumer.rows_read = 0;
	D_ASSERT(consumer.mode != PipelineBroadcastExchangeConsumerMode::UNRESOLVED);
	consumer.lifecycle = consumer.mode == PipelineBroadcastExchangeConsumerMode::BUFFERED ? ConsumerLifecycle::ACTIVE
	                                                                                      : ConsumerLifecycle::INACTIVE;
	ResetConsumerReadStateLocked(consumer, buffer->BasePosition());
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
                                               const SourcePartitionInfo &partition_info,
                                               const InterruptState &interrupt_state) {
	if (lstate.HasDirectConsumers() &&
	    (lstate.direct_push_state == PipelineBroadcastExchangeDirectPushState::NOT_STARTED ||
	     lstate.direct_push_state == PipelineBroadcastExchangeDirectPushState::RESUMING)) {
		auto direct_result = lstate.Push(chunk, partition_info, interrupt_state);
		if (direct_result == SinkResultType::BLOCKED) {
			return SinkResultType::BLOCKED;
		}
	}

	if (lstate.mode == PipelineBroadcastExchangeLocalMode::BUFFERED) {
		idx_t batch_index = DConstants::INVALID_INDEX;
		idx_t min_batch_index = DConstants::INVALID_INDEX;
		if (SupportsBatchIndex()) {
			D_ASSERT(partition_info.batch_index.IsValid());
			D_ASSERT(partition_info.min_batch_index.IsValid());
			batch_index = partition_info.batch_index.GetIndex();
			min_batch_index = partition_info.min_batch_index.GetIndex();
		}
		auto append_result = Append(chunk, batch_index, min_batch_index, interrupt_state);
		if (append_result == BufferedPushState::BLOCKED) {
			return SinkResultType::BLOCKED;
		}
		return CompletePush(chunk, lstate, append_result);
	}
	return CompletePush(chunk, lstate, BufferedPushState::NOT_REQUIRED);
}

SinkResultType PipelineBroadcastExchange::CompletePush(DataChunk &chunk, PipelineBroadcastExchangeLocalState &lstate,
                                                       BufferedPushState buffered_state) {
	if (buffered_state != BufferedPushState::APPENDED && buffered_state != BufferedPushState::STAGED &&
	    buffered_state != BufferedPushState::CANCELLED) {
		RecordProducedRows(chunk.size());
	}
	const auto direct_consumers_finished = lstate.DirectConsumersFinished();
	lstate.ResetPush();
	if (buffered_state == BufferedPushState::CANCELLED ||
	    (!RunToCompletion() && direct_consumers_finished &&
	     (buffered_state == BufferedPushState::UNCONSUMED || buffered_state == BufferedPushState::NOT_REQUIRED))) {
		return SinkResultType::FINISHED;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkNextBatchType PipelineBroadcastExchange::NextBatch(const SourcePartitionInfo &partition_info,
                                                       const InterruptState &interrupt_state) {
	if (!SupportsBatchIndex()) {
		return SinkNextBatchType::READY;
	}
	D_ASSERT(partition_info.min_batch_index.IsValid());
	auto result = FlushReadyBatches(partition_info.min_batch_index.GetIndex(), interrupt_state);
	return result == BufferedPushState::BLOCKED ? SinkNextBatchType::BLOCKED : SinkNextBatchType::READY;
}

SinkCombineResultType PipelineBroadcastExchange::FinishLocal(PipelineBroadcastExchangeLocalState &lstate,
                                                             const SourcePartitionInfo &partition_info,
                                                             const InterruptState &interrupt_state) {
	if (SupportsBatchIndex()) {
		D_ASSERT(partition_info.min_batch_index.IsValid());
		auto result = FlushReadyBatches(partition_info.min_batch_index.GetIndex(), interrupt_state);
		if (result == BufferedPushState::BLOCKED) {
			return SinkCombineResultType::BLOCKED;
		}
	}
	return lstate.Finish(interrupt_state);
}

SinkResultType PipelineBroadcastExchangeLocalState::Push(DataChunk &chunk, const SourcePartitionInfo &partition_info,
                                                         const InterruptState &interrupt_state) {
	if (direct_push_state != PipelineBroadcastExchangeDirectPushState::RESUMING) {
		direct_idx = 0;
	}
	OperatorPartitionData source_partition_data(0);
	if (supports_batch_index) {
		if (!partition_info.batch_index.IsValid()) {
			throw InternalException("Batch-indexed pipeline broadcast exchange received no batch index");
		}
		auto batch_index = partition_info.batch_index.GetIndex();
		if (batch_index <= producer_base_batch_index) {
			throw InternalException("Pipeline broadcast exchange received invalid producer batch index %llu",
			                        batch_index);
		}
		source_partition_data.batch_index = batch_index - producer_base_batch_index - 1;
		if (source_partition_data.batch_index >= PipelineBuildState::BATCH_INCREMENT - 2) {
			throw InternalException("Pipeline broadcast exchange received producer batch index outside its pipeline");
		}
	}
	for (; direct_idx < direct_executors.size(); direct_idx++) {
		auto &executor = *direct_executors[direct_idx];
		executor.SetInterruptState(interrupt_state);
		if (executor.IsFinishedProcessing()) {
			continue;
		}
		auto result = executor.PushExternal(chunk, source_partition_data);
		if (result == PipelineExecuteResult::INTERRUPTED) {
			direct_push_state = PipelineBroadcastExchangeDirectPushState::RESUMING;
			return SinkResultType::BLOCKED;
		}
	}

	direct_push_state = PipelineBroadcastExchangeDirectPushState::FINISHED;
	for (auto &executor : direct_executors) {
		if (!executor->IsFinishedProcessing()) {
			direct_push_state = PipelineBroadcastExchangeDirectPushState::ACTIVE;
			break;
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PipelineBroadcastExchangeLocalState::Finish(const InterruptState &interrupt_state) {
	for (; direct_finalize_idx < direct_executors.size(); direct_finalize_idx++) {
		auto &executor = *direct_executors[direct_finalize_idx];
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

bool PipelineBroadcastExchangeLocalState::HasDirectConsumers() const {
	return !direct_executors.empty();
}

bool PipelineBroadcastExchangeLocalState::DirectConsumersFinished() const {
	return direct_executors.empty() || direct_push_state == PipelineBroadcastExchangeDirectPushState::FINISHED;
}

void PipelineBroadcastExchangeLocalState::ResetPush() {
	direct_push_state = PipelineBroadcastExchangeDirectPushState::NOT_STARTED;
}

PipelineBroadcastExchange::AppendAdmission
PipelineBroadcastExchange::PrepareAppendLocked(const InterruptState &interrupt_state,
                                               vector<ExchangeLogEntry> &log_entries) {
	if (producer_state != ProducerState::ACTIVE) {
		return AppendAdmission::CANCELLED;
	}
	if (active_consumers == 0) {
		return AppendAdmission::UNCONSUMED;
	}
	if (append_reservation_state == AppendReservationState::RESERVED) {
		blocked_appenders.push_back(interrupt_state);
		return AppendAdmission::BLOCKED;
	}
	if (ShouldCreateSharedSpoolLocked()) {
		CreateSharedSpoolLocked(log_entries);
	}
	if (ShouldThrottleProducerLocked()) {
		if (watermark_state == WatermarkState::BELOW_HIGH_WATERMARK) {
			watermark_state = WatermarkState::ABOVE_HIGH_WATERMARK;
			log_entries.push_back(
			    {ExchangeLogEvent::HIGH_WATERMARK_BLOCKED, active_consumers, buffer->BufferedCount()});
		}
		blocked_writers.push_back(interrupt_state);
		return AppendAdmission::BLOCKED;
	}
	return AppendAdmission::READY;
}

PipelineBroadcastExchange::AppendAdmission
PipelineBroadcastExchange::ReserveAppendLocked(idx_t batch_index, const InterruptState &interrupt_state,
                                               AppendReservation &reservation, vector<ExchangeLogEntry> &log_entries) {
	auto admission = PrepareAppendLocked(interrupt_state, log_entries);
	if (admission != AppendAdmission::READY) {
		return admission;
	}
	append_reservation_state = AppendReservationState::RESERVED;
	buffer->ReserveAppend(reservation, batch_index);
	return AppendAdmission::READY;
}

PipelineBroadcastExchange::AppendAdmission
PipelineBroadcastExchange::PrepareStageLocked(idx_t batch_index, const InterruptState &interrupt_state,
                                              vector<ExchangeLogEntry> &log_entries) {
	if (producer_state != ProducerState::ACTIVE) {
		return AppendAdmission::CANCELLED;
	}
	if (active_consumers == 0) {
		return AppendAdmission::UNCONSUMED;
	}
	if (batch_index > buffer->MinBatchIndex() && buffer->PendingCount() >= PIPELINE_BROADCAST_HIGH_WATERMARK_CHUNKS) {
		if (watermark_state == WatermarkState::BELOW_HIGH_WATERMARK) {
			watermark_state = WatermarkState::ABOVE_HIGH_WATERMARK;
			log_entries.push_back({ExchangeLogEvent::HIGH_WATERMARK_BLOCKED, active_consumers, buffer->PendingCount()});
		}
		blocked_writers.push_back(interrupt_state);
		return AppendAdmission::BLOCKED;
	}
	return AppendAdmission::READY;
}

PipelineBroadcastExchange::BufferedPushState PipelineBroadcastExchange::CompleteAppendLocked(
    const AppendReservation &reservation, shared_ptr<DataChunk> copy, idx_t row_count, bool record_produced_rows,
    vector<InterruptState> &readers, vector<InterruptState> &appenders, vector<ExchangeLogEntry> &log_entries) {
	D_ASSERT(append_reservation_state == AppendReservationState::RESERVED);
	append_reservation_state = AppendReservationState::IDLE;
	if (producer_state == ProducerState::CANCELLED) {
		TryReleaseBufferedStorageLocked();
		WakeReadersLocked(readers);
		WakeAppendersLocked(appenders);
		return BufferedPushState::CANCELLED;
	}
	D_ASSERT(producer_state == ProducerState::ACTIVE);
	if (active_consumers == 0) {
		TryReleaseBufferedStorageLocked();
		WakeAppendersLocked(appenders);
		return BufferedPushState::UNCONSUMED;
	}
	buffer->CompleteAppend(reservation, std::move(copy));
	WakeReadersLocked(readers);
	if (record_produced_rows) {
		RecordProducedRows(row_count);
	}
	WakeAppendersLocked(appenders);
	return BufferedPushState::APPENDED;
}

void PipelineBroadcastExchange::AbortAppendReservation(vector<InterruptState> &readers, vector<InterruptState> &writers,
                                                       vector<InterruptState> &appenders) {
	D_ASSERT(append_reservation_state == AppendReservationState::RESERVED);
	append_reservation_state = AppendReservationState::IDLE;
	producer_state = ProducerState::CANCELLED;
	DeactivateAllConsumersLocked();
	TryReleaseBufferedStorageLocked();
	WakeReadersLocked(readers);
	vector<ExchangeLogEntry> ignored_log_entries;
	WakeWritersLocked(writers, ignored_log_entries, WriterWakeMode::FORCE);
	WakeAppendersLocked(appenders);
}

PipelineBroadcastExchange::BufferedPushState
PipelineBroadcastExchange::FlushReadyBatches(idx_t min_batch_index, const InterruptState &interrupt_state) {
	D_ASSERT(SupportsBatchIndex());
	bool appended = false;
	while (true) {
		vector<ExchangeLogEntry> log_entries;
		AppendAdmission admission;
		AppendReservation reservation;
		shared_ptr<DataChunk> copy;
		try {
			annotated_lock_guard<annotated_mutex> guard(lock);
			buffer->UpdateMinBatchIndex(min_batch_index);
			if (!buffer->HasReadyBatch()) {
				return appended ? BufferedPushState::APPENDED : BufferedPushState::NOT_REQUIRED;
			}
			admission = PrepareAppendLocked(interrupt_state, log_entries);
			if (admission == AppendAdmission::READY) {
				append_reservation_state = AppendReservationState::RESERVED;
				copy = buffer->ReservePendingAppend(reservation);
			}
		} catch (...) {
			Cancel();
			throw;
		}
		LogTransitions(log_entries);
		if (admission == AppendAdmission::BLOCKED) {
			return BufferedPushState::BLOCKED;
		}
		if (admission == AppendAdmission::UNCONSUMED) {
			return BufferedPushState::UNCONSUMED;
		}
		if (admission == AppendAdmission::CANCELLED) {
			return BufferedPushState::CANCELLED;
		}

		try {
			if (reservation.shared_spool) {
				reservation.shared_spool->Append(*copy, reservation.exchange_batch_index);
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
			buffer->EndExecution();
			throw;
		}

		vector<InterruptState> readers;
		vector<InterruptState> writers;
		vector<InterruptState> appenders;
		log_entries.clear();
		BufferedPushState result;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			result = CompleteAppendLocked(reservation, std::move(copy), 0, false, readers, appenders, log_entries);
			WakeWritersLocked(writers, log_entries, WriterWakeMode::FORCE);
		}
		CallbackAll(readers);
		CallbackAll(writers);
		CallbackAll(appenders);
		LogTransitions(log_entries);
		if (result != BufferedPushState::APPENDED) {
			return result;
		}
		appended = true;
	}
}

PipelineBroadcastExchange::BufferedPushState PipelineBroadcastExchange::Append(DataChunk &chunk, idx_t batch_index,
                                                                               idx_t min_batch_index,
                                                                               const InterruptState &interrupt_state) {
	if (SupportsBatchIndex()) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		buffer->RegisterActiveBatch(batch_index);
	}
	auto unregister_active_batch = [&](idx_t *) {
		if (!SupportsBatchIndex()) {
			return;
		}
		annotated_lock_guard<annotated_mutex> guard(lock);
		buffer->UnregisterActiveBatch(batch_index);
	};
	unique_ptr<idx_t, decltype(unregister_active_batch)> active_batch_guard(
	    SupportsBatchIndex() ? &batch_index : nullptr, unregister_active_batch);

	if (SupportsBatchIndex()) {
		auto flush_result = FlushReadyBatches(min_batch_index, interrupt_state);
		if (flush_result == BufferedPushState::BLOCKED || flush_result == BufferedPushState::UNCONSUMED ||
		    flush_result == BufferedPushState::CANCELLED) {
			return flush_result;
		}
	}

	vector<ExchangeLogEntry> log_entries;
	AppendAdmission admission;
	try {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (SupportsBatchIndex()) {
			buffer->UpdateMinBatchIndex(min_batch_index);
		}
		admission = SupportsBatchIndex() && batch_index > buffer->MinBatchIndex()
		                ? PrepareStageLocked(batch_index, interrupt_state, log_entries)
		                : PrepareAppendLocked(interrupt_state, log_entries);
	} catch (...) {
		Cancel();
		throw;
	}
	LogTransitions(log_entries);
	if (admission == AppendAdmission::BLOCKED) {
		return BufferedPushState::BLOCKED;
	}
	if (admission == AppendAdmission::UNCONSUMED) {
		return BufferedPushState::UNCONSUMED;
	}
	if (admission == AppendAdmission::CANCELLED) {
		return BufferedPushState::CANCELLED;
	}

	shared_ptr<DataChunk> copy;
	try {
		copy = buffer->Copy(chunk);
	} catch (...) {
		Cancel();
		throw;
	}
	AppendReservation reservation;
	log_entries.clear();
	bool staged = false;
	try {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (SupportsBatchIndex()) {
			buffer->UpdateMinBatchIndex(min_batch_index);
		}
		if (SupportsBatchIndex() && (batch_index > buffer->MinBatchIndex() ||
		                             buffer->HasEarlierActiveBatch(batch_index) || buffer->HasReadyBatch())) {
			admission = PrepareStageLocked(batch_index, interrupt_state, log_entries);
			if (admission == AppendAdmission::READY) {
				buffer->Stage(copy, batch_index);
				RecordProducedRows(chunk.size());
				staged = true;
			}
		} else {
			admission = ReserveAppendLocked(batch_index, interrupt_state, reservation, log_entries);
		}
	} catch (...) {
		Cancel();
		throw;
	}
	LogTransitions(log_entries);
	if (admission == AppendAdmission::BLOCKED) {
		return BufferedPushState::BLOCKED;
	}
	if (admission == AppendAdmission::UNCONSUMED) {
		return BufferedPushState::UNCONSUMED;
	}
	if (admission == AppendAdmission::CANCELLED) {
		return BufferedPushState::CANCELLED;
	}
	if (staged) {
		return BufferedPushState::STAGED;
	}

	try {
		if (reservation.shared_spool) {
			reservation.shared_spool->Append(*copy, reservation.exchange_batch_index);
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
		buffer->EndExecution();
		throw;
	}

	vector<InterruptState> readers;
	vector<InterruptState> appenders;
	log_entries.clear();
	BufferedPushState result;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		result =
		    CompleteAppendLocked(reservation, std::move(copy), chunk.size(), true, readers, appenders, log_entries);
	}
	CallbackAll(readers);
	CallbackAll(appenders);
	LogTransitions(log_entries);
	return result;
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
		if (active_consumers > 0 && buffer->PendingCount() > 0) {
			throw InternalException("Finishing ordered pipeline broadcast exchange with %llu pending chunks",
			                        buffer->PendingCount());
		}
		if (producer_state == ProducerState::ACTIVE) {
			producer_state = ProducerState::FINISHED;
		}
		TryReleaseBufferedStorageLocked();
		WakeReadersLocked(readers);
		WakeWritersLocked(writers, log_entries, WriterWakeMode::FORCE);
	}
	buffer->EndExecution();
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
		if (producer_state != ProducerState::CANCELLED) {
			producer_state = ProducerState::CANCELLED;
			DeactivateAllConsumersLocked();
			TryReleaseBufferedStorageLocked();
		}
		WakeReadersLocked(readers);
		WakeWritersLocked(writers, log_entries, WriterWakeMode::FORCE);
		WakeAppendersLocked(appenders);
	}
	buffer->EndExecution();
	CallbackAll(readers);
	CallbackAll(writers);
	CallbackAll(appenders);
	LogTransitions(log_entries);
}

SourceResultType PipelineBroadcastExchange::Scan(idx_t consumer_idx, DataChunk &chunk,
                                                 PipelineBroadcastExchangeScanState &scan_state,
                                                 optional_idx &batch_index, const InterruptState &interrupt_state) {
	vector<InterruptState> writers;
	vector<InterruptState> readers;
	vector<ExchangeLogEntry> log_entries;
	shared_ptr<DataChunk> next_chunk;
	SpoolReadReservation spool_read;
	SourceResultType result;
	batch_index = optional_idx();
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		result = ReserveScanLocked(consumer_idx, interrupt_state, scan_state, next_chunk, batch_index, spool_read,
		                           writers, log_entries);
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
				consumer.in_flight_reads.erase(spool_read.position);
				producer_state = ProducerState::CANCELLED;
				DeactivateAllConsumersLocked();
				TryReleaseBufferedStorageLocked();
				WakeReadersLocked(failed_readers);
				vector<ExchangeLogEntry> ignored_log_entries;
				WakeWritersLocked(failed_writers, ignored_log_entries, WriterWakeMode::FORCE);
				WakeAppendersLocked(appenders);
			}
			CallbackAll(failed_readers);
			CallbackAll(failed_writers);
			CallbackAll(appenders);
			buffer->EndExecution();
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
		scan_state.current_chunk = std::move(next_chunk);
		chunk.Reference(*scan_state.current_chunk);
	}
	return result;
}

SourceResultType PipelineBroadcastExchange::ReserveScanLocked(
    idx_t consumer_idx, const InterruptState &interrupt_state, PipelineBroadcastExchangeScanState &scan_state,
    shared_ptr<DataChunk> &next_chunk, optional_idx &batch_index, SpoolReadReservation &spool_read,
    vector<InterruptState> &writers, vector<ExchangeLogEntry> &log_entries) {
	D_ASSERT(consumer_idx < consumers.size());
	auto &consumer = consumers[consumer_idx];
	if (consumer.lifecycle != ConsumerLifecycle::ACTIVE || producer_state == ProducerState::CANCELLED) {
		return SourceResultType::FINISHED;
	}
	if (consumer.position < buffer->NextPosition()) {
		auto position = consumer.position++;
		buffer->ReserveRead(position, scan_state.spool_reader, next_chunk, batch_index, spool_read);
		if (spool_read.IsSet()) {
			auto inserted = consumer.in_flight_reads.insert(position);
			D_ASSERT(inserted.second);
		} else {
			consumer.rows_read += next_chunk->size();
			RetireChunksLocked();
			WakeWritersLocked(writers, log_entries);
		}
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	if (producer_state == ProducerState::FINISHED) {
		consumer.exhausted = true;
		if (consumer.in_flight_reads.empty()) {
			consumer.lifecycle = ConsumerLifecycle::INACTIVE;
			D_ASSERT(active_consumers > 0);
			active_consumers--;
		}
		RetireChunksLocked();
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
	auto entry = consumer.in_flight_reads.find(spool_read.position);
	D_ASSERT(entry != consumer.in_flight_reads.end());
	consumer.in_flight_reads.erase(entry);
	if (consumer.lifecycle != ConsumerLifecycle::ACTIVE || producer_state == ProducerState::CANCELLED) {
		chunk.Reset();
		RetireChunksLocked();
		WakeWritersLocked(writers, log_entries, WriterWakeMode::FORCE);
		WakeReadersLocked(readers);
		return;
	}
	consumer.rows_read += chunk.size();
	if (consumer.exhausted && consumer.in_flight_reads.empty()) {
		consumer.lifecycle = ConsumerLifecycle::INACTIVE;
		D_ASSERT(active_consumers > 0);
		active_consumers--;
	}
	RetireChunksLocked();
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
		if (consumer.exhausted) {
			return;
		}
		consumer.lifecycle = ConsumerLifecycle::INACTIVE;
		consumer.position = buffer->NextPosition();
		D_ASSERT(active_consumers > 0);
		active_consumers--;
		RetireChunksLocked();
		WakeReadersLocked(readers);
		WakeWritersLocked(writers, log_entries, WriterWakeMode::FORCE);
		log_entries.push_back({ExchangeLogEvent::CONSUMER_UNREGISTERED, active_consumers, buffer->BufferedCount()});
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
	return order_mode == PipelineBroadcastExchangeOrderMode::SEQUENTIAL ? 1 : MaxValue<idx_t>(max_threads, 1);
}

idx_t PipelineBroadcastExchange::RegisteredConsumerCount() const {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return consumers.size();
}

PipelineBroadcastExchangeConsumerMode PipelineBroadcastExchange::GetConsumerMode(idx_t consumer_idx) const {
	annotated_lock_guard<annotated_mutex> guard(lock);
	D_ASSERT(consumer_idx < consumers.size());
	return consumers[consumer_idx].mode;
}

PipelineBroadcastExchangeConsumerSummary PipelineBroadcastExchange::GetConsumerSummaryLocked() const {
	PipelineBroadcastExchangeConsumerSummary result;
	for (auto &consumer : consumers) {
		switch (consumer.mode) {
		case PipelineBroadcastExchangeConsumerMode::UNRESOLVED:
			result.unresolved++;
			break;
		case PipelineBroadcastExchangeConsumerMode::BUFFERED:
			result.buffered++;
			break;
		case PipelineBroadcastExchangeConsumerMode::DIRECT:
			result.direct++;
			break;
		case PipelineBroadcastExchangeConsumerMode::MATERIALIZED:
			result.materialized++;
			break;
		default:
			throw InternalException("Unknown pipeline broadcast exchange consumer mode");
		}
	}
	return result;
}

PipelineBroadcastExchangeConsumerSummary PipelineBroadcastExchange::GetConsumerSummary() const {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return GetConsumerSummaryLocked();
}

bool PipelineBroadcastExchange::ShouldStopProducerLocked() const {
	return completion_mode == PipelineBroadcastExchangeCompletionMode::STOP_WHEN_UNCONSUMED && active_consumers == 0;
}

bool PipelineBroadcastExchange::ShouldThrottleProducerLocked() const {
	if (buffer->HasSharedSpool()) {
		return false;
	}
	return active_consumers > 0 && buffer->BufferedCount() >= PIPELINE_BROADCAST_HIGH_WATERMARK_CHUNKS;
}

bool PipelineBroadcastExchange::ShouldCreateSharedSpoolLocked() const {
	return !buffer->HasSharedSpool() && (!direct_pipelines.empty() || active_consumers > 1) &&
	       buffer->BufferedCount() >= PIPELINE_BROADCAST_HIGH_WATERMARK_CHUNKS;
}

void PipelineBroadcastExchange::CreateSharedSpoolLocked(vector<ExchangeLogEntry> &log_entries) {
	buffer->CreateSharedSpool();
	log_entries.push_back({ExchangeLogEvent::SPOOL_CREATED, active_consumers, buffer->BufferedCount()});
}

void PipelineBroadcastExchange::RetireChunksLocked() {
	if (buffer->Empty()) {
		return;
	}
	idx_t min_position = buffer->NextPosition();
	bool found_reader = false;
	for (auto &consumer : consumers) {
		if (!consumer.in_flight_reads.empty()) {
			found_reader = true;
			min_position = MinValue(min_position, *consumer.in_flight_reads.begin());
		}
		if (consumer.lifecycle == ConsumerLifecycle::ACTIVE) {
			found_reader = true;
			min_position = MinValue(min_position, consumer.position);
		}
	}
	if (!found_reader) {
		min_position = buffer->NextPosition();
	}
	buffer->RetireBefore(min_position);
	TryReleaseBufferedStorageLocked();
}

void PipelineBroadcastExchange::TryReleaseBufferedStorageLocked() {
	if (active_consumers > 0 || append_reservation_state == AppendReservationState::RESERVED) {
		return;
	}
	for (auto &consumer : consumers) {
		if (!consumer.in_flight_reads.empty()) {
			return;
		}
	}
	buffer->Release();
}

void PipelineBroadcastExchange::DeactivateAllConsumersLocked() {
	for (auto &consumer : consumers) {
		if (consumer.lifecycle != ConsumerLifecycle::ACTIVE) {
			continue;
		}
		consumer.lifecycle = ConsumerLifecycle::INACTIVE;
		consumer.position = buffer->NextPosition();
	}
	active_consumers = 0;
}

void PipelineBroadcastExchange::WakeReadersLocked(vector<InterruptState> &readers) {
	readers.insert(readers.end(), blocked_readers.begin(), blocked_readers.end());
	blocked_readers.clear();
}

void PipelineBroadcastExchange::WakeWritersLocked(vector<InterruptState> &writers,
                                                  vector<ExchangeLogEntry> &log_entries, WriterWakeMode mode) {
	if (mode == WriterWakeMode::LOW_WATERMARK && buffer->BufferedCount() > PIPELINE_BROADCAST_LOW_WATERMARK_CHUNKS &&
	    producer_state == ProducerState::ACTIVE && !ShouldStopProducerLocked()) {
		return;
	}
	if (mode == WriterWakeMode::LOW_WATERMARK && watermark_state == WatermarkState::ABOVE_HIGH_WATERMARK) {
		watermark_state = WatermarkState::BELOW_HIGH_WATERMARK;
		log_entries.push_back({ExchangeLogEvent::LOW_WATERMARK_WAKE, active_consumers, buffer->BufferedCount()});
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
