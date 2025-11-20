#include "duckdb/storage/compression/roaring/roaring.hpp"
#include "duckdb/storage/compression/roaring/appender.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/bitpacking.hpp"

namespace duckdb {

namespace roaring {

ContainerCompressionState::ContainerCompressionState() {
	Reset();
}

inline void AppendBitset(ContainerCompressionState &state, bool null, uint16_t amount) {
	D_ASSERT(state.uncompressed);
	if (null) {
		ValidityMask mask(state.uncompressed, ROARING_CONTAINER_SIZE);
		SetInvalidRange(mask, state.appended_count, state.appended_count + amount);
	}
}

inline void AppendRun(ContainerCompressionState &state, bool null, uint16_t amount) {
	// Adjust the run
	auto run_idx = state.run_idx;
	auto appended_count = state.appended_count;
	if (!null && run_idx < MAX_RUN_IDX && appended_count && (null != state.last_is_null)) {
		if (run_idx < COMPRESSED_RUN_THRESHOLD) {
			auto &last_run = state.runs[run_idx];
			// End the last run
			last_run.length = (appended_count - last_run.start) - 1;
		}
		state.compressed_runs[(run_idx * 2) + 1] = static_cast<uint8_t>(appended_count & (COMPRESSED_SEGMENT_SIZE - 1));
		state.run_counts[appended_count >> COMPRESSED_SEGMENT_SHIFT_AMOUNT]++;
		state.run_idx++;
	} else if (null && run_idx < MAX_RUN_IDX && (!appended_count || null != state.last_is_null)) {
		if (run_idx < COMPRESSED_RUN_THRESHOLD) {
			auto &current_run = state.runs[run_idx];
			// Initialize a new run
			current_run.start = appended_count;
		}
		state.compressed_runs[(run_idx * 2) + 0] = static_cast<uint8_t>(appended_count & (COMPRESSED_SEGMENT_SIZE - 1));
		state.run_counts[appended_count >> COMPRESSED_SEGMENT_SHIFT_AMOUNT]++;
	}
}

template <bool INVERTED>
inline void AppendToArray(ContainerCompressionState &state, bool null, uint16_t amount) {
	if (DUCKDB_LIKELY(INVERTED != null)) {
		return;
	}

	auto current_array_idx = state.array_idx[null];
	if (current_array_idx + amount > MAX_ARRAY_IDX) {
		return;
	}
	auto appended_count = state.appended_count;
	auto array_count = state.array_counts[null];
	auto compressed_array = state.compressed_arrays[null];
	uint16_t appended = 0;
	while (appended < amount) {
		uint16_t remaining = amount - appended;
		uint8_t segment_offset = appended ? 0 : (appended_count + appended) & (COMPRESSED_SEGMENT_SIZE - 1);
		uint8_t to_append =
		    static_cast<uint8_t>(MinValue<uint16_t>(remaining, COMPRESSED_SEGMENT_SIZE - segment_offset));
		for (uint8_t i = 0; i < to_append; i++) {
			auto index = current_array_idx + appended + i;
			compressed_array[index] = segment_offset + i;
		}

		idx_t segment_index = (appended_count + appended) / COMPRESSED_SEGMENT_SIZE;
		array_count[segment_index] += to_append;
		appended += to_append;
	}

	if (current_array_idx + amount < COMPRESSED_ARRAY_THRESHOLD) {
		auto &array = state.arrays[null];
		for (uint16_t i = 0; i < amount; i++) {
			array[current_array_idx + i] = appended_count + i;
		}
	}
	state.array_idx[null] += amount;
}

void ContainerCompressionState::Append(bool null, uint16_t amount) {
	append_function(*this, null, amount);
	last_is_null = null;
	null_count += null * amount;
	appended_count += amount;
}

void ContainerCompressionState::OverrideArray(data_ptr_t &destination, bool nulls, idx_t count) {
	if (nulls) {
		append_function = AppendToArray<true>;
	} else {
		append_function = AppendToArray<false>;
	}

	if (count >= COMPRESSED_ARRAY_THRESHOLD) {
		memset(destination, 0, sizeof(uint8_t) * COMPRESSED_SEGMENT_COUNT);
		array_counts[nulls] = reinterpret_cast<uint8_t *>(destination);
		auto data_start = destination + sizeof(uint8_t) * COMPRESSED_SEGMENT_COUNT;
		compressed_arrays[nulls] = reinterpret_cast<uint8_t *>(data_start);
	} else {
		destination = AlignPointer<sizeof(uint16_t)>(destination);
		arrays[nulls] = reinterpret_cast<uint16_t *>(destination);
	}
}

void ContainerCompressionState::OverrideRun(data_ptr_t &destination, idx_t count) {
	append_function = AppendRun;

	if (count >= COMPRESSED_RUN_THRESHOLD) {
		memset(destination, 0, sizeof(uint8_t) * COMPRESSED_SEGMENT_COUNT);
		run_counts = reinterpret_cast<uint8_t *>(destination);
		auto data_start = destination + sizeof(uint8_t) * COMPRESSED_SEGMENT_COUNT;
		compressed_runs = reinterpret_cast<uint8_t *>(data_start);
	} else {
		destination = AlignPointer<sizeof(RunContainerRLEPair)>(destination);
		runs = reinterpret_cast<RunContainerRLEPair *>(destination);
	}
}

void ContainerCompressionState::OverrideUncompressed(data_ptr_t &destination) {
	append_function = AppendBitset;
	destination = AlignPointer<sizeof(idx_t)>(destination);
	uncompressed = reinterpret_cast<validity_t *>(destination);
}

void ContainerCompressionState::Finalize() {
	D_ASSERT(!finalized);
	if (appended_count && last_is_null && run_idx < MAX_RUN_IDX) {
		if (run_idx < COMPRESSED_RUN_THRESHOLD) {
			auto &last_run = runs[run_idx];
			// End the last run
			last_run.length = (appended_count - last_run.start);
		}
		compressed_runs[(run_idx * 2) + 1] = static_cast<uint8_t>(appended_count % COMPRESSED_SEGMENT_SIZE);
		if (appended_count != ROARING_CONTAINER_SIZE) {
			run_counts[appended_count >> COMPRESSED_SEGMENT_SHIFT_AMOUNT]++;
		}
		run_idx++;
	}
	finalized = true;
}

ContainerMetadata ContainerCompressionState::GetResult() {
	if (uncompressed) {
		return ContainerMetadata::BitsetContainer(appended_count);
	}
	D_ASSERT(finalized);
	return ContainerMetadata::CreateMetadata(appended_count, array_idx[NULLS], array_idx[NON_NULLS], run_idx);
}

void ContainerCompressionState::Reset() {
	length = 0;

	appended_count = 0;
	null_count = 0;
	run_idx = 0;
	array_idx[NON_NULLS] = 0;
	array_idx[NULLS] = 0;
	finalized = false;
	last_is_null = false;

	// Reset the arrays + runs
	arrays[NULLS] = base_arrays[NULLS];
	arrays[NON_NULLS] = base_arrays[NON_NULLS];
	runs = base_runs;

	compressed_arrays[NULLS] = base_compressed_arrays[NULLS];
	compressed_arrays[NON_NULLS] = base_compressed_arrays[NON_NULLS];
	compressed_runs = base_compressed_runs;

	array_counts[NULLS] = base_array_counts[NULLS];
	array_counts[NON_NULLS] = base_array_counts[NON_NULLS];
	run_counts = base_run_counts;

	memset(array_counts[NULLS], 0, sizeof(uint8_t) * COMPRESSED_SEGMENT_COUNT);
	memset(array_counts[NON_NULLS], 0, sizeof(uint8_t) * COMPRESSED_SEGMENT_COUNT);
	memset(run_counts, 0, sizeof(uint8_t) * COMPRESSED_SEGMENT_COUNT);
	uncompressed = nullptr;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
RoaringCompressState::RoaringCompressState(ColumnDataCheckpointData &checkpoint_data,
                                           unique_ptr<AnalyzeState> analyze_state_p)
    : CompressionState(analyze_state_p->info), owned_analyze_state(std::move(analyze_state_p)),
      analyze_state(owned_analyze_state->Cast<RoaringAnalyzeState>()), container_state(),
      container_metadata(analyze_state.container_metadata), checkpoint_data(checkpoint_data),
      function(checkpoint_data.GetCompressionFunction(CompressionType::COMPRESSION_ROARING)) {
	CreateEmptySegment();
	total_count = 0;
	InitializeContainer();
}

idx_t RoaringCompressState::GetContainerIndex() {
	idx_t index = total_count / ROARING_CONTAINER_SIZE;
	return index;
}

idx_t RoaringCompressState::GetRemainingSpace() {
	return static_cast<idx_t>(metadata_ptr - data_ptr);
}

bool RoaringCompressState::CanStore(idx_t container_size, const ContainerMetadata &metadata) {
	idx_t required_space = 0;
	if (metadata.IsUncompressed()) {
		// Account for the alignment we might need for this container
		required_space += (AlignValue<idx_t>(reinterpret_cast<idx_t>(data_ptr))) - reinterpret_cast<idx_t>(data_ptr);
	}
	required_space += metadata.GetDataSizeInBytes(container_size);

	idx_t runs_count = metadata_collection.GetRunContainerCount();
	idx_t arrays_count = metadata_collection.GetArrayAndBitsetContainerCount();
#ifdef DEBUG
	idx_t current_size = metadata_collection.GetMetadataSize(runs_count + arrays_count, runs_count, arrays_count);
	(void)current_size;
	D_ASSERT(required_space + current_size <= GetRemainingSpace());
#endif
	if (metadata.IsRun()) {
		runs_count++;
	} else {
		arrays_count++;
	}
	idx_t metadata_size = metadata_collection.GetMetadataSize(runs_count + arrays_count, runs_count, arrays_count);
	required_space += metadata_size;

	if (required_space > GetRemainingSpace()) {
		return false;
	}
	return true;
}

void RoaringCompressState::InitializeContainer() {
	if (total_count == analyze_state.total_count) {
		// No more containers left
		return;
	}
	auto container_index = GetContainerIndex();
	D_ASSERT(container_index < container_metadata.size());
	auto metadata = container_metadata[container_index];

	idx_t container_size = AlignValue<idx_t, ValidityMask::BITS_PER_VALUE>(
	    MinValue<idx_t>(analyze_state.total_count - container_state.appended_count, ROARING_CONTAINER_SIZE));
	if (!CanStore(container_size, metadata)) {
		FlushSegment();
		CreateEmptySegment();
	}

	// Override the pointer to write directly into the block
	if (metadata.IsUncompressed()) {
		data_ptr = reinterpret_cast<data_ptr_t>(AlignValue<idx_t>(reinterpret_cast<idx_t>(data_ptr)));
		FastMemset(data_ptr, ~0, sizeof(validity_t) * (container_size / ValidityMask::BITS_PER_VALUE));
		container_state.OverrideUncompressed(data_ptr);
	} else if (metadata.IsRun()) {
		auto number_of_runs = metadata.NumberOfRuns();
		container_state.OverrideRun(data_ptr, number_of_runs);
	} else {
		auto cardinality = metadata.Cardinality();
		container_state.OverrideArray(data_ptr, metadata.IsInverted(), cardinality);
	}
	data_ptr += metadata.GetDataSizeInBytes(container_size);
	metadata_collection.AddMetadata(metadata);
}

void RoaringCompressState::CreateEmptySegment() {
	auto &db = checkpoint_data.GetDatabase();
	auto &type = checkpoint_data.GetType();

	auto compressed_segment =
	    ColumnSegment::CreateTransientSegment(db, function, type, info.GetBlockSize(), info.GetBlockManager());
	current_segment = std::move(compressed_segment);

	auto &buffer_manager = BufferManager::GetBufferManager(db);
	handle = buffer_manager.Pin(current_segment->block);
	data_ptr = handle.Ptr();
	data_ptr += sizeof(idx_t);
	metadata_ptr = handle.Ptr() + info.GetBlockSize();
}

void RoaringCompressState::FlushSegment() {
	auto &state = checkpoint_data.GetCheckpointState();
	auto base_ptr = handle.Ptr();
	// +======================================+
	// |x|ddddddddddddddd||mmm|               |
	// +======================================+

	// x: metadata_offset (to the "right" of it)
	// d: data of the containers
	// m: metadata of the containers

	// This is after 'x'
	base_ptr += sizeof(idx_t);

	// Size of the 'd' part
	auto unaligned_data_size = NumericCast<idx_t>(data_ptr - base_ptr);
	auto data_size = AlignValue(unaligned_data_size);
	data_ptr += data_size - unaligned_data_size;

	// Size of the 'm' part
	auto metadata_size = metadata_collection.GetMetadataSizeForSegment();
	if (current_segment->count.load() == 0) {
		D_ASSERT(metadata_size == 0);
		return;
	}

	auto serialized_metadata_size = metadata_collection.Serialize(data_ptr);
	if (metadata_size != serialized_metadata_size) {
		throw InternalException("mismatch in metadata size during RoaringCompressState::FlushSegment");
	}

	metadata_collection.FlushSegment();
	auto metadata_start = static_cast<idx_t>(data_ptr - base_ptr);
	if (metadata_start > info.GetBlockSize()) {
		throw InternalException("metadata start outside of block size during RoaringCompressState::FlushSegment");
	}

	Store<idx_t>(metadata_start, handle.Ptr());
	auto total_segment_size = sizeof(idx_t) + data_size + metadata_size;
	state.FlushSegment(std::move(current_segment), std::move(handle), total_segment_size);
}

void RoaringCompressState::Finalize() {
	FlushContainer();
	FlushSegment();
	current_segment.reset();
}

void RoaringCompressState::FlushContainer() {
	if (container_state.length) {
		container_state.Append(!container_state.last_bit_set, container_state.length);
		container_state.length = 0;
	}

	if (!container_state.appended_count) {
		return;
	}
	container_state.Finalize();
#ifdef DEBUG
	auto container_index = GetContainerIndex();
	auto metadata = container_metadata[container_index];

	idx_t container_size = container_state.appended_count;
	if (!metadata.IsUncompressed()) {
		unique_ptr<ContainerScanState> scan_state;
		if (metadata.IsRun()) {
			D_ASSERT(metadata.IsInverted());
			auto number_of_runs = metadata.NumberOfRuns();
			if (number_of_runs >= COMPRESSED_RUN_THRESHOLD) {
				auto segments = container_state.run_counts;
				auto data_ptr = container_state.compressed_runs;
				scan_state = make_uniq<CompressedRunContainerScanState>(container_index, container_size, number_of_runs,
				                                                        segments, data_ptr);
			} else {
				auto data_ptr = reinterpret_cast<data_ptr_t>(container_state.runs);
				scan_state =
				    make_uniq<RunContainerScanState>(container_index, container_size, number_of_runs, data_ptr);
			}
		} else {
			auto cardinality = metadata.Cardinality();
			if (cardinality >= COMPRESSED_ARRAY_THRESHOLD) {
				if (metadata.IsInverted()) {
					auto segments = reinterpret_cast<data_ptr_t>(container_state.array_counts[NULLS]);
					auto data_ptr = reinterpret_cast<data_ptr_t>(container_state.compressed_arrays[NULLS]);
					scan_state = make_uniq<CompressedArrayContainerScanState<NULLS>>(container_index, container_size,
					                                                                 cardinality, segments, data_ptr);
				} else {
					auto segments = reinterpret_cast<data_ptr_t>(container_state.array_counts[NON_NULLS]);
					auto data_ptr = reinterpret_cast<data_ptr_t>(container_state.compressed_arrays[NON_NULLS]);
					scan_state = make_uniq<CompressedArrayContainerScanState<NON_NULLS>>(
					    container_index, container_size, cardinality, segments, data_ptr);
				}
			} else {
				if (metadata.IsInverted()) {
					auto data_ptr = reinterpret_cast<data_ptr_t>(container_state.arrays[NULLS]);
					scan_state = make_uniq<ArrayContainerScanState<NULLS>>(container_index, container_size, cardinality,
					                                                       data_ptr);
				} else {
					auto data_ptr = reinterpret_cast<data_ptr_t>(container_state.arrays[NON_NULLS]);
					scan_state = make_uniq<ArrayContainerScanState<NON_NULLS>>(container_index, container_size,
					                                                           cardinality, data_ptr);
				}
			}
		}
		scan_state->Verify();
	}

#endif
	total_count += container_state.appended_count;
	bool has_nulls = container_state.null_count != 0;
	bool has_non_nulls = container_state.null_count != container_state.appended_count;
	if (has_nulls || container_state.uncompressed) {
		current_segment->stats.statistics.SetHasNullFast();
	}
	if (has_non_nulls || container_state.uncompressed) {
		current_segment->stats.statistics.SetHasNoNullFast();
	}
	current_segment->count += container_state.appended_count;
	container_state.Reset();
}

void RoaringCompressState::NextContainer() {
	FlushContainer();
	InitializeContainer();
}

void RoaringCompressState::HandleByte(RoaringCompressState &state, uint8_t array_index) {
	if (array_index == NumericLimits<uint8_t>::Maximum()) {
		HandleAllValid(state, 8);
	} else if (array_index == 0) {
		HandleNoneValid(state, 8);
	} else {
		HandleRaggedByte(state, array_index, 8);
	}
}

static inline void HandleBit(RoaringCompressState &state, bool bit_set) {
	auto &container_state = state.container_state;
	if (container_state.length && container_state.last_bit_set != bit_set) {
		container_state.Append(!container_state.last_bit_set, container_state.length);
		container_state.length = 0;
	}
	container_state.length += 1;
	container_state.last_bit_set = bit_set;
}

void RoaringCompressState::HandleRaggedByte(RoaringCompressState &state, uint8_t array_index, idx_t relevant_bits) {
	D_ASSERT(relevant_bits <= 8);
	for (idx_t i = 0; i < relevant_bits; i++) {
		const bool bit_set = array_index & (1 << i);
		HandleBit(state, bit_set);
	}
}

void RoaringCompressState::HandleAllValid(RoaringCompressState &state, idx_t amount) {
	auto &container_state = state.container_state;
	if (container_state.length && container_state.last_bit_set == false) {
		container_state.Append(!container_state.last_bit_set, container_state.length);
		container_state.length = 0;
	}
	container_state.length += amount;
	container_state.last_bit_set = true;
}

void RoaringCompressState::HandleNoneValid(RoaringCompressState &state, idx_t amount) {
	auto &container_state = state.container_state;
	if (container_state.length && container_state.last_bit_set == true) {
		container_state.Append(!container_state.last_bit_set, container_state.length);
		container_state.length = 0;
	}
	container_state.length += amount;
	container_state.last_bit_set = false;
}

idx_t RoaringCompressState::Count(RoaringCompressState &state) {
	auto &container_state = state.container_state;
	// How much is appended and waiting to be appended
	return container_state.appended_count + container_state.length;
}

void RoaringCompressState::Flush(RoaringCompressState &state) {
	state.NextContainer();
}
template <>
void RoaringCompressState::Compress<PhysicalType::BIT>(Vector &input, idx_t count) {
	auto &self = *this;
	RoaringStateAppender<RoaringCompressState>::AppendVector(self, input, count);
}
template <>
void RoaringCompressState::Compress<PhysicalType::BOOL>(Vector &input, idx_t count) {
	auto &self = *this;
	input.Flatten(count);
	const bool *src = FlatVector::GetData<bool>(input);

	Vector bitpacked_vector(LogicalType::UBIGINT, count);
	auto &bitpacked_vector_validity = FlatVector::Validity(bitpacked_vector);
	bitpacked_vector_validity.EnsureWritable();
	const auto dst = data_ptr_cast(bitpacked_vector_validity.GetData());

	const auto &validity = FlatVector::Validity(input);
	// Bitpack the booleans, so they can be fed through the current compression code, with the same format as a validity
	// mask.
	if (validity.AllValid()) {
		BitPackBooleans<true, true>(dst, src, count, &validity, &this->current_segment->stats.statistics);
	} else {
		BitPackBooleans<true, false>(dst, src, count, &validity, &this->current_segment->stats.statistics);
	}
	RoaringStateAppender<RoaringCompressState>::AppendVector(self, bitpacked_vector, count);
}

} // namespace roaring

} // namespace duckdb
