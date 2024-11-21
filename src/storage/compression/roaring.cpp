#include "duckdb/storage/compression/roaring.hpp"

#include "duckdb/common/limits.hpp"
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

// Set all the bits from start (inclusive) to end (exclusive) to 0
void SetInvalidRange(ValidityMask &result, idx_t start, idx_t end) {
	if (end <= start) {
		throw InternalException("SetInvalidRange called with end (%d) <= start (%d)", end, start);
	}
	result.EnsureWritable();
	auto result_data = (validity_t *)result.GetData();

#ifdef DEBUG
	ValidityMask copy_for_verification(result.Capacity());
	copy_for_verification.EnsureWritable();
	for (idx_t i = 0;
	     i < AlignValue<idx_t, ValidityMask::BITS_PER_VALUE>(result.Capacity()) / ValidityMask::BITS_PER_VALUE; i++) {
		copy_for_verification.GetData()[i] = result.GetData()[i];
	}
#endif
	idx_t index = start;

	if ((index % ValidityMask::BITS_PER_VALUE) != 0) {
		// Adjust the high bits of the first entry

		// +======================================+
		// |xxxxxxxxxxxxxxxxxxxxxxxxx|            |
		// +======================================+
		//
		// 'x': bits to set to 0 in the result

		idx_t right_bits = index % ValidityMask::BITS_PER_VALUE;
		idx_t bits_to_set = ValidityMask::BITS_PER_VALUE - right_bits;
		idx_t left_bits = 0;
		if (index + bits_to_set > end) {
			// Limit the amount of bits to set
			left_bits = (index + bits_to_set) - end;
			bits_to_set = end - index;
		}

		// Prepare the mask
		validity_t mask = ValidityUncompressed::LOWER_MASKS[right_bits];
		if (left_bits) {
			// Mask off the part that we don't want to touch (if the range doesn't fully cover the bits)
			mask |= ValidityUncompressed::UPPER_MASKS[left_bits];
		}

		idx_t entry_idx = index / ValidityMask::BITS_PER_VALUE;
		index += bits_to_set;
		result_data[entry_idx] &= mask;
	}

	idx_t remaining_bits = end - index;
	idx_t full_entries = remaining_bits / ValidityMask::BITS_PER_VALUE;
	idx_t entry_idx = index / ValidityMask::BITS_PER_VALUE;
	// Set all the entries that are fully covered by the range to 0
	for (idx_t i = 0; i < full_entries; i++) {
		result_data[entry_idx + i] = (validity_t)0;
	}

	if ((remaining_bits % ValidityMask::BITS_PER_VALUE) != 0) {
		// The last entry touched by the range is only partially covered

		// +======================================+
		// |                         |xxxxxxxxxxxx|
		// +======================================+
		//
		// 'x': bits to set to 0 in the result

		idx_t bits_to_set = end % ValidityMask::BITS_PER_VALUE;
		idx_t left_bits = ValidityMask::BITS_PER_VALUE - bits_to_set;
		validity_t mask = ValidityUncompressed::UPPER_MASKS[left_bits];
		idx_t entry_idx = end / ValidityMask::BITS_PER_VALUE;
		result_data[entry_idx] &= mask;
	}

#ifdef DEBUG
	D_ASSERT(end <= result.Capacity());
	for (idx_t i = 0; i < result.Capacity(); i++) {
		if (i >= start && i < end) {
			D_ASSERT(!result.RowIsValidUnsafe(i));
		} else {
			// Ensure no others bits are touched by this method
			D_ASSERT(copy_for_verification.RowIsValidUnsafe(i) == result.RowIsValidUnsafe(i));
		}
	}
#endif
}

idx_t ContainerMetadata::GetDataSizeInBytes(idx_t container_size) const {
	if (IsUncompressed()) {
		return (container_size / ValidityMask::BITS_PER_VALUE) * sizeof(validity_t);
	}
	if (IsRun()) {
		auto number_of_runs = NumberOfRuns();
		if (number_of_runs >= COMPRESSED_RUN_THRESHOLD) {
			return COMPRESSED_SEGMENT_COUNT + (sizeof(uint8_t) * number_of_runs * 2);
		} else {
			return sizeof(RunContainerRLEPair) * number_of_runs;
		}
	} else {
		auto cardinality = Cardinality();
		if (cardinality >= COMPRESSED_ARRAY_THRESHOLD) {
			return COMPRESSED_SEGMENT_COUNT + (sizeof(uint8_t) * cardinality);
		} else {
			return sizeof(uint16_t) * cardinality;
		}
	}
}

ContainerMetadataCollection::ContainerMetadataCollection() {
}

void ContainerMetadataCollection::AddMetadata(ContainerMetadata metadata) {
	if (metadata.IsRun()) {
		AddRunContainer(metadata.NumberOfRuns(), metadata.IsInverted());
	} else if (metadata.IsUncompressed()) {
		AddBitsetContainer();
	} else {
		AddArrayContainer(metadata.Cardinality(), metadata.IsInverted());
	}
}

idx_t ContainerMetadataCollection::GetMetadataSizeForSegment() const {
	idx_t runs_count = GetRunContainerCount();
	idx_t arrays_count = GetArrayAndBitsetContainerCount();
	return GetMetadataSize(runs_count + arrays_count, runs_count, arrays_count);
}

idx_t ContainerMetadataCollection::GetMetadataSize(idx_t container_count, idx_t run_containers,
                                                   idx_t array_containers) const {
	idx_t types_size = BitpackingPrimitives::GetRequiredSize(container_count, CONTAINER_TYPE_BITWIDTH);
	idx_t runs_size = BitpackingPrimitives::GetRequiredSize(run_containers, RUN_CONTAINER_SIZE_BITWIDTH);
	idx_t arrays_size = sizeof(uint8_t) * array_containers;
	return types_size + runs_size + arrays_size;
}

idx_t ContainerMetadataCollection::GetRunContainerCount() const {
	return runs_in_segment;
}
idx_t ContainerMetadataCollection::GetArrayAndBitsetContainerCount() const {
	return arrays_in_segment;
}

void ContainerMetadataCollection::FlushSegment() {
	runs_in_segment = 0;
	count_in_segment = 0;
	arrays_in_segment = 0;
}

void ContainerMetadataCollection::Reset() {
	FlushSegment();
	container_type.clear();
	number_of_runs.clear();
	cardinality.clear();
}

// Write the metadata for the current segment
idx_t ContainerMetadataCollection::Serialize(data_ptr_t dest) const {
	// Element sizes (in bits) for written metadata
	// +======================================+
	// |mmmmmm|rrrrrr|aaaaaaa|                |
	// +======================================+
	//
	// m: 2: (1: is_run, 1: is_inverted)
	// r: 7: number_of_runs
	// a: 8: cardinality

	idx_t types_size = BitpackingPrimitives::GetRequiredSize(count_in_segment, CONTAINER_TYPE_BITWIDTH);
	idx_t runs_size = BitpackingPrimitives::GetRequiredSize(runs_in_segment, RUN_CONTAINER_SIZE_BITWIDTH);
	idx_t arrays_size = sizeof(uint8_t) * arrays_in_segment;

	idx_t types_offset = container_type.size() - count_in_segment;
	data_ptr_t types_data = (data_ptr_t)(container_type.data()); // NOLINT: c-style cast (for const)
	BitpackingPrimitives::PackBuffer<uint8_t>(dest, types_data + types_offset, count_in_segment,
	                                          CONTAINER_TYPE_BITWIDTH);
	dest += types_size;

	if (!number_of_runs.empty()) {
		idx_t runs_offset = number_of_runs.size() - runs_in_segment;
		data_ptr_t run_data = (data_ptr_t)(number_of_runs.data()); // NOLINT: c-style cast (for const)
		BitpackingPrimitives::PackBuffer<uint8_t>(dest, run_data + runs_offset, runs_in_segment,
		                                          RUN_CONTAINER_SIZE_BITWIDTH);
		dest += runs_size;
	}

	if (!cardinality.empty()) {
		idx_t arrays_offset = cardinality.size() - arrays_in_segment;
		data_ptr_t arrays_data = (data_ptr_t)(cardinality.data()); // NOLINT: c-style cast (for const)
		memcpy(dest, arrays_data + arrays_offset, sizeof(uint8_t) * arrays_in_segment);
	}
	return types_size + runs_size + arrays_size;
}

void ContainerMetadataCollection::Deserialize(data_ptr_t src, idx_t container_count) {
	container_type.resize(AlignValue<idx_t, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE>(container_count));
	count_in_segment = container_count;

	// Load the types of the containers
	idx_t types_size = BitpackingPrimitives::GetRequiredSize(container_type.size(), 2);
	BitpackingPrimitives::UnPackBuffer<uint8_t>(container_type.data(), src, container_count, 2, true);
	src += types_size;

	// Figure out how many are run containers
	idx_t runs_count = 0;
	for (idx_t i = 0; i < container_count; i++) {
		auto type = container_type[i];
		runs_count += ((type >> 1) & 1) == 1;
	}
	runs_in_segment = runs_count;
	number_of_runs.resize(AlignValue<idx_t, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE>(runs_count));
	cardinality.resize(container_count - runs_count);

	// Load the run containers
	if (runs_count) {
		idx_t runs_size = BitpackingPrimitives::GetRequiredSize(runs_count, RUN_CONTAINER_SIZE_BITWIDTH);
		BitpackingPrimitives::UnPackBuffer<uint8_t>(number_of_runs.data(), src, runs_count, RUN_CONTAINER_SIZE_BITWIDTH,
		                                            true);
		src += runs_size;
	}

	// Load the array/bitset containers
	if (!cardinality.empty()) {
		idx_t arrays_size = sizeof(uint8_t) * cardinality.size();
		arrays_in_segment = arrays_size;
		memcpy(cardinality.data(), src, arrays_size);
	}
}

void ContainerMetadataCollection::AddBitsetContainer() {
	AddContainerType(false, false);
	cardinality.push_back(MAX_ARRAY_IDX + 1);
	arrays_in_segment++;
	count_in_segment++;
}

void ContainerMetadataCollection::AddArrayContainer(idx_t amount, bool is_inverted) {
	AddContainerType(false, is_inverted);
	D_ASSERT(amount < MAX_ARRAY_IDX);
	cardinality.push_back(NumericCast<uint8_t>(amount));
	arrays_in_segment++;
	count_in_segment++;
}

void ContainerMetadataCollection::AddRunContainer(idx_t amount, bool is_inverted) {
	AddContainerType(true, is_inverted);
	D_ASSERT(amount < MAX_RUN_IDX);
	number_of_runs.push_back(NumericCast<uint8_t>(amount));
	runs_in_segment++;
	count_in_segment++;
}

void ContainerMetadataCollection::AddContainerType(bool is_run, bool is_inverted) {
	uint8_t type = 0;
	if (is_run) {
		type |= IS_RUN_FLAG;
	}
	if (is_inverted) {
		type |= IS_INVERTED_FLAG;
	}
	container_type.push_back(type);
}

struct ContainerMetadataCollectionScanner {
public:
	explicit ContainerMetadataCollectionScanner(ContainerMetadataCollection &collection) : collection(collection) {
	}

public:
	ContainerMetadata GetNext() {
		D_ASSERT(idx < collection.count_in_segment);
		auto type = collection.container_type[idx++];
		const bool is_inverted = (type & 1) == 1;
		const bool is_run = ((type >> 1) & 1) == 1;
		uint8_t amount;
		if (is_run) {
			amount = collection.number_of_runs[run_idx++];
		} else {
			amount = collection.cardinality[array_idx++];
		}
		if (is_run) {
			return ContainerMetadata::RunContainer(amount);
		}
		if (amount == MAX_ARRAY_IDX + 1) {
			return ContainerMetadata::BitsetContainer(amount);
		}
		return ContainerMetadata::ArrayContainer(amount, is_inverted);
	}

public:
	const ContainerMetadataCollection &collection;
	idx_t array_idx = 0;
	idx_t run_idx = 0;
	idx_t idx = 0;
};

struct ContainerCompressionState {
public:
	ContainerCompressionState() {
		Reset();
	}

public:
	void AppendVector(Vector &input, idx_t input_size, const std::function<void()> &on_full_container) {
		UnifiedVectorFormat unified;
		input.ToUnifiedFormat(input_size, unified);
		auto &validity = unified.validity;

		if (validity.AllValid()) {
			idx_t appended = 0;
			while (appended < input_size) {
				idx_t to_append = MinValue<idx_t>(ROARING_CONTAINER_SIZE - count, input_size - appended);
				Append(false, NumericCast<uint16_t>(to_append));
				if (IsFull()) {
					on_full_container();
				}
				appended += to_append;
			}
		} else {
			idx_t appended = 0;
			while (appended < input_size) {
				idx_t to_append = MinValue<idx_t>(ROARING_CONTAINER_SIZE - count, input_size - appended);
				for (idx_t i = 0; i < to_append; i++) {
					auto idx = unified.sel->get_index(appended + i);
					auto is_null = validity.RowIsValidUnsafe(idx);
					Append(!is_null);
				}
				if (IsFull()) {
					on_full_container();
				}
				appended += to_append;
			}
		}
	}

	void Append(bool null, uint16_t amount = 1) {
		if (uncompressed) {
			if (null) {
				ValidityMask mask(uncompressed, ROARING_CONTAINER_SIZE);
				SetInvalidRange(mask, count, count + amount);
			}
			count += amount;
			return;
		}

		// Adjust the run
		if (count && (null != last_is_null) && !null && run_idx < MAX_RUN_IDX) {
			if (run_idx < COMPRESSED_RUN_THRESHOLD) {
				auto &last_run = runs[run_idx];
				// End the last run
				last_run.length = (count - last_run.start) - 1;
			}
			compressed_runs[(run_idx * 2) + 1] = static_cast<uint8_t>(count % COMPRESSED_SEGMENT_SIZE);
			run_counts[count / COMPRESSED_SEGMENT_SIZE]++;
			run_idx++;
		} else if (null && (!count || null != last_is_null) && run_idx < MAX_RUN_IDX) {
			if (run_idx < COMPRESSED_RUN_THRESHOLD) {
				auto &current_run = runs[run_idx];
				// Initialize a new run
				current_run.start = count;
			}
			compressed_runs[(run_idx * 2) + 0] = static_cast<uint8_t>(count % COMPRESSED_SEGMENT_SIZE);
			run_counts[count / COMPRESSED_SEGMENT_SIZE]++;
		}

		// Add to the array
		auto &current_array_idx = array_idx[null];
		if (current_array_idx < MAX_ARRAY_IDX) {
			if (current_array_idx + amount <= MAX_ARRAY_IDX) {
				for (uint16_t i = 0; i < amount; i++) {
					compressed_arrays[null][current_array_idx + i] =
					    static_cast<uint8_t>((count + i) % COMPRESSED_SEGMENT_SIZE);
					array_counts[null][(count + i) / COMPRESSED_SEGMENT_SIZE]++;
				}
			}
			if (current_array_idx + amount < COMPRESSED_ARRAY_THRESHOLD) {
				for (uint16_t i = 0; i < amount; i++) {
					arrays[null][current_array_idx + i] = count + i;
				}
			}
			current_array_idx += amount;
		}

		last_is_null = null;
		null_count += null * amount;
		count += amount;
	}

	bool IsFull() const {
		return count == ROARING_CONTAINER_SIZE;
	}

	void OverrideArray(data_ptr_t destination, bool nulls, idx_t count) {
		if (count >= COMPRESSED_ARRAY_THRESHOLD) {
			memset(destination, 0, sizeof(uint8_t) * COMPRESSED_SEGMENT_COUNT);
			array_counts[nulls] = reinterpret_cast<uint8_t *>(destination);
			destination += sizeof(uint8_t) * COMPRESSED_SEGMENT_COUNT;
			compressed_arrays[nulls] = reinterpret_cast<uint8_t *>(destination);
		} else {
			arrays[nulls] = reinterpret_cast<uint16_t *>(destination);
		}
	}

	void OverrideRun(data_ptr_t destination, idx_t count) {
		if (count >= COMPRESSED_RUN_THRESHOLD) {
			memset(destination, 0, sizeof(uint8_t) * COMPRESSED_SEGMENT_COUNT);
			run_counts = reinterpret_cast<uint8_t *>(destination);
			destination += sizeof(uint8_t) * COMPRESSED_SEGMENT_COUNT;
			compressed_runs = reinterpret_cast<uint8_t *>(destination);
		} else {
			runs = reinterpret_cast<RunContainerRLEPair *>(destination);
		}
	}

	void OverrideUncompressed(data_ptr_t destination) {
		uncompressed = reinterpret_cast<validity_t *>(destination);
	}

	void Finalize() {
		D_ASSERT(!finalized);
		if (count && last_is_null && run_idx < MAX_RUN_IDX) {
			if (run_idx < COMPRESSED_RUN_THRESHOLD) {
				auto &last_run = runs[run_idx];
				// End the last run
				last_run.length = (count - last_run.start);
			}
			compressed_runs[(run_idx * 2) + 1] = static_cast<uint8_t>(count % COMPRESSED_SEGMENT_SIZE);
			if (count != ROARING_CONTAINER_SIZE) {
				run_counts[count / COMPRESSED_SEGMENT_SIZE]++;
			}
			run_idx++;
		}
		finalized = true;
	}

	ContainerMetadata GetResult() {
		if (uncompressed) {
			return ContainerMetadata::BitsetContainer(count);
		}
		D_ASSERT(finalized);
		const bool can_use_null_array = array_idx[NULLS] < MAX_ARRAY_IDX;
		const bool can_use_non_null_array = array_idx[NON_NULLS] < MAX_ARRAY_IDX;

		const bool can_use_run = run_idx < MAX_RUN_IDX;

		const bool can_use_array = can_use_null_array || can_use_non_null_array;
		if (!can_use_array && !can_use_run) {
			// Can not efficiently encode at all, write it as bitset
			return ContainerMetadata::BitsetContainer(count);
		}
		uint16_t null_array_cost = array_idx[NULLS] < COMPRESSED_ARRAY_THRESHOLD
		                               ? array_idx[NULLS] * sizeof(uint16_t)
		                               : COMPRESSED_SEGMENT_COUNT + (array_idx[NULLS] * sizeof(uint8_t));
		uint16_t non_null_array_cost = array_idx[NON_NULLS] < COMPRESSED_ARRAY_THRESHOLD
		                                   ? array_idx[NON_NULLS] * sizeof(uint16_t)
		                                   : COMPRESSED_SEGMENT_COUNT + (array_idx[NON_NULLS] * sizeof(uint8_t));

		uint16_t lowest_array_cost = MinValue<uint16_t>(null_array_cost, non_null_array_cost);
		uint16_t lowest_run_cost = run_idx < COMPRESSED_RUN_THRESHOLD
		                               ? run_idx * sizeof(uint32_t)
		                               : COMPRESSED_SEGMENT_COUNT + (run_idx * sizeof(uint16_t));
		uint16_t bitset_cost =
		    (AlignValue<uint16_t, ValidityMask::BITS_PER_VALUE>(count) / ValidityMask::BITS_PER_VALUE) *
		    sizeof(validity_t);
		if (MinValue<uint16_t>(lowest_array_cost, lowest_run_cost) > bitset_cost) {
			// The amount of values is too small, better off using bitset
			// we can detect this at decompression because we know how many values are left
			return ContainerMetadata::BitsetContainer(count);
		}

		if (lowest_array_cost <= lowest_run_cost) {
			if (array_idx[NULLS] <= array_idx[NON_NULLS]) {
				return ContainerMetadata::ArrayContainer(array_idx[NULLS], NULLS);
			} else {
				return ContainerMetadata::ArrayContainer(array_idx[NON_NULLS], NON_NULLS);
			}
		} else {
			return ContainerMetadata::RunContainer(run_idx);
		}
	}

	void Reset() {
		count = 0;
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

public:
	//! Total amount of values covered by the container
	uint16_t count = 0;
	//! How many of the total are null
	uint16_t null_count = 0;
	bool last_is_null = false;

	RunContainerRLEPair *runs;
	uint8_t *compressed_runs;
	uint8_t *compressed_arrays[2];
	uint16_t *arrays[2];

	//! The runs (for sequential nulls)
	RunContainerRLEPair base_runs[COMPRESSED_RUN_THRESHOLD];
	//! The indices (for nulls | non-nulls)
	uint16_t base_arrays[2][COMPRESSED_ARRAY_THRESHOLD];

	uint16_t run_idx;
	uint16_t array_idx[2];

	uint8_t *array_counts[2];
	uint8_t *run_counts;

	uint8_t base_compressed_arrays[2][MAX_ARRAY_IDX];
	uint8_t base_compressed_runs[MAX_RUN_IDX * 2];
	uint8_t base_array_counts[2][COMPRESSED_SEGMENT_COUNT];
	uint8_t base_run_counts[COMPRESSED_SEGMENT_COUNT];

	validity_t *uncompressed = nullptr;
	//! Whether the state has been finalized
	bool finalized = false;
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct RoaringAnalyzeState : public AnalyzeState {
public:
	explicit RoaringAnalyzeState(const CompressionInfo &info) : AnalyzeState(info) {};

public:
	bool HasEnoughSpaceInSegment(idx_t required_space) {
		D_ASSERT(space_used <= info.GetBlockSize());
		idx_t remaining_space = info.GetBlockSize() - space_used;
		if (required_space > remaining_space) {
			return false;
		}
		return true;
	}

	void FlushSegment() {
		if (!current_count) {
			D_ASSERT(!space_used);
			return;
		}
		metadata_collection.FlushSegment();
		total_size += space_used;
		space_used = 0;
		current_count = 0;
		segment_count++;
	}

	void FlushContainer() {
		if (!container_state.count) {
			return;
		}
		container_state.Finalize();
		auto metadata = container_state.GetResult();
		idx_t runs_count = metadata_collection.GetRunContainerCount();
		idx_t arrays_count = metadata_collection.GetArrayAndBitsetContainerCount();

#ifdef DEBUG
		idx_t container_index = 0;
		idx_t container_size = container_state.count;
		if (!metadata.IsUncompressed()) {
			unique_ptr<ContainerScanState> scan_state;
			if (metadata.IsRun()) {
				D_ASSERT(metadata.IsInverted());
				auto number_of_runs = metadata.NumberOfRuns();
				if (number_of_runs >= COMPRESSED_RUN_THRESHOLD) {
					auto segments = container_state.run_counts;
					auto data_ptr = container_state.compressed_runs;
					scan_state = make_uniq<CompressedRunContainerScanState>(container_index, container_size,
					                                                        number_of_runs, segments, data_ptr);
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
						scan_state = make_uniq<CompressedArrayContainerScanState<NULLS>>(
						    container_index, container_size, cardinality, segments, data_ptr);
					} else {
						auto segments = reinterpret_cast<data_ptr_t>(container_state.array_counts[NON_NULLS]);
						auto data_ptr = reinterpret_cast<data_ptr_t>(container_state.compressed_arrays[NON_NULLS]);
						scan_state = make_uniq<CompressedArrayContainerScanState<NON_NULLS>>(
						    container_index, container_size, cardinality, segments, data_ptr);
					}
				} else {
					if (metadata.IsInverted()) {
						auto data_ptr = reinterpret_cast<data_ptr_t>(container_state.arrays[NULLS]);
						scan_state = make_uniq<ArrayContainerScanState<NULLS>>(container_index, container_size,
						                                                       cardinality, data_ptr);
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

		if (metadata.IsRun()) {
			runs_count++;
		} else {
			arrays_count++;
		}

		idx_t required_space = metadata_collection.GetMetadataSize(runs_count + arrays_count, runs_count, arrays_count);

		required_space += metadata.GetDataSizeInBytes(container_state.count);
		if (!HasEnoughSpaceInSegment(required_space)) {
			FlushSegment();
		}
		container_metadata.push_back(metadata);
		metadata_collection.AddMetadata(metadata);
		space_used += required_space;
		current_count += container_state.count;
		container_state.Reset();
	}

	void Analyze(Vector &input, idx_t count) {
		auto &self = *this;
		container_state.AppendVector(input, count, [&self]() { self.FlushContainer(); });

		this->count += count;
	}

public:
	ContainerCompressionState container_state;
	//! The space used by the current segment
	idx_t space_used = 0;
	//! The total amount of segments to write
	idx_t segment_count = 0;
	//! The amount of values in the current segment;
	idx_t current_count = 0;
	//! The total amount of data to serialize
	idx_t count = 0;

	//! The total amount of bytes used to compress the whole segment
	idx_t total_size = 0;
	//! The container metadata, determining the type of each container to use during compression
	ContainerMetadataCollection metadata_collection;
	vector<ContainerMetadata> container_metadata;
};

unique_ptr<AnalyzeState> RoaringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	CompressionInfo info(col_data.GetBlockManager().GetBlockSize());
	auto state = make_uniq<RoaringAnalyzeState>(info);

	return std::move(state);
}

bool RoaringAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = state.Cast<RoaringAnalyzeState>();
	analyze_state.Analyze(input, count);
	return true;
}

idx_t RoaringFinalAnalyze(AnalyzeState &state) {
	auto &roaring_state = state.Cast<RoaringAnalyzeState>();
	roaring_state.FlushContainer();
	roaring_state.FlushSegment();
	return roaring_state.total_size;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct RoaringCompressState : public CompressionState {
public:
	explicit RoaringCompressState(ColumnDataCheckpointer &checkpointer, unique_ptr<AnalyzeState> analyze_state_p)
	    : CompressionState(analyze_state_p->info), owned_analyze_state(std::move(analyze_state_p)),
	      analyze_state(owned_analyze_state->Cast<RoaringAnalyzeState>()),
	      container_state(analyze_state.container_state), container_metadata(analyze_state.container_metadata),
	      checkpointer(checkpointer),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_ROARING)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);
		count = 0;
		InitializeContainer();
	}

public:
	inline idx_t GetContainerIndex() {
		idx_t index = count / ROARING_CONTAINER_SIZE;
		return index;
	}

	idx_t GetRemainingSpace() {
		return static_cast<idx_t>(metadata_ptr - data_ptr);
	}

	bool CanStore(idx_t container_size, const ContainerMetadata &metadata) {
		idx_t required_space = 0;
		if (metadata.IsUncompressed()) {
			// Account for the alignment we might need for this container
			required_space +=
			    (AlignValue<idx_t>(reinterpret_cast<idx_t>(data_ptr))) - reinterpret_cast<idx_t>(data_ptr);
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

	void InitializeContainer() {
		if (count == analyze_state.count) {
			// No more containers left
			return;
		}
		auto container_index = GetContainerIndex();
		D_ASSERT(container_index < container_metadata.size());
		auto metadata = container_metadata[container_index];

		idx_t container_size = AlignValue<idx_t, ValidityMask::BITS_PER_VALUE>(
		    MinValue<idx_t>(analyze_state.count - count, ROARING_CONTAINER_SIZE));
		if (!CanStore(container_size, metadata)) {
			idx_t row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
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

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();

		auto compressed_segment =
		    ColumnSegment::CreateTransientSegment(db, type, row_start, info.GetBlockSize(), info.GetBlockSize());
		compressed_segment->function = function;
		current_segment = std::move(compressed_segment);

		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);
		data_ptr = handle.Ptr();
		data_ptr += sizeof(idx_t);
		metadata_ptr = handle.Ptr() + info.GetBlockSize();
	}

	void FlushSegment() {
		auto &state = checkpointer.GetCheckpointState();
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
		idx_t data_size = NumericCast<idx_t>(data_ptr - base_ptr);
		data_size = AlignValue(data_size);

		// Size of the 'm' part
		idx_t metadata_size = metadata_collection.GetMetadataSizeForSegment();

		if (current_segment->count.load() == 0) {
			D_ASSERT(metadata_size == 0);
			return;
		}

		idx_t serialized_metadata_size = metadata_collection.Serialize(data_ptr);
		metadata_collection.FlushSegment();
		(void)serialized_metadata_size;
		D_ASSERT(metadata_size == serialized_metadata_size);
		idx_t metadata_start = static_cast<idx_t>(data_ptr - base_ptr);
		Store<idx_t>(metadata_start, handle.Ptr());
		idx_t total_segment_size = sizeof(idx_t) + data_size + metadata_size;
		state.FlushSegment(std::move(current_segment), std::move(handle), total_segment_size);
	}

	void Finalize() {
		FlushContainer();
		FlushSegment();
		current_segment.reset();
	}

	void FlushContainer() {
		if (!container_state.count) {
			return;
		}
		container_state.Finalize();
#ifdef DEBUG
		auto container_index = GetContainerIndex();
		auto analyzed_metadata = container_metadata[container_index];
		auto actual_metadata = container_state.GetResult();
		D_ASSERT(analyzed_metadata == actual_metadata);
#endif
		count += container_state.count;
		bool has_nulls = container_state.null_count != 0;
		bool has_non_nulls = container_state.null_count != container_state.count;
		if (has_nulls || container_state.uncompressed) {
			current_segment->stats.statistics.SetHasNullFast();
		}
		if (has_non_nulls || container_state.uncompressed) {
			current_segment->stats.statistics.SetHasNoNullFast();
		}
		current_segment->count += container_state.count;
		container_state.Reset();
	}

	void NextContainer() {
		FlushContainer();
		InitializeContainer();
	}

	void Compress(Vector &input, idx_t count) {
		auto &self = *this;
		container_state.AppendVector(input, count, [&self]() { self.NextContainer(); });
	}

public:
	unique_ptr<AnalyzeState> owned_analyze_state;
	RoaringAnalyzeState &analyze_state;

	ContainerCompressionState &container_state;
	ContainerMetadataCollection metadata_collection;
	vector<ContainerMetadata> &container_metadata;

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;

	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;
	// Ptr to next free spot for storing
	data_ptr_t metadata_ptr;
	//! The amount of values already compressed
	idx_t count = 0;
};

unique_ptr<CompressionState> RoaringInitCompression(ColumnDataCheckpointer &checkpointer,
                                                    unique_ptr<AnalyzeState> state) {
	return make_uniq<RoaringCompressState>(checkpointer, std::move(state));
}

void RoaringCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = state_p.Cast<RoaringCompressState>();
	state.Compress(scan_vector, count);
}

void RoaringFinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<RoaringCompressState>();
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//

ContainerSegmentScan::ContainerSegmentScan(data_ptr_t data)
    : segments(reinterpret_cast<uint8_t *>(data)), index(0), count(0) {
}

// Returns the base of the current segment, forwarding the index if the segment is depleted of values
uint16_t ContainerSegmentScan::operator++(int) {
	while (index < COMPRESSED_SEGMENT_COUNT && count >= segments[index]) {
		count = 0;
		index++;
	}
	count++;

	// index == COMPRESSED_SEGMENT_COUNT is allowed for runs, as the last run could end at ROARING_CONTAINER_SIZE
	D_ASSERT(index <= COMPRESSED_SEGMENT_COUNT);
	if (index < COMPRESSED_SEGMENT_COUNT) {
		D_ASSERT(segments[index] != 0);
	}
	uint16_t base = static_cast<uint16_t>(index) * COMPRESSED_SEGMENT_SIZE;
	return base;
}

BitsetContainerScanState::BitsetContainerScanState(idx_t container_index, idx_t count, validity_t *bitset)
    : ContainerScanState(container_index, count), bitset(bitset) {
}

void BitsetContainerScanState::ScanPartial(Vector &result, idx_t result_offset, idx_t to_scan) {
	if (!result_offset && (to_scan % ValidityMask::BITS_PER_VALUE) == 0 &&
	    (scanned_count % ValidityMask::BITS_PER_VALUE) == 0) {
		ValidityUncompressed::AlignedScan(reinterpret_cast<data_ptr_t>(bitset), scanned_count, result, to_scan);
	} else {
		ValidityUncompressed::UnalignedScan(reinterpret_cast<data_ptr_t>(bitset), container_size, scanned_count, result,
		                                    result_offset, to_scan);
	}
	scanned_count += to_scan;
}

void BitsetContainerScanState::Skip(idx_t to_skip) {
	// NO OP: we only need to forward scanned_count
	scanned_count += to_skip;
}

void BitsetContainerScanState::Verify() const {
	// uncompressed, nothing to verify
	return;
}

struct RoaringScanState : public SegmentScanState {
public:
	explicit RoaringScanState(ColumnSegment &segment) : segment(segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);
		auto base_ptr = handle.Ptr() + segment.GetBlockOffset();
		data_ptr = base_ptr + sizeof(idx_t);

		// Deserialize the container metadata for this segment
		auto metadata_offset = Load<idx_t>(base_ptr);
		auto metadata_ptr = data_ptr + metadata_offset;

		auto segment_count = segment.count.load();
		auto container_count = segment_count / ROARING_CONTAINER_SIZE;
		if (segment_count % ROARING_CONTAINER_SIZE != 0) {
			container_count++;
		}
		metadata_collection.Deserialize(metadata_ptr, container_count);
		ContainerMetadataCollectionScanner scanner(metadata_collection);
		data_start_position.reserve(container_count);
		idx_t position = 0;
		for (idx_t i = 0; i < container_count; i++) {
			auto metadata = scanner.GetNext();
			container_metadata.push_back(metadata);
			if (metadata.IsUncompressed()) {
				position = AlignValue<idx_t>(position);
			}
			data_start_position.push_back(position);
			position += SkipVector(metadata);
		}
	}

public:
	idx_t SkipVector(const ContainerMetadata &metadata) {
		// NOTE: this doesn't care about smaller containers, since only the last container can be smaller
		return metadata.GetDataSizeInBytes(ROARING_CONTAINER_SIZE);
	}

	bool UseContainerStateCache(idx_t container_index, idx_t internal_offset) {
		if (!current_container) {
			// No container loaded yet
			return false;
		}
		if (current_container->container_index != container_index) {
			// Not the same container
			return false;
		}
		if (current_container->scanned_count != internal_offset) {
			// Not the same scan offset
			return false;
		}
		return true;
	}

	ContainerMetadata GetContainerMetadata(idx_t container_index) {
		return container_metadata[container_index];
	}

	data_ptr_t GetStartOfContainerData(idx_t container_index) {
		return data_ptr + data_start_position[container_index];
	}

	ContainerScanState &LoadContainer(idx_t container_index, idx_t internal_offset) {
		if (UseContainerStateCache(container_index, internal_offset)) {
			return *current_container;
		}
		auto metadata = GetContainerMetadata(container_index);
		auto data_ptr = GetStartOfContainerData(container_index);

		auto segment_count = segment.count.load();
		auto start_of_container = container_index * ROARING_CONTAINER_SIZE;
		auto container_size = MinValue<idx_t>(segment_count - start_of_container, ROARING_CONTAINER_SIZE);
		if (metadata.IsUncompressed()) {
			current_container = make_uniq<BitsetContainerScanState>(container_index, container_size,
			                                                        reinterpret_cast<validity_t *>(data_ptr));
		} else if (metadata.IsRun()) {
			D_ASSERT(metadata.IsInverted());
			auto number_of_runs = metadata.NumberOfRuns();
			if (number_of_runs >= COMPRESSED_RUN_THRESHOLD) {
				auto segments = data_ptr;
				data_ptr = segments + COMPRESSED_SEGMENT_COUNT;
				current_container = make_uniq<CompressedRunContainerScanState>(container_index, container_size,
				                                                               number_of_runs, segments, data_ptr);
			} else {
				current_container =
				    make_uniq<RunContainerScanState>(container_index, container_size, number_of_runs, data_ptr);
			}
		} else {
			auto cardinality = metadata.Cardinality();
			if (cardinality >= COMPRESSED_ARRAY_THRESHOLD) {
				auto segments = data_ptr;
				data_ptr = segments + COMPRESSED_SEGMENT_COUNT;
				if (metadata.IsInverted()) {
					current_container = make_uniq<CompressedArrayContainerScanState<NULLS>>(
					    container_index, container_size, cardinality, segments, data_ptr);
				} else {
					current_container = make_uniq<CompressedArrayContainerScanState<NON_NULLS>>(
					    container_index, container_size, cardinality, segments, data_ptr);
				}
			} else {
				if (metadata.IsInverted()) {
					current_container = make_uniq<ArrayContainerScanState<NULLS>>(container_index, container_size,
					                                                              cardinality, data_ptr);
				} else {
					current_container = make_uniq<ArrayContainerScanState<NON_NULLS>>(container_index, container_size,
					                                                                  cardinality, data_ptr);
				}
			}
		}

		current_container->Verify();

		auto &scan_state = *current_container;
		if (internal_offset) {
			Skip(scan_state, internal_offset);
		}
		return *current_container;
	}

	void ScanInternal(ContainerScanState &scan_state, idx_t to_scan, Vector &result, idx_t offset) {
		scan_state.ScanPartial(result, offset, to_scan);
#ifdef DEBUG
		auto &result_mask = FlatVector::Validity(result);
#endif
	}

	idx_t GetContainerIndex(idx_t start_index, idx_t &offset) {
		idx_t container_index = start_index / ROARING_CONTAINER_SIZE;
		offset = start_index % ROARING_CONTAINER_SIZE;
		return container_index;
	}

	void ScanPartial(idx_t start_idx, Vector &result, idx_t offset, idx_t count) {
		result.Flatten(count);
		idx_t remaining = count;
		idx_t scanned = 0;
		while (remaining) {
			idx_t internal_offset;
			idx_t container_idx = GetContainerIndex(start_idx + scanned, internal_offset);
			auto &scan_state = LoadContainer(container_idx, internal_offset);
			idx_t remaining_in_container = scan_state.container_size - scan_state.scanned_count;
			idx_t to_scan = MinValue<idx_t>(remaining, remaining_in_container);
			ScanInternal(scan_state, to_scan, result, offset + scanned);
			remaining -= to_scan;
			scanned += to_scan;
		}
		D_ASSERT(scanned == count);
	}

	void Skip(ContainerScanState &scan_state, idx_t skip_count) {
		D_ASSERT(scan_state.scanned_count + skip_count <= scan_state.container_size);
		if (scan_state.scanned_count + skip_count == scan_state.container_size) {
			scan_state.scanned_count = scan_state.container_size;
			// This skips all remaining values covered by this container
			return;
		}
		scan_state.Skip(skip_count);
	}

public:
	BufferHandle handle;
	ColumnSegment &segment;
	unique_ptr<ContainerScanState> current_container;
	data_ptr_t data_ptr;
	ContainerMetadataCollection metadata_collection;
	vector<ContainerMetadata> container_metadata;
	vector<idx_t> data_start_position;
};

unique_ptr<SegmentScanState> RoaringInitScan(ColumnSegment &segment) {
	auto result = make_uniq<RoaringScanState>(segment);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void RoaringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                        idx_t result_offset) {
	auto &scan_state = state.scan_state->Cast<RoaringScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	scan_state.ScanPartial(start, result, result_offset, scan_count);
}

void RoaringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	RoaringScanPartial(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void RoaringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	RoaringScanState scan_state(segment);

	idx_t internal_offset;
	idx_t container_idx = scan_state.GetContainerIndex(static_cast<idx_t>(row_id), internal_offset);
	auto &container_state = scan_state.LoadContainer(container_idx, internal_offset);

	scan_state.ScanInternal(container_state, 1, result, result_idx);
}

void RoaringSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	// NO OP
	// We skip inside scan instead, if the container boundary gets crossed we can avoid a bunch of work anyways
	return;
}

} // namespace roaring

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction GetCompressionFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_ROARING, data_type, roaring::RoaringInitAnalyze,
	                           roaring::RoaringAnalyze, roaring::RoaringFinalAnalyze, roaring::RoaringInitCompression,
	                           roaring::RoaringCompress, roaring::RoaringFinalizeCompress, roaring::RoaringInitScan,
	                           roaring::RoaringScan, roaring::RoaringScanPartial, roaring::RoaringFetchRow,
	                           roaring::RoaringSkip);
}

CompressionFunction RoaringCompressionFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return GetCompressionFunction(type);
	default:
		throw InternalException("Unsupported type for Roaring");
	}
}

bool RoaringCompressionFun::TypeIsSupported(const PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::BIT:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
