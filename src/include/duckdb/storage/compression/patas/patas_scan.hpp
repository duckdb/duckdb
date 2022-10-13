//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/storage/compression/chimp/chimp_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/chimp/chimp.hpp"
#include "duckdb/storage/compression/chimp/algorithm/chimp_utils.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/operator/subtract.hpp"

namespace duckdb {

using duckdb_chimp::SignificantBits;

template <class EXACT_TYPE>
struct PatasGroupState {
public:
	void Init(uint8_t *data) {
		byte_reader.SetStream(data);
	}

	void Reset() {
		index = 0;
	}

	void LoadByteCounts(uint8_t *bitpacked_data, idx_t block_count) {
		//! Unpack 'count' values of bitpacked data, unpacked per group of 32 values
		const auto value_count = block_count * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
		BitpackingPrimitives::UnPackBuffer<uint8_t>(byte_counts, bitpacked_data, value_count,
		                                            PatasPrimitives::BYTECOUNT_BITSIZE);
	}
	void LoadTrailingZeros(uint8_t *bitpacked_data, idx_t block_count) {
		const auto value_count = block_count * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
		BitpackingPrimitives::UnPackBuffer<uint8_t>(trailing_zeros, bitpacked_data, value_count,
		                                            SignificantBits<EXACT_TYPE>::size);
	}
	void LoadIndexDifferences(uint8_t *bitpacked_data, idx_t block_count) {
		const auto value_count = block_count * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
		BitpackingPrimitives::UnPackBuffer<uint8_t>(index_diffs, bitpacked_data, value_count,
		                                            PatasPrimitives::INDEX_BITSIZE);
	}

	void Scan(uint8_t *dest, idx_t count) {
		memcpy(dest, (void *)(values + index), sizeof(EXACT_TYPE) * count);
		index += count;
	}

	// FIXME: could optimize this to scan directly to the result if the subsequent scan would scan the entire group
	// anyways
	void LoadValues(idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			values[i] = patas::PatasDecompression<EXACT_TYPE>::DecompressValue(
			    byte_reader, i, byte_counts, trailing_zeros, (i != 0) * values[i - index_diffs[i]]);
		}
	}

public:
	idx_t index;
	uint8_t trailing_zeros[PatasPrimitives::PATAS_GROUP_SIZE];
	uint8_t byte_counts[PatasPrimitives::PATAS_GROUP_SIZE];
	uint8_t index_diffs[PatasPrimitives::PATAS_GROUP_SIZE];
	EXACT_TYPE values[PatasPrimitives::PATAS_GROUP_SIZE];

private:
	duckdb_chimp::ByteReader byte_reader;
};

template <class T>
struct PatasScanState : public SegmentScanState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	explicit PatasScanState(ColumnSegment &segment) : segment(segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);

		handle = buffer_manager.Pin(segment.block);
		auto dataptr = handle.Ptr();
		// ScanStates never exceed the boundaries of a Segment,
		// but are not guaranteed to start at the beginning of the Block
		auto start_of_data_segment = dataptr + segment.GetBlockOffset() + PatasPrimitives::HEADER_SIZE;
		auto metadata_offset = Load<uint32_t>(dataptr + segment.GetBlockOffset());
		metadata_ptr = dataptr + segment.GetBlockOffset() + metadata_offset;
		group_state.Init(start_of_data_segment);
		LoadGroup();
	}

	BufferHandle handle;
	data_ptr_t metadata_ptr;
	idx_t total_value_count = 0;
	PatasGroupState<EXACT_TYPE> group_state;

	ColumnSegment &segment;

	idx_t LeftInGroup() const {
		return PatasPrimitives::PATAS_GROUP_SIZE - (total_value_count % PatasPrimitives::PATAS_GROUP_SIZE);
	}

	bool GroupFinished() const {
		return (total_value_count % PatasPrimitives::PATAS_GROUP_SIZE) == 0;
	}

	// Scan up to a group boundary
	template <class EXACT_TYPE>
	void ScanGroup(EXACT_TYPE *values, idx_t group_size) {
		D_ASSERT(group_size <= PatasPrimitives::PATAS_GROUP_SIZE);
		D_ASSERT(group_size <= LeftInGroup());

		group_state.Scan((uint8_t *)values, group_size);

		total_value_count += group_size;
		if (GroupFinished() && total_value_count < segment.count) {
			LoadGroup();
		}
	}

	void LoadGroup() {
		group_state.Reset();

		// Load the offset indicating where a groups data starts
		metadata_ptr -= sizeof(uint32_t);
		auto data_byte_offset = Load<uint32_t>(metadata_ptr);
		D_ASSERT(data_byte_offset < Storage::BLOCK_SIZE);
		//  Only used for point queries
		(void)data_byte_offset;

		// Load how many blocks of bitpacked data we have
		metadata_ptr -= sizeof(uint8_t);
		auto bitpacked_block_count = Load<uint8_t>(metadata_ptr);
		D_ASSERT(bitpacked_block_count <=
		         PatasPrimitives::PATAS_GROUP_SIZE / BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE);

		const uint64_t trailing_zeros_bits =
		    (SignificantBits<EXACT_TYPE>::size * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) *
		    bitpacked_block_count;
		const uint64_t byte_counts_bits =
		    (PatasPrimitives::BYTECOUNT_BITSIZE * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) *
		    bitpacked_block_count;
		const uint64_t index_diff_bits =
		    (PatasPrimitives::INDEX_BITSIZE * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) *
		    bitpacked_block_count;
		metadata_ptr -= AlignValue(trailing_zeros_bits) / 8;
		// Unpack and store the trailing zeros for the entire group
		group_state.LoadTrailingZeros(metadata_ptr, bitpacked_block_count);

		metadata_ptr -= AlignValue(byte_counts_bits) / 8;
		// Unpack and store the byte counts for the entire group
		group_state.LoadByteCounts(metadata_ptr, bitpacked_block_count);

		metadata_ptr -= AlignValue(index_diff_bits) / 8;
		// Unpack and store the index differences for the entire group
		group_state.LoadIndexDifferences(metadata_ptr, bitpacked_block_count);

		idx_t group_size = MinValue((idx_t)PatasPrimitives::PATAS_GROUP_SIZE, (segment.count - total_value_count));
		group_state.LoadValues(group_size);
	}

public:
	//! Skip the next 'skip_count' values, we don't store the values
	// TODO: use the metadata to determine if we can skip a group
	void Skip(ColumnSegment &segment, idx_t skip_count) {
		using EXACT_TYPE = typename FloatingToExact<T>::type;
		EXACT_TYPE buffer[PatasPrimitives::PATAS_GROUP_SIZE];

		while (skip_count) {
			auto skip_size = std::min(skip_count, LeftInGroup());
			ScanGroup(buffer, skip_size);
			skip_count -= skip_size;
		}
	}
};

template <class T>
unique_ptr<SegmentScanState> PatasInitScan(ColumnSegment &segment) {
	auto result = make_unique_base<SegmentScanState, PatasScanState<T>>(segment);
	return result;
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void PatasScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                      idx_t result_offset) {
	using EXACT_TYPE = typename FloatingToExact<T>::type;
	auto &scan_state = (PatasScanState<T> &)*state.scan_state;

	T *result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto current_result_ptr = (EXACT_TYPE *)(result_data + result_offset);

	idx_t scanned = 0;

	while (scanned < scan_count) {
		const idx_t to_scan = MinValue(scan_count - scanned, scan_state.LeftInGroup());

		scan_state.template ScanGroup<EXACT_TYPE>(current_result_ptr + scanned, to_scan);
		scanned += to_scan;
	}
}

template <class T>
void PatasSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = (PatasScanState<T> &)*state.scan_state;
	scan_state.Skip(segment, skip_count);
}

template <class T>
void PatasScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	PatasScanPartial<T>(segment, state, scan_count, result, 0);
}

} // namespace duckdb
