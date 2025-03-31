#include "duckdb/storage/compression/roaring/roaring.hpp"

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

/*
Data layout per segment:

                Offsets
+--------------------------------------+
|   +------------------------------+   |
|   |   uint64_t metadata_offset   |   |
|   +------------------------------+   |
+--------------------------------------+

                [Container Data]+
+------------------------------------------------------+
|              Uncompressed Array Container            |
|   +----------------------------------------------+   |
|   |   uint16_t values[]                          |   |
|   +----------------------------------------------+   |
|                                                      |
|               Compressed Array Container             |
|   +----------------------------------------------+   |
|   |   uint8_t counts[COMPRESSED_SEGMENT_COUNT]   |   |
|   |   uint8_t values[]                           |   |
|   +----------------------------------------------+   |
+------------------------------------------------------+
|                    Bitset Container                  |
|   +----------------------------------------------+   |
|   |   uint32_t page_offset[]                     |   |
|   |   uint64_t uncompressed_size[]               |   |
|   |   uint64_t compressed_size[]                 |   |
|   +----------------------------------------------+   |
|                                                      |
+------------------------------------------------------+
|               Uncompressed Run Container             |
|   +----------------------------------------------+   |
|   |   (uint16_t, uint16_t) runs[]                |   |
|   +----------------------------------------------+   |
|                                                      |
|                Compressed Run Container              |
|   +----------------------------------------------+   |
|   |   uint8_t counts[COMPRESSED_SEGMENT_COUNT]   |   |
|   |   (uint8_t, uint8_t) runs[]                  |   |
|   +----------------------------------------------+   |
+------------------------------------------------------+

              Container Metadata
+--------------------------------------------+
|             Container Types                |
|   +------------------------------------+   |
|   |   uint8_t:1 is_run                 |   |
|   |   uint8_t:1 is_inverted            |   |
|   +------------------------------------+   |
|                                            |
|            Run Container Sizes             |
|   +------------------------------------+   |
|   |   uint8_t:7 size                   |   |
|   +------------------------------------+   |
|                                            |
|        Array/Bitset Container Sizes        |
|   +------------------------------------+   |
|   |   uint8_t:8 size                   |   |
|   +------------------------------------+   |
+--------------------------------------------+
*/

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

unique_ptr<AnalyzeState> RoaringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	// check if the storage version we are writing to supports roaring
	auto &storage = col_data.GetStorageManager();
	if (storage.GetStorageVersion() < 4) {
		// compatibility mode with old versions - disable roaring
		return nullptr;
	}
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

	constexpr const double ROARING_COMPRESS_PENALTY = 2.0;
	return LossyNumericCast<idx_t>((double)roaring_state.total_size * ROARING_COMPRESS_PENALTY);
}

unique_ptr<CompressionState> RoaringInitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                    unique_ptr<AnalyzeState> state) {
	return make_uniq<RoaringCompressState>(checkpoint_data, std::move(state));
}

void RoaringCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = state_p.Cast<RoaringCompressState>();
	state.Compress(scan_vector, count);
}

void RoaringFinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<RoaringCompressState>();
	state.Finalize();
}

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

unique_ptr<CompressedSegmentState> RoaringInitSegment(ColumnSegment &segment, block_id_t block_id,
                                                      optional_ptr<ColumnSegmentState> segment_state) {
	// 'ValidityInitSegment' is used normally, which memsets the page to all bits set.
	return nullptr;
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
	                           roaring::RoaringSkip, roaring::RoaringInitSegment);
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
