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

static unsafe_unique_array<BitmaskTableEntry> CreateBitmaskTable() {
	unsafe_unique_array<BitmaskTableEntry> result;
	result = make_unsafe_uniq_array_uninitialized<BitmaskTableEntry>(NumericLimits<uint8_t>::Maximum() + 1);

	for (uint16_t val = 0; val < NumericLimits<uint8_t>::Maximum() + 1; val++) {
		bool previous_bit;
		auto &entry = result[val];
		entry.valid_count = 0;
		entry.run_count = 0;
		for (uint8_t i = 0; i < 8; i++) {
			const bool bit_set = val & (1 << i);
			if (!i) {
				entry.first_bit_set = bit_set;
			} else if (i == 7) {
				entry.last_bit_set = bit_set;
			}
			entry.valid_count += bit_set;

			if (i && !bit_set && previous_bit == true) {
				entry.run_count++;
			}
			previous_bit = bit_set;
		}
	}

	return result;
}

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
RoaringAnalyzeState::RoaringAnalyzeState(const CompressionInfo &info)
    : AnalyzeState(info), bitmask_table(CreateBitmaskTable()) {
}

void RoaringAnalyzeState::HandleByte(RoaringAnalyzeState &state, uint8_t array_index) {
	auto bit_info = state.bitmask_table[static_cast<uint8_t>(array_index)];

	state.run_count +=
	    bit_info.run_count + (bit_info.first_bit_set == false && (!state.count || state.last_bit_set == true));
	state.one_count += bit_info.valid_count;
	D_ASSERT(bit_info.valid_count <= 8);
	state.zero_count += 8 - bit_info.valid_count;
	state.last_bit_set = bit_info.last_bit_set;
	state.count += 8;
}

static inline void HandleBit(RoaringAnalyzeState &state, bool bit_set) {
	if (!bit_set && (state.count == 0 || state.last_bit_set == true)) {
		state.run_count++;
	}
	state.one_count += bit_set;
	state.zero_count += !bit_set;
	state.last_bit_set = bit_set;
	state.count++;
}

void RoaringAnalyzeState::HandleRaggedByte(RoaringAnalyzeState &state, uint8_t array_index, idx_t relevant_bits) {
	D_ASSERT(relevant_bits <= 8);
	for (idx_t i = 0; i < relevant_bits; i++) {
		const bool bit_set = array_index & (1 << i);
		HandleBit(state, bit_set);
	}
}

void RoaringAnalyzeState::HandleAllValid(RoaringAnalyzeState &state, idx_t amount) {
	state.one_count += amount;
	state.last_bit_set = true;
	state.count += amount;
}

void RoaringAnalyzeState::HandleNoneValid(RoaringAnalyzeState &state, idx_t amount) {
	if (!state.count || (state.last_bit_set != false)) {
		state.run_count++;
	}
	state.zero_count += amount;
	state.last_bit_set = false;
	state.count += amount;
}

idx_t RoaringAnalyzeState::Count(RoaringAnalyzeState &state) {
	return state.count;
}

void RoaringAnalyzeState::Flush(RoaringAnalyzeState &state) {
	state.FlushContainer();
}

bool RoaringAnalyzeState::HasEnoughSpaceInSegment(idx_t required_space) {
	auto space_used = data_size + metadata_size;
	D_ASSERT(space_used <= info.GetBlockSize());
	idx_t remaining_space = info.GetBlockSize() - space_used;
	if (required_space > remaining_space) {
		return false;
	}
	return true;
}

void RoaringAnalyzeState::FlushSegment() {
	auto space_used = data_size + metadata_size;
	if (!current_count) {
		D_ASSERT(!space_used);
		return;
	}
	metadata_collection.FlushSegment();
	total_size += space_used;
	data_size = 0;
	metadata_size = 0;
	current_count = 0;
	segment_count++;
}

ContainerMetadata RoaringAnalyzeState::GetResult() {
	return ContainerMetadata::CreateMetadata(count, zero_count, one_count, run_count);
}

void RoaringAnalyzeState::FlushContainer() {
	if (!count) {
		return;
	}
	auto metadata = GetResult();
	idx_t runs_count = metadata_collection.GetRunContainerCount();
	idx_t arrays_count = metadata_collection.GetArrayAndBitsetContainerCount();

	if (metadata.IsRun()) {
		runs_count++;
	} else {
		arrays_count++;
	}

	metadata_size = metadata_collection.GetMetadataSize(runs_count + arrays_count, runs_count, arrays_count);

	data_size += metadata.GetDataSizeInBytes(count);
	if (!HasEnoughSpaceInSegment(metadata_size + data_size)) {
		FlushSegment();
	}
	container_metadata.push_back(metadata);
	metadata_collection.AddMetadata(metadata);
	current_count += count;

	// Reset the container analyze state
	one_count = 0;
	zero_count = 0;
	run_count = 0;
	last_bit_set = false;
	count = 0;
}

void RoaringAnalyzeState::Analyze(Vector &input, idx_t count) {
	auto &self = *this;

	RoaringStateAppender<RoaringAnalyzeState>::AppendVector(self, input, count);
	total_count += count;
}

} // namespace roaring

} // namespace duckdb
