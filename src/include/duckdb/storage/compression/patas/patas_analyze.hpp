//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/patas/patas_analyze.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/patas/patas.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

struct EmptyPatasWriter;

template <class T>
struct PatasAnalyzeState : public AnalyzeState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	PatasAnalyzeState() : state((void *)this) {
		state.AssignDataBuffer(nullptr);
	}
	PatasState<T, true> state;
	idx_t group_idx = 0;
	idx_t data_byte_size = 0;
	idx_t metadata_byte_size = 0;

public:
	void WriteValue(EXACT_TYPE value, bool is_valid) {
		// TODO: transform NULL values to a value that requires the least amount of bits to store
		// 3 + 6 + 0 is probably possible

		//! Keep track of when a segment would end, to accurately simulate Reset()s in compress step
		if (!HasEnoughSpace()) {
			StartNewSegment();
		}
		patas::PatasCompression<EXACT_TYPE, true>::Store(value, state.patas_state);
		group_idx++;
		if (group_idx == PatasPrimitives::PATAS_GROUP_SIZE) {
			StartNewGroup();
		}
	}

	void StartNewSegment() {
		StartNewGroup();
		data_byte_size += UsedSpace();
		metadata_byte_size += PatasPrimitives::HEADER_SIZE;
		state.patas_state.byte_writer.SetStream(nullptr);
	}

	idx_t CurrentGroupMetadataSize() const {
		// Metadata size is constant, we require only 3 + 6 bits of metadata per value, aligned to bitpacking algorithm
		// group size (32)
		const uint64_t byte_count_bits =
		    AlignValue<uint64_t, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE>(state.patas_state.Index()) *
		    PatasPrimitives::BYTECOUNT_BITSIZE;
		const uint64_t trailing_zero_bits =
		    AlignValue<uint64_t, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE>(state.patas_state.Index()) *
		    PatasPrimitives::TRAILING_ZERO_BITSIZE;
		return byte_count_bits + trailing_zero_bits;
	}

	idx_t RequiredSpace() const {
		idx_t required_space = sizeof(EXACT_TYPE);
		// Any value could be the last,
		// so the cost of flushing metadata should be factored into the cost
		// byte offset of data
		required_space += sizeof(byte_index_t);
		// amount of bitpacked blocks (can probably be removed!!)
		required_space += sizeof(uint8_t);
		return required_space;
	}

	void StartNewGroup() {
		group_idx = 0;
		metadata_byte_size += CurrentGroupMetadataSize();
		state.patas_state.Reset();
	}

	idx_t UsedSpace() const {
		return state.patas_state.byte_writer.BytesWritten();
	}

	bool HasEnoughSpace() {
		idx_t total_bytes_used = 0;
		total_bytes_used += PatasPrimitives::HEADER_SIZE;
		total_bytes_used += AlignValue(UsedSpace() + RequiredSpace());
		total_bytes_used += CurrentGroupMetadataSize();
		total_bytes_used += metadata_byte_size;
		return total_bytes_used <= Storage::BLOCK_SIZE;
	}

	idx_t TotalUsedBytes() const {
		return metadata_byte_size + AlignValue(data_byte_size + UsedSpace());
	}
};

struct EmptyPatasWriter {

	template <class VALUE_TYPE>
	static void Operation(VALUE_TYPE uncompressed_value, bool is_valid, void *state_p) {
		using EXACT_TYPE = typename FloatingToExact<VALUE_TYPE>::type;

		auto state_wrapper = (PatasAnalyzeState<VALUE_TYPE> *)state_p;
		state_wrapper->WriteValue(*(EXACT_TYPE *)(&uncompressed_value), is_valid);
	}
};

template <class T>
unique_ptr<AnalyzeState> PatasInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<PatasAnalyzeState<T>>();
}

template <class T>
bool PatasAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = (PatasAnalyzeState<T> &)state;
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	auto data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		analyze_state.state.template Update<EmptyPatasWriter>(data[idx], vdata.validity.RowIsValid(idx));
	}
	return true;
}

template <class T>
idx_t PatasFinalAnalyze(AnalyzeState &state) {
	auto &patas_state = (PatasAnalyzeState<T> &)state;
	// Finish the last "segment"
	patas_state.StartNewSegment();
	const auto final_analyze_size = patas_state.TotalUsedBytes();
	return final_analyze_size;
}

} // namespace duckdb
