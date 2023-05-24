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
	//! To optimally store NULL, we keep track of the directly previous value
	EXACT_TYPE previous_value;

public:
	void WriteValue(EXACT_TYPE value, bool is_valid) {
		if (!is_valid) {
			value = previous_value;
		}
		//! Keep track of when a segment would end, to accurately simulate Reset()s in compress step
		if (!HasEnoughSpace()) {
			StartNewSegment();
		}
		patas::PatasCompression<EXACT_TYPE, true>::Store(value, state.patas_state);
		previous_value = value;
		group_idx++;
		if (group_idx == PatasPrimitives::PATAS_GROUP_SIZE) {
			StartNewGroup();
		}
	}

	idx_t CurrentGroupMetadataSize() const {
		idx_t metadata_size = 0;

		// Offset to the data of the group
		metadata_size += sizeof(uint32_t);
		// Packed Trailing zeros + significant bytes + index_offsets for group
		metadata_size += 2 * group_idx;
		return metadata_size;
	}

	void StartNewSegment() {
		StartNewGroup();
		data_byte_size += UsedSpace();
		metadata_byte_size += PatasPrimitives::HEADER_SIZE;
		state.patas_state.byte_writer.SetStream(nullptr);
	}

	idx_t RequiredSpace() const {
		idx_t required_space = 0;
		required_space += sizeof(EXACT_TYPE);
		required_space += sizeof(uint16_t);
		return required_space;
	}

	void StartNewGroup() {
		previous_value = 0;
		metadata_byte_size += CurrentGroupMetadataSize();
		group_idx = 0;
		state.patas_state.Reset();
	}

	idx_t UsedSpace() const {
		return state.patas_state.byte_writer.BytesWritten();
	}

	bool HasEnoughSpace() {
		idx_t total_bytes_used = 0;
		total_bytes_used += AlignValue(PatasPrimitives::HEADER_SIZE + UsedSpace() + RequiredSpace());
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
		state_wrapper->WriteValue(Load<EXACT_TYPE>(const_data_ptr_cast(&uncompressed_value)), is_valid);
	}
};

template <class T>
unique_ptr<AnalyzeState> PatasInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<PatasAnalyzeState<T>>();
}

template <class T>
bool PatasAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = (PatasAnalyzeState<T> &)state;
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	auto data = UnifiedVectorFormat::GetData<T>(vdata);
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
	// Multiply the final size to factor in the extra cost of decompression time
	const auto multiplier = 1.2;
	return final_analyze_size * multiplier;
}

} // namespace duckdb
