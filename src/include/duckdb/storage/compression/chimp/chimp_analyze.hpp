//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/chimp_analyze.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/chimp/chimp.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

struct EmptyChimpWriter;

template <class T>
struct ChimpAnalyzeState : public AnalyzeState {
public:
	using CHIMP_TYPE = typename ChimpType<T>::type;

	ChimpAnalyzeState() : state() {
		state.AssignDataBuffer(nullptr);
	}
	ChimpState<T, true> state;
	idx_t group_idx = 0;
	idx_t data_byte_size = 0;
	idx_t metadata_byte_size = 0;

public:
	void WriteValue(CHIMP_TYPE value, bool is_valid) {
		if (!is_valid) {
			return;
		}
		//! Keep track of when a segment would end, to accurately simulate Reset()s in compress step
		if (!HasEnoughSpace()) {
			StartNewSegment();
		}
		Chimp128Compression<CHIMP_TYPE, true>::Store(value, state.chimp);
		group_idx++;
		if (group_idx == ChimpPrimitives::CHIMP_SEQUENCE_SIZE) {
			StartNewGroup();
		}
	}

	void StartNewSegment() {
		state.Flush();
		StartNewGroup();
		data_byte_size += UsedSpace();
		metadata_byte_size += ChimpPrimitives::HEADER_SIZE;
		state.chimp.output.SetStream(nullptr);
	}

	idx_t CurrentGroupMetadataSize() const {
		idx_t metadata_size = 0;

		metadata_size += 3 * state.chimp.leading_zero_buffer.BlockCount();
		metadata_size += state.chimp.flag_buffer.BytesUsed();
		metadata_size += 2 * state.chimp.packed_data_buffer.index;
		return metadata_size;
	}

	idx_t RequiredSpace() const {
		idx_t required_space = ChimpPrimitives::MAX_BYTES_PER_VALUE;
		// Any value could be the last,
		// so the cost of flushing metadata should be factored into the cost
		// byte offset of data
		required_space += sizeof(byte_index_t);
		// amount of leading zero blocks
		required_space += sizeof(uint8_t);
		// first leading zero block
		required_space += 3;
		// amount of flag bytes
		required_space += sizeof(uint8_t);
		// first flag byte
		required_space += 1;
		return required_space;
	}

	void StartNewGroup() {
		metadata_byte_size += CurrentGroupMetadataSize();
		group_idx = 0;
		state.chimp.Reset();
	}

	idx_t UsedSpace() const {
		return state.chimp.output.BytesWritten();
	}

	bool HasEnoughSpace() {
		idx_t total_bytes_used = 0;
		total_bytes_used += AlignValue(ChimpPrimitives::HEADER_SIZE + UsedSpace() + RequiredSpace());
		total_bytes_used += CurrentGroupMetadataSize();
		total_bytes_used += metadata_byte_size;
		return total_bytes_used <= Storage::BLOCK_SIZE;
	}

	idx_t TotalUsedBytes() const {
		return metadata_byte_size + AlignValue(data_byte_size + UsedSpace());
	}
};

template <class T>
unique_ptr<AnalyzeState> ChimpInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<ChimpAnalyzeState<T>>();
}

template <class T>
bool ChimpAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	using CHIMP_TYPE = typename ChimpType<T>::type;
	auto &analyze_state = (ChimpAnalyzeState<T> &)state;
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	auto data = UnifiedVectorFormat::GetData<CHIMP_TYPE>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		analyze_state.WriteValue(data[idx], vdata.validity.RowIsValid(idx));
	}
	return true;
}

template <class T>
idx_t ChimpFinalAnalyze(AnalyzeState &state) {
	auto &chimp = (ChimpAnalyzeState<T> &)state;
	// Finish the last "segment"
	chimp.StartNewSegment();
	// Multiply the final size to factor in the extra cost of decompression time
	const auto multiplier = 2.0;
	const auto final_analyze_size = chimp.TotalUsedBytes();
	return final_analyze_size * multiplier;
}

} // namespace duckdb
