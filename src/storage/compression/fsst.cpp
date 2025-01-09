#include "duckdb/common/fsst.hpp"

#include "duckdb/common/bitpacking.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"

#include "fsst.h"
#include "miniz_wrapper.hpp"

namespace duckdb {
struct FSSTScanState;

typedef struct {
	uint32_t dict_size;
	uint32_t dict_end;
	uint32_t bitpacking_width;
	uint32_t fsst_symbol_table_offset;
} fsst_compression_header_t;

// Counts and offsets used during scanning/fetching
//                                         |               ColumnSegment to be scanned / fetched from				 |
//                                         | untouched | bp align | unused d-values | to scan | bp align | untouched |
typedef struct BPDeltaDecodeOffsets {
	idx_t delta_decode_start_row;      //                         X
	idx_t bitunpack_alignment_offset;  //			   <--------->
	idx_t bitunpack_start_row;         //	           X
	idx_t unused_delta_decoded_values; //						  <----------------->
	idx_t scan_offset;                 //			   <---------------------------->
	idx_t total_delta_decode_count;    //					      <-------------------------->
	idx_t total_bitunpack_count;       //              <------------------------------------------------>
} bp_delta_offsets_t;

struct FSSTStorage {
	static constexpr double MINIMUM_COMPRESSION_RATIO = 1.2;
	static constexpr double ANALYSIS_SAMPLE_SIZE = 0.25;

	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	template <bool ALLOW_FSST_VECTORS = false>
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);
	static void Select(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
	                   const SelectionVector &sel, idx_t sel_count);

	static void SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container);
	static StringDictionaryContainer GetDictionary(ColumnSegment &segment, BufferHandle &handle);

	static char *FetchStringPointer(StringDictionaryContainer dict, data_ptr_t baseptr, int32_t dict_offset);
	static bp_delta_offsets_t CalculateBpDeltaOffsets(int64_t last_known_row, idx_t start, idx_t scan_count);
	static bool ParseFSSTSegmentHeader(data_ptr_t base_ptr, duckdb_fsst_decoder_t *decoder_out,
	                                   bitpacking_width_t *width_out);
	static bp_delta_offsets_t StartScan(FSSTScanState &scan_state, data_ptr_t base_data, idx_t start,
	                                    idx_t vector_count);
	static void EndScan(FSSTScanState &scan_state, bp_delta_offsets_t &offsets, idx_t start, idx_t scan_count);
};

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct FSSTScanState : public StringScanState {
	explicit FSSTScanState(const idx_t string_block_limit) {
		ResetStoredDelta();
		decompress_buffer.resize(string_block_limit + 1);
	}

	buffer_ptr<void> duckdb_fsst_decoder;
	vector<unsigned char> decompress_buffer;
	bitpacking_width_t current_width;

	// To speed up delta decoding we store the last index
	uint32_t last_known_index;
	int64_t last_known_row;

	unsafe_unique_array<uint32_t> bitunpack_buffer;
	idx_t bitunpack_buffer_capacity = 0;
	unsafe_unique_array<uint32_t> delta_decode_buffer;
	idx_t delta_decode_capacity = 0;

	void StoreLastDelta(uint32_t value, int64_t row) {
		last_known_index = value;
		last_known_row = row;
	}
	void ResetStoredDelta() {
		last_known_index = 0;
		last_known_row = -1;
	}
	inline string_t DecompressString(StringDictionaryContainer dict, data_ptr_t baseptr,
	                                 const bp_delta_offsets_t &offsets, idx_t index, Vector &result) {
		uint32_t str_len = bitunpack_buffer[offsets.scan_offset + index];
		auto str_ptr = FSSTStorage::FetchStringPointer(
		    dict, baseptr,
		    UnsafeNumericCast<int32_t>(delta_decode_buffer[index + offsets.unused_delta_decoded_values]));

		if (str_len == 0) {
			return string_t(nullptr, 0);
		}
		return FSSTPrimitives::DecompressValue(duckdb_fsst_decoder.get(), result, str_ptr, str_len, decompress_buffer);
	}
};

unique_ptr<SegmentScanState> FSSTStorage::StringInitScan(ColumnSegment &segment) {
	auto string_block_limit = StringUncompressed::GetStringBlockLimit(segment.GetBlockManager().GetBlockSize());
	auto state = make_uniq<FSSTScanState>(string_block_limit);
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	state->handle = buffer_manager.Pin(segment.block);
	auto base_ptr = state->handle.Ptr() + segment.GetBlockOffset();

	state->duckdb_fsst_decoder = make_buffer<duckdb_fsst_decoder_t>();
	auto retval = ParseFSSTSegmentHeader(
	    base_ptr, reinterpret_cast<duckdb_fsst_decoder_t *>(state->duckdb_fsst_decoder.get()), &state->current_width);
	if (!retval) {
		state->duckdb_fsst_decoder = nullptr;
	}

	return std::move(state);
}

void DeltaDecodeIndices(uint32_t *buffer_in, uint32_t *buffer_out, idx_t decode_count, uint32_t last_known_value) {
	buffer_out[0] = buffer_in[0];
	buffer_out[0] += last_known_value;
	for (idx_t i = 1; i < decode_count; i++) {
		buffer_out[i] = buffer_in[i] + buffer_out[i - 1];
	}
}

void BitUnpackRange(data_ptr_t src_ptr, data_ptr_t dst_ptr, idx_t count, idx_t row, bitpacking_width_t width) {
	auto bitunpack_src_ptr = &src_ptr[(row * width) / 8];
	BitpackingPrimitives::UnPackBuffer<uint32_t>(dst_ptr, bitunpack_src_ptr, count, width);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
bp_delta_offsets_t FSSTStorage::StartScan(FSSTScanState &scan_state, data_ptr_t base_data, idx_t start,
                                          idx_t scan_count) {
	if (start == 0 || scan_state.last_known_row >= (int64_t)start) {
		scan_state.ResetStoredDelta();
	}

	auto offsets = CalculateBpDeltaOffsets(scan_state.last_known_row, start, scan_count);

	if (scan_state.bitunpack_buffer_capacity < offsets.total_bitunpack_count) {
		scan_state.bitunpack_buffer = make_unsafe_uniq_array<uint32_t>(offsets.total_bitunpack_count);
		scan_state.bitunpack_buffer_capacity = offsets.total_bitunpack_count;
	}
	BitUnpackRange(base_data, data_ptr_cast(scan_state.bitunpack_buffer.get()), offsets.total_bitunpack_count,
	               offsets.bitunpack_start_row, scan_state.current_width);
	if (scan_state.delta_decode_capacity < offsets.total_delta_decode_count) {
		scan_state.delta_decode_buffer = make_unsafe_uniq_array<uint32_t>(offsets.total_delta_decode_count);
		scan_state.delta_decode_capacity = offsets.total_delta_decode_count;
	}
	DeltaDecodeIndices(scan_state.bitunpack_buffer.get() + offsets.bitunpack_alignment_offset,
	                   scan_state.delta_decode_buffer.get(), offsets.total_delta_decode_count,
	                   scan_state.last_known_index);
	return offsets;
}

void FSSTStorage::EndScan(FSSTScanState &scan_state, bp_delta_offsets_t &offsets, idx_t start, idx_t scan_count) {
	scan_state.StoreLastDelta(scan_state.delta_decode_buffer[scan_count + offsets.unused_delta_decoded_values - 1],
	                          UnsafeNumericCast<int64_t>(start + scan_count - 1));
}

template <bool ALLOW_FSST_VECTORS>
void FSSTStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                                    idx_t result_offset) {

	auto &scan_state = state.scan_state->Cast<FSSTScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	bool enable_fsst_vectors;
	if (ALLOW_FSST_VECTORS) {
		auto &config = DBConfig::GetConfig(segment.db);
		enable_fsst_vectors = config.options.enable_fsst_vectors;
	} else {
		enable_fsst_vectors = false;
	}

	auto baseptr = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto dict = GetDictionary(segment, scan_state.handle);
	auto base_data = data_ptr_cast(baseptr + sizeof(fsst_compression_header_t));
	string_t *result_data;

	if (scan_count == 0) {
		return;
	}

	if (enable_fsst_vectors) {
		D_ASSERT(result_offset == 0);
		if (scan_state.duckdb_fsst_decoder) {
			D_ASSERT(result_offset == 0 || result.GetVectorType() == VectorType::FSST_VECTOR);
			result.SetVectorType(VectorType::FSST_VECTOR);
			auto string_block_limit = StringUncompressed::GetStringBlockLimit(segment.GetBlockManager().GetBlockSize());
			FSSTVector::RegisterDecoder(result, scan_state.duckdb_fsst_decoder, string_block_limit);
			result_data = FSSTVector::GetCompressedData<string_t>(result);
		} else {
			D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
			result_data = FlatVector::GetData<string_t>(result);
		}
	} else {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		result_data = FlatVector::GetData<string_t>(result);
	}

	auto offsets = StartScan(scan_state, base_data, start, scan_count);
	auto &bitunpack_buffer = scan_state.bitunpack_buffer;
	auto &delta_decode_buffer = scan_state.delta_decode_buffer;
	if (enable_fsst_vectors) {
		// Lookup decompressed offsets in dict
		for (idx_t i = 0; i < scan_count; i++) {
			uint32_t string_length = bitunpack_buffer[i + offsets.scan_offset];
			result_data[i] = UncompressedStringStorage::FetchStringFromDict(
			    segment, dict, result, baseptr,
			    UnsafeNumericCast<int32_t>(delta_decode_buffer[i + offsets.unused_delta_decoded_values]),
			    string_length);
			FSSTVector::SetCount(result, scan_count);
		}
	} else {
		// Just decompress
		for (idx_t i = 0; i < scan_count; i++) {
			result_data[i + result_offset] = scan_state.DecompressString(dict, baseptr, offsets, i, result);
		}
	}
	EndScan(scan_state, offsets, start, scan_count);
}

void FSSTStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	StringScanPartial<true>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
void FSSTStorage::Select(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
                         const SelectionVector &sel, idx_t sel_count) {
	auto &scan_state = state.scan_state->Cast<FSSTScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	auto baseptr = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto dict = GetDictionary(segment, scan_state.handle);
	auto base_data = data_ptr_cast(baseptr + sizeof(fsst_compression_header_t));

	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);

	auto offsets = StartScan(scan_state, base_data, start, vector_count);
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < sel_count; i++) {
		idx_t index = sel.get_index(i);
		result_data[i] = scan_state.DecompressString(dict, baseptr, offsets, index, result);
	}
	EndScan(scan_state, offsets, start, vector_count);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void FSSTStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                                 idx_t result_idx) {

	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	auto base_ptr = handle.Ptr() + segment.GetBlockOffset();
	auto base_data = data_ptr_cast(base_ptr + sizeof(fsst_compression_header_t));
	auto dict = GetDictionary(segment, handle);

	duckdb_fsst_decoder_t decoder;
	bitpacking_width_t width;
	auto have_symbol_table = ParseFSSTSegmentHeader(base_ptr, &decoder, &width);

	auto result_data = FlatVector::GetData<string_t>(result);
	if (!have_symbol_table) {
		// There is no FSST symtable. This is only the case for empty strings or NULLs. We emit an empty string.
		result_data[result_idx] = string_t(nullptr, 0);
		return;
	}

	// We basically just do a scan of 1 which is kinda expensive as we need to repeatedly delta decode until we
	// reach the row we want, we could consider a more clever caching trick if this is slow
	auto offsets = CalculateBpDeltaOffsets(-1, UnsafeNumericCast<idx_t>(row_id), 1);

	auto bitunpack_buffer = unique_ptr<uint32_t[]>(new uint32_t[offsets.total_bitunpack_count]);
	BitUnpackRange(base_data, data_ptr_cast(bitunpack_buffer.get()), offsets.total_bitunpack_count,
	               offsets.bitunpack_start_row, width);
	auto delta_decode_buffer = unique_ptr<uint32_t[]>(new uint32_t[offsets.total_delta_decode_count]);
	DeltaDecodeIndices(bitunpack_buffer.get() + offsets.bitunpack_alignment_offset, delta_decode_buffer.get(),
	                   offsets.total_delta_decode_count, 0);

	uint32_t string_length = bitunpack_buffer[offsets.scan_offset];

	string_t compressed_string = UncompressedStringStorage::FetchStringFromDict(
	    segment, dict, result, base_ptr,
	    UnsafeNumericCast<int32_t>(delta_decode_buffer[offsets.unused_delta_decoded_values]), string_length);

	vector<unsigned char> uncompress_buffer;
	auto string_block_limit = StringUncompressed::GetStringBlockLimit(segment.GetBlockManager().GetBlockSize());
	uncompress_buffer.resize(string_block_limit + 1);
	result_data[result_idx] = FSSTPrimitives::DecompressValue((void *)&decoder, result, compressed_string.GetData(),
	                                                          compressed_string.GetSize(), uncompress_buffer);
}

unique_ptr<AnalyzeState> FSSTInitAnalyze(ColumnData &col_data, PhysicalType type) {
	// This compression type is deprecated
	return nullptr;
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction FSSTFun::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::VARCHAR);
	return CompressionFunction(CompressionType::COMPRESSION_FSST, data_type, FSSTInitAnalyze, nullptr, nullptr, nullptr,
	                           nullptr, nullptr, FSSTStorage::StringInitScan, FSSTStorage::StringScan,
	                           FSSTStorage::StringScanPartial<false>, FSSTStorage::StringFetchRow,
	                           UncompressedFunctions::EmptySkip, UncompressedStringStorage::StringInitSegment, nullptr,
	                           nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, FSSTStorage::Select);
}

bool FSSTFun::TypeIsSupported(const PhysicalType physical_type) {
	return physical_type == PhysicalType::VARCHAR;
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
void FSSTStorage::SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container) {
	auto header_ptr = reinterpret_cast<fsst_compression_header_t *>(handle.Ptr() + segment.GetBlockOffset());
	Store<uint32_t>(container.size, data_ptr_cast(&header_ptr->dict_size));
	Store<uint32_t>(container.end, data_ptr_cast(&header_ptr->dict_end));
}

StringDictionaryContainer FSSTStorage::GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto header_ptr = reinterpret_cast<fsst_compression_header_t *>(handle.Ptr() + segment.GetBlockOffset());
	StringDictionaryContainer container;
	container.size = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_size));
	container.end = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_end));
	return container;
}

char *FSSTStorage::FetchStringPointer(StringDictionaryContainer dict, data_ptr_t baseptr, int32_t dict_offset) {
	if (dict_offset == 0) {
		return nullptr;
	}

	auto dict_end = baseptr + dict.end;
	auto dict_pos = dict_end - dict_offset;
	return char_ptr_cast(dict_pos);
}

// Returns false if no symbol table was found. This means all strings are either empty or null
bool FSSTStorage::ParseFSSTSegmentHeader(data_ptr_t base_ptr, duckdb_fsst_decoder_t *decoder_out,
                                         bitpacking_width_t *width_out) {
	auto header_ptr = reinterpret_cast<fsst_compression_header_t *>(base_ptr);
	auto fsst_symbol_table_offset = Load<uint32_t>(data_ptr_cast(&header_ptr->fsst_symbol_table_offset));
	*width_out = (bitpacking_width_t)(Load<uint32_t>(data_ptr_cast(&header_ptr->bitpacking_width)));
	return duckdb_fsst_import(decoder_out, base_ptr + fsst_symbol_table_offset);
}

// The calculation of offsets and counts while scanning or fetching is a bit tricky, for two reasons:
// - bitunpacking needs to be aligned to BITPACKING_ALGORITHM_GROUP_SIZE
// - delta decoding needs to decode from the last known value.
bp_delta_offsets_t FSSTStorage::CalculateBpDeltaOffsets(int64_t last_known_row, idx_t start, idx_t scan_count) {
	D_ASSERT((idx_t)(last_known_row + 1) <= start);
	bp_delta_offsets_t result;

	result.delta_decode_start_row = (idx_t)(last_known_row + 1);
	result.bitunpack_alignment_offset =
	    result.delta_decode_start_row % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
	result.bitunpack_start_row = result.delta_decode_start_row - result.bitunpack_alignment_offset;
	result.unused_delta_decoded_values = start - result.delta_decode_start_row;
	result.scan_offset = result.bitunpack_alignment_offset + result.unused_delta_decoded_values;
	result.total_delta_decode_count = scan_count + result.unused_delta_decoded_values;
	result.total_bitunpack_count =
	    BitpackingPrimitives::RoundUpToAlgorithmGroupSize<idx_t>(scan_count + result.scan_offset);

	D_ASSERT(result.total_delta_decode_count + result.bitunpack_alignment_offset <= result.total_bitunpack_count);
	return result;
}

} // namespace duckdb
