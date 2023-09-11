#include "duckdb/common/bitpacking.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/constants.hpp"
#include "zstd_wrapper.hpp"

namespace duckdb {

struct ZSTDStorage {
	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
	                                                    unique_ptr<AnalyzeState> analyze_state_p);
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);

	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct ZSTDAnalyzeState : public AnalyzeState {
	idx_t total_size = 0;
	idx_t count = 0;

	inline void AppendEmptyString() {
		count++;
	}

	inline void AppendString(const string_t &str) {
		auto string_size = str.GetSize();
		total_size += string_size;
		count++;
	}
};

unique_ptr<AnalyzeState> ZSTDStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<ZSTDAnalyzeState>();
}

bool ZSTDStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<ZSTDAnalyzeState>();
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			state.AppendEmptyString();
			continue;
		}
		state.AppendString(data[idx]);
	}
	return true;
}

idx_t ZSTDStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<ZSTDAnalyzeState>();

	if (!state.count) {
		return DConstants::INVALID_INDEX;
	}
	// get the size of the offsets into the buffer
	auto bits_per_value = BitpackingPrimitives::MinimumBitWidth(state.total_size);
	auto total_offset_size = (bits_per_value * state.count) / 8;
	// get the size of the buffer
	// we estimate a compression ratio of 2X
	auto string_data_size = state.total_size / 2;

	// we only use zstd if it is at least 1.3 times better than the alternative
	auto zstd_penalty_factor = 1.3;

	return (total_offset_size + string_data_size) * zstd_penalty_factor;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
class ZSTDCompressionState : public CompressionState {
public:
	explicit ZSTDCompressionState(ColumnDataCheckpointer &checkpointer)

	    : checkpointer(checkpointer), function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_ZSTD)),
		block_manager(BlockManager::GetBlockManager(checkpointer.GetDatabase())) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	~ZSTDCompressionState() override {
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		current_segment = std::move(compressed_segment);
		current_segment->function = function;
	}

//	void UpdateState(string_t uncompressed_string, unsigned char *compressed_string, size_t compressed_string_len) {
//		if (!HasEnoughSpace(compressed_string_len)) {
//			Flush();
//			if (!HasEnoughSpace(compressed_string_len)) {
//				throw InternalException("FSST string compression failed due to insufficient space in empty block");
//			};
//		}
//
//		UncompressedStringStorage::UpdateStringStats(current_segment->stats, uncompressed_string);
//
//		// Write string into dictionary
//		current_dictionary.size += compressed_string_len;
//		auto dict_pos = current_end_ptr - current_dictionary.size;
//		memcpy(dict_pos, compressed_string, compressed_string_len);
//		current_dictionary.Verify();
//
//		// We just push the string length to effectively delta encode the strings
//		index_buffer.push_back(compressed_string_len);
//
//		max_compressed_string_length = MaxValue(max_compressed_string_length, compressed_string_len);
//
//		current_width = BitpackingPrimitives::MinimumBitWidth(max_compressed_string_length);
//		current_segment->count++;
//	}

	void AddNull() {
		if (!HasEnoughSpace(0)) {
			Flush();
			if (!HasEnoughSpace(0)) {
				throw InternalException("FSST string compression failed due to insufficient space in empty block");
			};
		}
		index_buffer.push_back(0);
		current_segment->count++;
	}

//	void AddEmptyString() {
//		AddNull();
//		UncompressedStringStorage::UpdateStringStats(current_segment->stats, "");
//	}
//
//	size_t GetRequiredSize(size_t string_len) {
//		bitpacking_width_t required_minimum_width;
//		if (string_len > max_compressed_string_length) {
//			required_minimum_width = BitpackingPrimitives::MinimumBitWidth(string_len);
//		} else {
//			required_minimum_width = current_width;
//		}
//
//		size_t current_dict_size = current_dictionary.size;
//		idx_t current_string_count = index_buffer.size();
//
//		size_t dict_offsets_size =
//		    BitpackingPrimitives::GetRequiredSize(current_string_count + 1, required_minimum_width);
//
//		// TODO switch to a symbol table per RowGroup, saves a bit of space
//		return sizeof(fsst_compression_header_t) + current_dict_size + dict_offsets_size + string_len +
//		       fsst_serialized_symbol_table_size;
//	}
//
//	// Checks if there is enough space, if there is, sets last_fitting_size
//	bool HasEnoughSpace(size_t string_len) {
//		auto required_size = GetRequiredSize(string_len);
//
//		if (required_size <= Storage::BLOCK_SIZE) {
//			last_fitting_size = required_size;
//			return true;
//		}
//		return false;
//	}
//
//	void Flush(bool final = false) {
//		auto next_start = current_segment->start + current_segment->count;
//
//		auto segment_size = Finalize();
//		auto &state = checkpointer.GetCheckpointState();
//		state.FlushSegment(std::move(current_segment), segment_size);
//
//		if (!final) {
//			CreateEmptySegment(next_start);
//		}
//	}
//
//	idx_t Finalize() {
//		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
//		auto handle = buffer_manager.Pin(current_segment->block);
//		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);
//
//		// calculate sizes
//		auto compressed_index_buffer_size =
//		    BitpackingPrimitives::GetRequiredSize(current_segment->count, current_width);
//		auto total_size = sizeof(fsst_compression_header_t) + compressed_index_buffer_size + current_dictionary.size +
//		                  fsst_serialized_symbol_table_size;
//
//		if (total_size != last_fitting_size) {
//			throw InternalException("FSST string compression failed due to incorrect size calculation");
//		}
//
//		// calculate ptr and offsets
//		auto base_ptr = handle.Ptr();
//		auto header_ptr = reinterpret_cast<fsst_compression_header_t *>(base_ptr);
//		auto compressed_index_buffer_offset = sizeof(fsst_compression_header_t);
//		auto symbol_table_offset = compressed_index_buffer_offset + compressed_index_buffer_size;
//
//		D_ASSERT(current_segment->count == index_buffer.size());
//		BitpackingPrimitives::PackBuffer<sel_t, false>(base_ptr + compressed_index_buffer_offset,
//		                                               reinterpret_cast<uint32_t *>(index_buffer.data()),
//		                                               current_segment->count, current_width);
//
//		// Write the fsst symbol table or nothing
//		if (fsst_encoder != nullptr) {
//			memcpy(base_ptr + symbol_table_offset, &fsst_serialized_symbol_table[0], fsst_serialized_symbol_table_size);
//		} else {
//			memset(base_ptr + symbol_table_offset, 0, fsst_serialized_symbol_table_size);
//		}
//
//		Store<uint32_t>(symbol_table_offset, data_ptr_cast(&header_ptr->fsst_symbol_table_offset));
//		Store<uint32_t>((uint32_t)current_width, data_ptr_cast(&header_ptr->bitpacking_width));
//
//		if (total_size >= FSSTStorage::COMPACTION_FLUSH_LIMIT) {
//			// the block is full enough, don't bother moving around the dictionary
//			return Storage::BLOCK_SIZE;
//		}
//		// the block has space left: figure out how much space we can save
//		auto move_amount = Storage::BLOCK_SIZE - total_size;
//		// move the dictionary so it lines up exactly with the offsets
//		auto new_dictionary_offset = symbol_table_offset + fsst_serialized_symbol_table_size;
//		memmove(base_ptr + new_dictionary_offset, base_ptr + current_dictionary.end - current_dictionary.size,
//		        current_dictionary.size);
//		current_dictionary.end -= move_amount;
//		D_ASSERT(current_dictionary.end == total_size);
//		// write the new dictionary (with the updated "end")
//		FSSTStorage::SetDictionary(*current_segment, handle, current_dictionary);
//
//		return total_size;
//	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle current_handle;
	vector<uint32_t> index_buffer;
private:
	//! The block manager
	BlockManager &block_manager;

	//! Temporary buffer
	BufferHandle handle;
	//! The block on-disk to which we are writing
	block_id_t block_id;
	//! The offset within the current block
	idx_t offset;
};

unique_ptr<CompressionState> ZSTDStorage::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                          unique_ptr<AnalyzeState> analyze_state_p) {
	return make_uniq<ZSTDCompressionState>(checkpointer);
}

void ZSTDStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = state_p.Cast<ZSTDCompressionState>();

	// Get vector data
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);

	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		// Note: we treat nulls and empty strings the same
		if (!vdata.validity.RowIsValid(idx) || data[idx].GetSize() == 0) {
			state.AddNull();
			continue;
		}
		state.AddString(data[idx]);
	}
}

void ZSTDStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<ZSTDCompressionState>();
	state.Flush();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
//struct ZSTDScanState : public StringScanState {
//	FSSTScanState() {
//		ResetStoredDelta();
//	}
//
//	buffer_ptr<void> duckdb_fsst_decoder;
//	bitpacking_width_t current_width;
//
//	// To speed up delta decoding we store the last index
//	uint32_t last_known_index;
//	int64_t last_known_row;
//
//	void StoreLastDelta(uint32_t value, int64_t row) {
//		last_known_index = value;
//		last_known_row = row;
//	}
//	void ResetStoredDelta() {
//		last_known_index = 0;
//		last_known_row = -1;
//	}
//};

unique_ptr<SegmentScanState> ZSTDStorage::StringInitScan(ColumnSegment &segment) {
	throw InternalException("FIXME: ZSTD StringInitScan");
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void ZSTDStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                                    idx_t result_offset) {
	throw InternalException("FIXME: ZSTD StringScanPartial");
}

void ZSTDStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	throw InternalException("FIXME: ZSTD StringScan");
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void ZSTDStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                                 idx_t result_idx) {
	throw InternalException("FIXME: ZSTD StringFetchRow");
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction ZSTDFun::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::VARCHAR);
	return CompressionFunction(
	    CompressionType::COMPRESSION_ZSTD, data_type, ZSTDStorage::StringInitAnalyze, ZSTDStorage::StringAnalyze,
	    ZSTDStorage::StringFinalAnalyze, ZSTDStorage::InitCompression, ZSTDStorage::Compress,
	    ZSTDStorage::FinalizeCompress, ZSTDStorage::StringInitScan, ZSTDStorage::StringScan,
	    ZSTDStorage::StringScanPartial, ZSTDStorage::StringFetchRow, UncompressedFunctions::EmptySkip);
}

bool ZSTDFun::TypeIsSupported(PhysicalType type) {
	return type == PhysicalType::VARCHAR;
}

} // namespace duckdb
