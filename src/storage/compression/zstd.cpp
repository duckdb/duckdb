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

// Determines wether compression is possible and calculates sizes for the FinalAnalyze
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

// Compression score to determine which compression to use
idx_t ZSTDStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<ZSTDAnalyzeState>();

	if (state.count == 0) {
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
struct StringMetadata {
	idx_t size;
};

class ZSTDCompressionState : public CompressionState {
public:
	explicit ZSTDCompressionState(ColumnDataCheckpointer &checkpointer)
	    : checkpointer(checkpointer),
		function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_ZSTD)),
		heap(BufferAllocator::Get(checkpointer.GetDatabase())) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;

	// current segment state
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle current_handle;
	// ZSTDDictionary current_dictionary

	// buffer for current segment
	idx_t total_data_size;
	StringHeap heap;
	vector<uint32_t> index_buffer;

	data_ptr_t current_data_ptr;

	//! Temporary buffer
	// BufferHandle handle;
	//! The block on-disk to which we are writing
	// block_id_t block_id;
	//! The offset within the current block
	// idx_t offset;

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		current_segment = std::move(compressed_segment);
		current_segment->function = function;

		total_data_size = 0;

		// reset buffer
		// index_buffer.clear();

		// reset pointers
		auto &buffer_manager = BufferManager::GetBufferManager(checkpointer.GetDatabase());
		current_handle = buffer_manager.Pin(current_segment->block);
		current_data_ptr = current_handle.Ptr();
	}

	void FlushSegment(idx_t segment_size) {
		auto& state = checkpointer.GetCheckpointState();
		state.FlushSegment(std::move(current_segment), segment_size);
	}

	void Finalize(idx_t segment_size) {
		FlushSegment(segment_size);
		current_segment.reset();
	}

	void UpdateState(string_t uncompressed_string, unsigned char *compressed_string, size_t compressed_string_len) {
		throw InternalException("FIXME: ZSTD UpdateState");
	}

	void AddNull() {
		// TODO: make this more efficient
		AddString("");
	}

	void AddEmptyString() {
		AddNull();
		// UncompressedStringStorage::UpdateStringStats(current_segment->stats, ""); //?
	}

	// size_t GetRequiredSize(size_t string_len) {
	// 	throw InternalException("FIXME: ZSTD GetRequiredSize");
	// 	return 0;
	// }

	// Checks if there is enough space, if there is, sets last_fitting_size
	// bool HasEnoughSpace(size_t string_len) {
	// 	throw InternalException("FIXME: ZSTD HasEnoughSpace");
	// 	return false;
	// }

	void AddString(const string_t &str) {

		// TODO: add to dictionary
		
		// Create metadata
		StringMetadata meta {
			.size = str.GetSize()
		};

		// TODO: check if there is space

		// Write metadata
		current_data_ptr = data_ptr_cast(memcpy(current_data_ptr, &meta, sizeof(StringMetadata)));
		total_data_size += sizeof(StringMetadata);

		// Write string
		current_data_ptr = data_ptr_cast(memcpy(current_data_ptr, str.GetData(), str.GetSize()));
		total_data_size += str.GetSize();

	}
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
	state.Finalize(state.total_data_size);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct ZSTDScanState : public StringScanState {
	BufferHandle handle;
};

unique_ptr<SegmentScanState> ZSTDStorage::StringInitScan(ColumnSegment &segment) {
	auto result = make_uniq<ZSTDScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void ZSTDStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                                    idx_t result_offset) {
	// auto &scan_state = state.scan_state->template Cast<ZSTDScanState>();
	// auto start = segment.GetRelativeIndex(state.row_index);

	// auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	// auto source_data = data + start * sizeof(T);

	// result.SetVectorType(VectorType::FLAT_VECTOR);
	// FlatVector::SetData(result, source_data);
	throw InternalException("FIXME: ZSTD StringScanPartial");
}

void ZSTDStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	StringScanPartial(segment, state, scan_count, result, 0);
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
