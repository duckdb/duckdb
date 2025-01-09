#include "duckdb/storage/compression/dictionary/decompression.hpp"

#include "duckdb/common/bitpacking.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"

/*
Data layout per segment:
+------------------------------------------------------+
|                  Header                              |
|   +----------------------------------------------+   |
|   |   dictionary_compression_header_t  header    |   |
|   +----------------------------------------------+   |
|                                                      |
+------------------------------------------------------+
|             Selection Buffer               |
|   +------------------------------------+   |
|   |   uint16_t index_buffer_idx[]      |   |
|   +------------------------------------+   |
|      tuple index -> index buffer idx       |
|                                            |
+--------------------------------------------+
|               Index Buffer                 |
|   +------------------------------------+   |
|   |   uint16_t  dictionary_offset[]    |   |
|   +------------------------------------+   |
|  string_index -> offset in the dictionary  |
|                                            |
+--------------------------------------------+
|                Dictionary                  |
|   +------------------------------------+   |
|   |   uint8_t *raw_string_data         |   |
|   +------------------------------------+   |
|      the string data without lengths       |
|                                            |
+--------------------------------------------+
*/

namespace duckdb {
namespace dictionary {

struct DictionaryCompressionStorage {
	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	template <bool ALLOW_DICT_VECTORS>
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);
};

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
unique_ptr<SegmentScanState> DictionaryCompressionStorage::StringInitScan(ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto state = make_uniq<CompressedStringScanState>(buffer_manager.Pin(segment.block));
	state->Initialize(segment, true);
	return std::move(state);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <bool ALLOW_DICT_VECTORS>
void DictionaryCompressionStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                                     Vector &result, idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<CompressedStringScanState>();

	auto start = segment.GetRelativeIndex(state.row_index);
	if (!ALLOW_DICT_VECTORS || scan_count != STANDARD_VECTOR_SIZE ||
	    start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE != 0) {
		scan_state.ScanToFlatVector(result, result_offset, start, scan_count);
	} else {
		scan_state.ScanToDictionaryVector(segment, result, result_offset, start, scan_count);
	}
}

void DictionaryCompressionStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                              Vector &result) {
	StringScanPartial<true>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DictionaryCompressionStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id,
                                                  Vector &result, idx_t result_idx) {
	// fetch a single row from the string segment
	CompressedStringScanState scan_state(state.GetOrInsertHandle(segment));
	scan_state.Initialize(segment, false);
	scan_state.ScanToFlatVector(result, result_idx, NumericCast<idx_t>(row_id), 1);
}

unique_ptr<AnalyzeState> InitAnalyze(ColumnData &col_data, PhysicalType type) {
	// This compression type is deprecated
	return nullptr;
}

} // namespace dictionary

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction DictionaryCompressionFun::GetFunction(PhysicalType data_type) {
	auto res = CompressionFunction(CompressionType::COMPRESSION_DICTIONARY, data_type, dictionary::InitAnalyze, nullptr,
	                               nullptr, nullptr, nullptr, nullptr,
	                               dictionary::DictionaryCompressionStorage::StringInitScan,
	                               dictionary::DictionaryCompressionStorage::StringScan,
	                               dictionary::DictionaryCompressionStorage::StringScanPartial<false>,
	                               dictionary::DictionaryCompressionStorage::StringFetchRow,
	                               UncompressedFunctions::EmptySkip, UncompressedStringStorage::StringInitSegment);
	res.validity = CompressionValidity::NO_VALIDITY_REQUIRED;
	return res;
}

bool DictionaryCompressionFun::TypeIsSupported(const PhysicalType physical_type) {
	return physical_type == PhysicalType::VARCHAR;
}

} // namespace duckdb
