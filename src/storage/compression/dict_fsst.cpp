#include "duckdb/storage/compression/dict_fsst/common.hpp"
#include "duckdb/storage/compression/dict_fsst/analyze.hpp"
#include "duckdb/storage/compression/dict_fsst/compression.hpp"
#include "duckdb/storage/compression/dict_fsst/decompression.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"

/*
Data layout per segment:
+-----------------------------------------------------+
|                  Header                             |
|   +---------------------------------------------+   |
|   |   dict_fsst_compression_header_t  header    |   |
|   +---------------------------------------------+   |
|                                                     |
+-----------------------------------------------------+
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
|             FSST Symbol Table (opt)        |
|   +------------------------------------+   |
|   |   duckdb_fsst_decoder_t table      |   |
|   +------------------------------------+   |
|                                            |
+--------------------------------------------+
*/

namespace duckdb {
namespace dict_fsst {

struct DictFSSTCompressionStorage {
	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointData &checkpoint_data,
	                                                    unique_ptr<AnalyzeState> state);
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);

	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	template <bool ALLOW_DICT_VECTORS>
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
unique_ptr<AnalyzeState> DictFSSTCompressionStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	auto &storage_manager = col_data.GetStorageManager();
	if (storage_manager.GetStorageVersion() < 5) {
		// dict_fsst not introduced yet, disable it
		return nullptr;
	}

	CompressionInfo info(col_data.GetBlockManager());
	return make_uniq<DictFSSTAnalyzeState>(info);
}

bool DictFSSTCompressionStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &analyze_state = state_p.Cast<DictFSSTAnalyzeState>();
	return analyze_state.Analyze(input, count);
}

idx_t DictFSSTCompressionStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &analyze_state = state_p.Cast<DictFSSTAnalyzeState>();
	return analyze_state.FinalAnalyze();
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
unique_ptr<CompressionState> DictFSSTCompressionStorage::InitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                                         unique_ptr<AnalyzeState> state) {
	return make_uniq<DictFSSTCompressionState>(checkpoint_data,
	                                           unique_ptr_cast<AnalyzeState, DictFSSTAnalyzeState>(std::move(state)));
}

void DictFSSTCompressionStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = state_p.Cast<DictFSSTCompressionState>();
	state.Compress(scan_vector, count);
}

void DictFSSTCompressionStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<DictFSSTCompressionState>();
	state.Flush(true);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
unique_ptr<SegmentScanState> DictFSSTCompressionStorage::StringInitScan(ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto state = make_uniq<CompressedStringScanState>(segment, buffer_manager.Pin(segment.block));
	state->Initialize(true);
	if (StringStats::HasMaxStringLength(segment.stats.statistics)) {
		state->all_values_inlined = StringStats::MaxStringLength(segment.stats.statistics) <= string_t::INLINE_LENGTH;
	}
	return std::move(state);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <bool ALLOW_DICT_VECTORS>
void DictFSSTCompressionStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                                   Vector &result, idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<CompressedStringScanState>();

	auto start = segment.GetRelativeIndex(state.row_index);
	if (!ALLOW_DICT_VECTORS || !scan_state.AllowDictionaryScan(start, scan_count)) {
		scan_state.ScanToFlatVector(result, result_offset, start, scan_count);
	} else {
		scan_state.ScanToDictionaryVector(segment, result, result_offset, start, scan_count);
	}
}

void DictFSSTCompressionStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                            Vector &result) {
	StringScanPartial<true>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DictFSSTCompressionStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id,
                                                Vector &result, idx_t result_idx) {
	// fetch a single row from the string segment
	CompressedStringScanState scan_state(segment, state.GetOrInsertHandle(segment));
	scan_state.Initialize(false);
	scan_state.ScanToFlatVector(result, result_idx, NumericCast<idx_t>(row_id), 1);
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
void DictFSSTSelect(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
                    const SelectionVector &sel, idx_t sel_count) {
	auto &scan_state = state.scan_state->Cast<CompressedStringScanState>();
	if (scan_state.mode == DictFSSTMode::FSST_ONLY) {
		// for FSST only
		auto start = segment.GetRelativeIndex(state.row_index);
		scan_state.Select(result, start, sel, sel_count);
		return;
	}
	// fallback: scan + slice
	DictFSSTCompressionStorage::StringScan(segment, state, vector_count, result);
	result.Slice(sel, sel_count);
}

//===--------------------------------------------------------------------===//
// Filter
//===--------------------------------------------------------------------===//
static void DictFSSTFilter(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
                           SelectionVector &sel, idx_t &sel_count, const TableFilter &filter,
                           TableFilterState &filter_state) {
	auto &scan_state = state.scan_state->Cast<CompressedStringScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);
	if (scan_state.AllowDictionaryScan(start, vector_count)) {
		// only pushdown filters on dictionaries
		if (!scan_state.filter_result) {
			// no filter result yet - apply filter to the dictionary
			// initialize the filter result - setting everything to false
			scan_state.filter_result = make_unsafe_uniq_array<bool>(scan_state.dict_count);

			// apply the filter
			UnifiedVectorFormat vdata;
			scan_state.dictionary->ToUnifiedFormat(scan_state.dict_count, vdata);
			SelectionVector dict_sel;
			idx_t filter_count = scan_state.dict_count;
			ColumnSegment::FilterSelection(dict_sel, *scan_state.dictionary, vdata, filter, filter_state,
			                               scan_state.dict_count, filter_count);

			// now set all matching tuples to true
			for (idx_t i = 0; i < filter_count; i++) {
				auto idx = dict_sel.get_index(i);
				scan_state.filter_result[idx] = true;
			}
		}
		auto &dict_sel = scan_state.GetSelVec(start, vector_count);
		SelectionVector new_sel(sel_count);
		idx_t approved_tuple_count = 0;
		for (idx_t idx = 0; idx < sel_count; idx++) {
			auto row_idx = sel.get_index(idx);
			auto dict_offset = dict_sel.get_index(row_idx);
			if (!scan_state.filter_result[dict_offset]) {
				// does not pass the filter
				continue;
			}
			new_sel.set_index(approved_tuple_count++, row_idx);
		}
		if (approved_tuple_count < vector_count) {
			sel.Initialize(new_sel);
		}
		sel_count = approved_tuple_count;

		result.Dictionary(*(scan_state.dictionary), scan_state.dict_count, dict_sel, vector_count);
		DictionaryVector::SetDictionaryId(result, to_string(CastPointerToValue(&segment)));
		return;
	}
	// fallback: scan + filter
	DictFSSTCompressionStorage::StringScan(segment, state, vector_count, result);

	UnifiedVectorFormat vdata;
	result.ToUnifiedFormat(vector_count, vdata);
	ColumnSegment::FilterSelection(sel, result, vdata, filter, filter_state, vector_count, sel_count);
}

} // namespace dict_fsst

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction DictFSSTCompressionFun::GetFunction(PhysicalType data_type) {
	auto res = CompressionFunction(
	    CompressionType::COMPRESSION_DICT_FSST, data_type, dict_fsst::DictFSSTCompressionStorage::StringInitAnalyze,
	    dict_fsst::DictFSSTCompressionStorage::StringAnalyze, dict_fsst::DictFSSTCompressionStorage::StringFinalAnalyze,
	    dict_fsst::DictFSSTCompressionStorage::InitCompression, dict_fsst::DictFSSTCompressionStorage::Compress,
	    dict_fsst::DictFSSTCompressionStorage::FinalizeCompress, dict_fsst::DictFSSTCompressionStorage::StringInitScan,
	    dict_fsst::DictFSSTCompressionStorage::StringScan,
	    dict_fsst::DictFSSTCompressionStorage::StringScanPartial<false>,
	    dict_fsst::DictFSSTCompressionStorage::StringFetchRow, UncompressedFunctions::EmptySkip,
	    UncompressedStringStorage::StringInitSegment);
	res.validity = CompressionValidity::NO_VALIDITY_REQUIRED;
	res.select = dict_fsst::DictFSSTSelect;
	res.filter = dict_fsst::DictFSSTFilter;
	return res;
}

bool DictFSSTCompressionFun::TypeIsSupported(const PhysicalType physical_type) {
	return physical_type == PhysicalType::VARCHAR;
}

} // namespace duckdb
