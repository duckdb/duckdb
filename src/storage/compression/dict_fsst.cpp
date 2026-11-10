#include "duckdb/storage/compression/dict_fsst/common.hpp"
#include "duckdb/storage/compression/dict_fsst/analyze.hpp"
#include "duckdb/storage/compression/dict_fsst/compression.hpp"
#include "duckdb/storage/compression/dict_fsst/decompression.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/storage/table/column_data.hpp"

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
	static bool StringAnalyze(AnalyzeState &state_p, const Vector &input);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointData &checkpoint_data,
	                                                    unique_ptr<AnalyzeState> state);
	static void Compress(CompressionState &state_p, const Vector &scan_vector);
	static void FinalizeCompress(CompressionState &state_p);

	static unique_ptr<SegmentScanState> StringInitScan(Context &context, ColumnSegment &segment);
	template <bool ALLOW_DICT_VECTORS>
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
unique_ptr<AnalyzeState> DictFSSTCompressionStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	auto &storage_manager = col_data.GetStorageManager();
	if (StorageManager::IsPriorToVersion(StorageVersion::V1_3_0, storage_manager.GetStorageVersion())) {
		// dict_fsst not introduced yet, disable it
		return nullptr;
	}

	return make_uniq<DictFSSTAnalyzeState>(col_data.GetBlockManager());
}

bool DictFSSTCompressionStorage::StringAnalyze(AnalyzeState &state_p, const Vector &input) {
	auto &analyze_state = state_p.Cast<DictFSSTAnalyzeState>();
	return analyze_state.Analyze(input);
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

void DictFSSTCompressionStorage::Compress(CompressionState &state_p, const Vector &scan_vector) {
	auto &state = state_p.Cast<DictFSSTCompressionState>();
	state.Compress(scan_vector);
}

void DictFSSTCompressionStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<DictFSSTCompressionState>();
	state.Flush(true);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
unique_ptr<SegmentScanState> DictFSSTCompressionStorage::StringInitScan(Context &context, ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto state = make_uniq<CompressedStringScanState>(segment, buffer_manager.Pin(segment.GetBlockHandle()));
	state->Initialize();

	const auto &stats = segment.GetStats();
	if (stats.GetStatsType() == StatisticsType::STRING_STATS && StringStats::HasMaxStringLength(stats)) {
		state->all_values_inlined = StringStats::MaxStringLength(stats) <= string_t::INLINE_LENGTH;
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

	auto start = state.GetPositionInSegment();
	if (!ALLOW_DICT_VECTORS || !scan_state.AllowDictionaryScan(scan_count)) {
		scan_state.ScanToFlatVector(result, result_offset, start, scan_count, state.string_scan_type);
	} else {
		scan_state.ScanToDictionaryVector(segment, result, result_offset, start, scan_count);
	}
}

void DictFSSTCompressionStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                            Vector &result) {
	StringScanPartial<true>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
static bool PreferSparseDictionary(const ColumnSegment &segment, const CompressedStringScanState &scan_state,
                                   idx_t vector_count, idx_t sel_count) {
	return scan_state.layout.mode == DictFSSTMode::DICT_FSST && scan_state.layout.dict_count >= 512 &&
	       idx_t(scan_state.layout.dict_count) * 8 >= segment.count.load() && sel_count <= vector_count / 64;
}

void DictFSSTSelect(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
                    const SelectionVector &sel, idx_t sel_count, idx_t result_offset, ScanVectorType scan_type) {
	D_ASSERT(sel_count <= vector_count);
	D_ASSERT(sel_count == 0 || sel.get_index(sel_count - 1) < vector_count);
	auto &scan_state = state.scan_state->Cast<CompressedStringScanState>();
	if (!scan_state.dictionary && PreferSparseDictionary(segment, scan_state, vector_count, sel_count)) {
		scan_state.Select(result, result_offset, state.GetPositionInSegment(), sel, sel_count,
		                  StringScanType::COPY_STRINGS);
		return;
	}
	if (scan_state.layout.mode != DictFSSTMode::FSST_ONLY && scan_type == ScanVectorType::SCAN_ENTIRE_VECTOR) {
		if (sel_count <= vector_count / 64) {
			scan_state.SelectToDictionaryVector(result, state.GetPositionInSegment(), vector_count, sel, sel_count);
		} else {
			DictFSSTCompressionStorage::StringScan(segment, state, vector_count, result);
			result.Slice(sel, sel_count);
		}
		return;
	}
	scan_state.Select(result, result_offset, state.GetPositionInSegment(), sel, sel_count, state.string_scan_type);
}

//===--------------------------------------------------------------------===//
// Filter
//===--------------------------------------------------------------------===//
static void DictFSSTFilter(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
                           SelectionVector &sel, idx_t &sel_count, const TableFilter &filter,
                           TableFilterState &filter_state) {
	D_ASSERT(sel_count <= vector_count);
	auto &scan_state = state.scan_state->Cast<CompressedStringScanState>();
	auto start = state.GetPositionInSegment();
	const bool sparse_dictionary = PreferSparseDictionary(segment, scan_state, vector_count, sel_count) &&
	                               (!scan_state.filter_cache || !scan_state.filter_cache->IsValidFor(filter_state));
	if (sparse_dictionary || (scan_state.layout.mode == DictFSSTMode::FSST_ONLY && sel_count <= vector_count / 2)) {
		SelectionVector input_rows(sel_count);
		SelectionVector compact_sel(sel_count);
		for (idx_t i = 0; i < sel_count; i++) {
			input_rows.set_index(i, sel.get_index(i));
			compact_sel.set_index(i, i);
		}
		Vector decoded(result.GetType());
		scan_state.Select(decoded, 0, start, input_rows, sel_count,
		                  sparse_dictionary && !scan_state.dictionary ? StringScanType::COPY_STRINGS
		                                                              : state.string_scan_type);
		const auto decoded_count = sel_count;
		ColumnSegment::FilterSelection(compact_sel, decoded, filter_state, decoded_count, sel_count);
		if (sel_count == 0) {
			return;
		}

		// Restore the original row positions expected by the scan.
		StringVector::AddHeapReference(result, decoded);
		if (scan_state.dictionary) {
			StringVector::AddHeapReference(result, scan_state.dictionary->data);
		}
		auto values = decoded.Values<string_t>();
		auto writer = FlatVector::Writer<string_t>(result, vector_count);
		idx_t survivor = 0;
		for (idx_t row = 0; row < vector_count; row++) {
			if (survivor == sel_count || input_rows.get_index(compact_sel.get_index(survivor)) != row) {
				writer.WriteNull();
				continue;
			}
			auto value = values[compact_sel.get_index(survivor)];
			if (value.IsValid()) {
				writer.WriteStringRef(value.GetValue());
			} else {
				writer.WriteNull();
			}
			sel.set_index(survivor++, row);
		}
		D_ASSERT(survivor == sel_count);
		return;
	}
	if (scan_state.AllowDictionaryScan(vector_count)) {
		if (!scan_state.filter_cache || !scan_state.filter_cache->IsValidFor(filter_state)) {
			scan_state.filter_cache =
			    filter_state.CreateCache(scan_state.dictionary->data, scan_state.layout.dict_count);
		}
		const auto &dict_sel = scan_state.GetSelVec(start, vector_count);
		scan_state.filter_cache->Filter(dict_sel, sel, sel_count);
		result.Dictionary(scan_state.dictionary, dict_sel, vector_count);
		return;
	}
	// fallback: scan + filter
	DictFSSTCompressionStorage::StringScan(segment, state, vector_count, result);
	ColumnSegment::FilterSelection(sel, result, filter_state, vector_count, sel_count);
}

static string DictFSSTModeToString(const DictFSSTMode mode) {
	switch (mode) {
	case DictFSSTMode::DICTIONARY:
		return "DICTIONARY";
	case DictFSSTMode::DICT_FSST:
		return "DICT_FSST";
	case DictFSSTMode::FSST_ONLY:
		return "FSST_ONLY";
	default:
		return "UNKNOWN";
	}
}

//===--------------------------------------------------------------------===//
// GetSegmentInfo
//===--------------------------------------------------------------------===//
static InsertionOrderPreservingMap<string> DictFSSTGetSegmentInfo(Context &, ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto state = make_uniq<CompressedStringScanState>(segment, buffer_manager.Pin(segment.GetBlockHandle()));
	state->Initialize();

	const auto tuple_count = segment.count.load();

	InsertionOrderPreservingMap<string> result;
	result[DictFSSTModeToString(state->layout.mode)] = StringUtil::Format("%d", tuple_count);
	return result;
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
	    dict_fsst::DictFSSTCompressionStorage::StringScanPartial<false>, UncompressedFunctions::EmptySkip,
	    UncompressedStringStorage::StringInitSegment);
	res.validity = CompressionValidity::NO_VALIDITY_REQUIRED;
	res.select = dict_fsst::DictFSSTSelect;
	res.filter = dict_fsst::DictFSSTFilter;
	res.get_segment_info = dict_fsst::DictFSSTGetSegmentInfo;
	res.zero_initialize_segments = false;
	return res;
}

bool DictFSSTCompressionFun::TypeIsSupported(const PhysicalType physical_type) {
	return physical_type == PhysicalType::VARCHAR;
}

} // namespace duckdb
