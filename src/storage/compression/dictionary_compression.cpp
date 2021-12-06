#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"

#include <duckdb/main/config.hpp>
#include <duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp>
#include <duckdb/storage/table/append_state.hpp>

namespace duckdb {

struct DictionaryCompressionState : UncompressedCompressState {
	explicit DictionaryCompressionState(ColumnDataCheckpointer &checkpointer)
	    : UncompressedCompressState(checkpointer) {
		auto &db = checkpointer.GetDatabase();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_DICTIONARY, PhysicalType::VARCHAR);
		current_segment->function = function;
		current_string_map = make_unique<std::unordered_map<string, int32_t>>();
	}

	void CreateEmptySegment(idx_t row_start) override {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		compressed_segment->function = function;
		if (type.InternalType() == PhysicalType::VARCHAR) {
			auto &state = (UncompressedStringSegmentState &)*compressed_segment->GetSegmentState();
			state.overflow_writer = make_unique<WriteOverflowStringsToDisk>(db);
		}
		current_segment = move(compressed_segment);

		// Reset the string map
//		current_string_map->clear(); TODO why is this slow?
		current_string_map.reset();
		current_string_map = make_unique<std::unordered_map<string, int32_t>>();
	}

	// TODO initialize with certain size?
	unique_ptr<std::unordered_map<string, int32_t>> current_string_map;
	CompressionFunction *function;
};

struct DictionaryCompressionStorage : UncompressedStringStorage {

	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
	                                                    unique_ptr<AnalyzeState> state);
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct DictionaryCompressionAnalyzeState : public AnalyzeState {
	DictionaryCompressionAnalyzeState() : count(0), total_string_size(0), overflow_strings(0), current_segment_fill(0) {
		current_string_map = make_unique<std::unordered_map<string, int32_t>>();
	}

	idx_t count;
	idx_t total_string_size;
	idx_t overflow_strings;

	unique_ptr<std::unordered_map<string, int32_t>> current_string_map;
	size_t current_segment_fill;
};

unique_ptr<AnalyzeState> DictionaryCompressionStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<DictionaryCompressionAnalyzeState>();
}

bool DictionaryCompressionStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = (DictionaryCompressionAnalyzeState &)state_p;
	VectorData vdata;
	input.Orrify(count, vdata);

	// TODO test analysis
	state.count += count;
	auto data = (string_t *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		state.current_segment_fill += sizeof(int32_t);
		if (vdata.validity.RowIsValid(idx)) {
			auto string_size = data[idx].GetSize();

			if (string_size >= StringUncompressed::STRING_BLOCK_LIMIT) {
				state.overflow_strings++;
				state.current_segment_fill += BIG_STRING_MARKER_SIZE;
			}

			if (state.current_string_map->count(data[idx].GetString()) == 0) {
				state.total_string_size += string_size;
				state.current_segment_fill += string_size;
				state.current_string_map->insert({data[idx].GetString(), string_size});
			}

			// If we have filled a segment size worth of data, we clear the string map to simulate a new segment being
			// used
			// TODO can we do better than this in size estimation?
			if (state.current_segment_fill >= Storage::BLOCK_SIZE) {
				// For some reason, clear is really slow?
//				state.current_string_map->clear();
				state.current_string_map.reset();
				state.current_string_map = make_unique<std::unordered_map<string, int32_t>>();
			}
		}
	}
	return true;
}

idx_t DictionaryCompressionStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = (DictionaryCompressionAnalyzeState &)state_p;
	return state.count * sizeof(int32_t) + state.total_string_size + state.overflow_strings * BIG_STRING_MARKER_SIZE;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
unique_ptr<CompressionState> DictionaryCompressionStorage::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                                           unique_ptr<AnalyzeState> state) {
	return make_unique<DictionaryCompressionState>(checkpointer);
}

void DictionaryCompressionStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (DictionaryCompressionState &)state_p;
	VectorData vdata;
	scan_vector.Orrify(count, vdata);

	ColumnAppendState append_state;
	idx_t offset = 0;
	while (count > 0) {
		idx_t appended = UncompressedStringStorage::StringAppendBase<true>(
		    *state.current_segment, state.current_segment->stats, vdata, offset, count, state.current_string_map.get());
		if (appended == count) {
			// appended everything: finished
			return;
		}
		auto next_start = state.current_segment->start + state.current_segment->count;
		// the segment is full: flush it to disk

		// TODO directly calling the finalize append may be hacky?
		state.FlushSegment(
		    UncompressedStringStorage::FinalizeAppend(*state.current_segment, state.current_segment->stats));

		// now create a new segment and continue appending
		state.CreateEmptySegment(next_start);
		offset += appended;
		count -= appended;
	}
}

void DictionaryCompressionStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = (DictionaryCompressionState &)state_p;
	state.Finalize(UncompressedStringStorage::FinalizeAppend(*state.current_segment, state.current_segment->stats));
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction DictionaryCompressionFun::GetFunction(PhysicalType data_type) {
	return CompressionFunction(
	    CompressionType::COMPRESSION_DICTIONARY, data_type, DictionaryCompressionStorage ::StringInitAnalyze,
	    DictionaryCompressionStorage::StringAnalyze, DictionaryCompressionStorage::StringFinalAnalyze,
	    DictionaryCompressionStorage::InitCompression, DictionaryCompressionStorage::Compress,
	    DictionaryCompressionStorage::FinalizeCompress, UncompressedStringStorage::StringInitScan,
	    UncompressedStringStorage::StringScan, UncompressedStringStorage::StringScanPartial,
	    UncompressedStringStorage::StringFetchRow, UncompressedFunctions::EmptySkip);
}

bool DictionaryCompressionFun::TypeIsSupported(PhysicalType type) {
	return type == PhysicalType::VARCHAR;
}
} // namespace duckdb