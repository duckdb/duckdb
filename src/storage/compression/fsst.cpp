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

	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
	                                                    unique_ptr<AnalyzeState> analyze_state_p);
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);

	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	template <bool ALLOW_FSST_VECTORS = false>
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);

	static void SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container);
	static StringDictionaryContainer GetDictionary(ColumnSegment &segment, BufferHandle &handle);

	static char *FetchStringPointer(StringDictionaryContainer dict, data_ptr_t baseptr, int32_t dict_offset);
	static bp_delta_offsets_t CalculateBpDeltaOffsets(int64_t last_known_row, idx_t start, idx_t scan_count);
	static bool ParseFSSTSegmentHeader(data_ptr_t base_ptr, duckdb_fsst_decoder_t *decoder_out,
	                                   bitpacking_width_t *width_out);
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct FSSTAnalyzeState : public AnalyzeState {
	explicit FSSTAnalyzeState(const CompressionInfo &info)
	    : AnalyzeState(info), count(0), fsst_string_total_size(0), empty_strings(0) {
	}

	~FSSTAnalyzeState() override {
		if (fsst_encoder) {
			duckdb_fsst_destroy(fsst_encoder);
		}
	}

	duckdb_fsst_encoder_t *fsst_encoder = nullptr;
	idx_t count;

	StringHeap fsst_string_heap;
	vector<string_t> fsst_strings;
	size_t fsst_string_total_size;

	RandomEngine random_engine;
	bool have_valid_row = false;

	idx_t empty_strings;
};

unique_ptr<AnalyzeState> FSSTStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	CompressionInfo info(col_data.GetBlockManager().GetBlockSize(), type);
	return make_uniq<FSSTAnalyzeState>(info);
}

bool FSSTStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<FSSTAnalyzeState>();
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	state.count += count;
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);

	// Note that we ignore the sampling in case we have not found any valid strings yet, this solves the issue of
	// not having seen any valid strings here leading to an empty fsst symbol table.
	bool sample_selected = !state.have_valid_row || state.random_engine.NextRandom() < ANALYSIS_SAMPLE_SIZE;

	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);

		if (!vdata.validity.RowIsValid(idx)) {
			continue;
		}

		// We need to check all strings for this, otherwise we run in to trouble during compression if we miss ones
		auto string_size = data[idx].GetSize();
		if (string_size >= StringUncompressed::GetStringBlockLimit(state.info.GetBlockSize())) {
			return false;
		}

		if (!sample_selected) {
			continue;
		}

		if (string_size > 0) {
			state.have_valid_row = true;
			if (data[idx].IsInlined()) {
				state.fsst_strings.push_back(data[idx]);
			} else {
				state.fsst_strings.emplace_back(state.fsst_string_heap.AddBlob(data[idx]));
			}
			state.fsst_string_total_size += string_size;
		} else {
			state.empty_strings++;
		}
	}
	return true;
}

idx_t FSSTStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<FSSTAnalyzeState>();

	size_t compressed_dict_size = 0;
	size_t max_compressed_string_length = 0;

	auto string_count = state.fsst_strings.size();

	if (!string_count) {
		return DConstants::INVALID_INDEX;
	}

	size_t output_buffer_size = 7 + 2 * state.fsst_string_total_size; // size as specified in fsst.h

	vector<size_t> fsst_string_sizes;
	vector<unsigned char *> fsst_string_ptrs;
	for (auto &str : state.fsst_strings) {
		fsst_string_sizes.push_back(str.GetSize());
		fsst_string_ptrs.push_back((unsigned char *)str.GetData()); // NOLINT
	}

	state.fsst_encoder = duckdb_fsst_create(string_count, &fsst_string_sizes[0], &fsst_string_ptrs[0], 0);

	// TODO: do we really need to encode to get a size estimate?
	auto compressed_ptrs = vector<unsigned char *>(string_count, nullptr);
	auto compressed_sizes = vector<size_t>(string_count, 0);
	unique_ptr<unsigned char[]> compressed_buffer(new unsigned char[output_buffer_size]);

	auto res =
	    duckdb_fsst_compress(state.fsst_encoder, string_count, &fsst_string_sizes[0], &fsst_string_ptrs[0],
	                         output_buffer_size, compressed_buffer.get(), &compressed_sizes[0], &compressed_ptrs[0]);

	if (string_count != res) {
		throw std::runtime_error("FSST output buffer is too small unexpectedly");
	}

	// Sum and and Max compressed lengths
	for (auto &size : compressed_sizes) {
		compressed_dict_size += size;
		max_compressed_string_length = MaxValue(max_compressed_string_length, size);
	}
	D_ASSERT(compressed_dict_size ==
	         (uint64_t)(compressed_ptrs[res - 1] - compressed_ptrs[0]) + compressed_sizes[res - 1]);

	auto minimum_width = BitpackingPrimitives::MinimumBitWidth(max_compressed_string_length);
	auto bitpacked_offsets_size =
	    BitpackingPrimitives::GetRequiredSize(string_count + state.empty_strings, minimum_width);

	auto estimated_base_size = double(bitpacked_offsets_size + compressed_dict_size) * (1 / ANALYSIS_SAMPLE_SIZE);
	auto num_blocks = estimated_base_size / double(state.info.GetBlockSize() - sizeof(duckdb_fsst_decoder_t));
	auto symtable_size = num_blocks * sizeof(duckdb_fsst_decoder_t);
	auto estimated_size = estimated_base_size + symtable_size;

	return NumericCast<idx_t>(estimated_size * MINIMUM_COMPRESSION_RATIO);
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//

class FSSTCompressionState : public CompressionState {
public:
	FSSTCompressionState(ColumnDataCheckpointer &checkpointer, const CompressionInfo &info)
	    : CompressionState(info), checkpointer(checkpointer),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_FSST)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	~FSSTCompressionState() override {
		if (fsst_encoder) {
			duckdb_fsst_destroy(fsst_encoder);
		}
	}

	void Reset() {
		index_buffer.clear();
		current_width = 0;
		max_compressed_string_length = 0;
		last_fitting_size = 0;

		// Reset the pointers into the current segment
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		current_handle = buffer_manager.Pin(current_segment->block);
		current_dictionary = FSSTStorage::GetDictionary(*current_segment, current_handle);
		current_end_ptr = current_handle.Ptr() + current_dictionary.end;
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();

		auto compressed_segment =
		    ColumnSegment::CreateTransientSegment(db, type, row_start, info.GetBlockSize(), info.GetBlockSize());
		current_segment = std::move(compressed_segment);
		current_segment->function = function;
		Reset();
	}

	void UpdateState(string_t uncompressed_string, unsigned char *compressed_string, size_t compressed_string_len) {
		if (!HasEnoughSpace(compressed_string_len)) {
			Flush();
			if (!HasEnoughSpace(compressed_string_len)) {
				throw InternalException("FSST string compression failed due to insufficient space in empty block");
			};
		}

		UncompressedStringStorage::UpdateStringStats(current_segment->stats, uncompressed_string);

		// Write string into dictionary
		current_dictionary.size += compressed_string_len;
		auto dict_pos = current_end_ptr - current_dictionary.size;
		memcpy(dict_pos, compressed_string, compressed_string_len);
		current_dictionary.Verify(info.GetBlockSize());

		// We just push the string length to effectively delta encode the strings
		index_buffer.push_back(NumericCast<uint32_t>(compressed_string_len));

		max_compressed_string_length = MaxValue(max_compressed_string_length, compressed_string_len);

		current_width = BitpackingPrimitives::MinimumBitWidth(max_compressed_string_length);
		current_segment->count++;
	}

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

	void AddEmptyString() {
		AddNull();
		UncompressedStringStorage::UpdateStringStats(current_segment->stats, "");
	}

	size_t GetRequiredSize(size_t string_len) {
		bitpacking_width_t required_minimum_width;
		if (string_len > max_compressed_string_length) {
			required_minimum_width = BitpackingPrimitives::MinimumBitWidth(string_len);
		} else {
			required_minimum_width = current_width;
		}

		size_t current_dict_size = current_dictionary.size;
		idx_t current_string_count = index_buffer.size();

		size_t dict_offsets_size =
		    BitpackingPrimitives::GetRequiredSize(current_string_count + 1, required_minimum_width);

		// TODO switch to a symbol table per RowGroup, saves a bit of space
		return sizeof(fsst_compression_header_t) + current_dict_size + dict_offsets_size + string_len +
		       fsst_serialized_symbol_table_size;
	}

	// Checks if there is enough space, if there is, sets last_fitting_size
	bool HasEnoughSpace(size_t string_len) {
		auto required_size = GetRequiredSize(string_len);

		if (required_size <= info.GetBlockSize()) {
			last_fitting_size = required_size;
			return true;
		}
		return false;
	}

	void Flush(bool final = false) {
		auto next_start = current_segment->start + current_segment->count;

		auto segment_size = Finalize();
		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(std::move(current_segment), segment_size);

		if (!final) {
			CreateEmptySegment(next_start);
		}
	}

	idx_t Finalize() {
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		auto handle = buffer_manager.Pin(current_segment->block);
		D_ASSERT(current_dictionary.end == info.GetBlockSize());

		// calculate sizes
		auto compressed_index_buffer_size =
		    BitpackingPrimitives::GetRequiredSize(current_segment->count, current_width);
		auto total_size = sizeof(fsst_compression_header_t) + compressed_index_buffer_size + current_dictionary.size +
		                  fsst_serialized_symbol_table_size;

		if (total_size != last_fitting_size) {
			throw InternalException("FSST string compression failed due to incorrect size calculation");
		}

		// calculate ptr and offsets
		auto base_ptr = handle.Ptr();
		auto header_ptr = reinterpret_cast<fsst_compression_header_t *>(base_ptr);
		auto compressed_index_buffer_offset = sizeof(fsst_compression_header_t);
		auto symbol_table_offset = compressed_index_buffer_offset + compressed_index_buffer_size;

		D_ASSERT(current_segment->count == index_buffer.size());
		BitpackingPrimitives::PackBuffer<sel_t, false>(base_ptr + compressed_index_buffer_offset,
		                                               reinterpret_cast<uint32_t *>(index_buffer.data()),
		                                               current_segment->count, current_width);

		// Write the fsst symbol table or nothing
		if (fsst_encoder != nullptr) {
			memcpy(base_ptr + symbol_table_offset, &fsst_serialized_symbol_table[0], fsst_serialized_symbol_table_size);
		} else {
			memset(base_ptr + symbol_table_offset, 0, fsst_serialized_symbol_table_size);
		}

		Store<uint32_t>(NumericCast<uint32_t>(symbol_table_offset),
		                data_ptr_cast(&header_ptr->fsst_symbol_table_offset));
		Store<uint32_t>((uint32_t)current_width, data_ptr_cast(&header_ptr->bitpacking_width));

		if (total_size >= info.GetCompactionFlushLimit()) {
			// the block is full enough, don't bother moving around the dictionary
			return info.GetBlockSize();
		}

		// the block has space left: figure out how much space we can save
		auto move_amount = info.GetBlockSize() - total_size;
		// move the dictionary so it lines up exactly with the offsets
		auto new_dictionary_offset = symbol_table_offset + fsst_serialized_symbol_table_size;
		memmove(base_ptr + new_dictionary_offset, base_ptr + current_dictionary.end - current_dictionary.size,
		        current_dictionary.size);
		current_dictionary.end -= move_amount;
		D_ASSERT(current_dictionary.end == total_size);
		// write the new dictionary (with the updated "end")
		FSSTStorage::SetDictionary(*current_segment, handle, current_dictionary);

		return total_size;
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;

	// State regarding current segment
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle current_handle;
	StringDictionaryContainer current_dictionary;
	data_ptr_t current_end_ptr;

	// Buffers and map for current segment
	vector<uint32_t> index_buffer;

	size_t max_compressed_string_length;
	bitpacking_width_t current_width;
	idx_t last_fitting_size;

	duckdb_fsst_encoder_t *fsst_encoder = nullptr;
	unsigned char fsst_serialized_symbol_table[sizeof(duckdb_fsst_decoder_t)];
	size_t fsst_serialized_symbol_table_size = sizeof(duckdb_fsst_decoder_t);
};

unique_ptr<CompressionState> FSSTStorage::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                          unique_ptr<AnalyzeState> analyze_state_p) {
	auto &analyze_state = analyze_state_p->Cast<FSSTAnalyzeState>();
	auto compression_state = make_uniq<FSSTCompressionState>(checkpointer, analyze_state.info);

	if (analyze_state.fsst_encoder == nullptr) {
		throw InternalException("No encoder found during FSST compression");
	}

	compression_state->fsst_encoder = analyze_state.fsst_encoder;
	compression_state->fsst_serialized_symbol_table_size =
	    duckdb_fsst_export(compression_state->fsst_encoder, &compression_state->fsst_serialized_symbol_table[0]);
	analyze_state.fsst_encoder = nullptr;

	return std::move(compression_state);
}

void FSSTStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = state_p.Cast<FSSTCompressionState>();

	// Get vector data
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);

	// Collect pointers to strings to compress
	vector<size_t> sizes_in;
	vector<unsigned char *> strings_in;
	size_t total_size = 0;
	idx_t total_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);

		// Note: we treat nulls and empty strings the same
		if (!vdata.validity.RowIsValid(idx) || data[idx].GetSize() == 0) {
			continue;
		}

		total_count++;
		total_size += data[idx].GetSize();
		sizes_in.push_back(data[idx].GetSize());
		strings_in.push_back((unsigned char *)data[idx].GetData()); // NOLINT
	}

	// Only Nulls or empty strings in this vector, nothing to compress
	if (total_count == 0) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				state.AddNull();
			} else if (data[idx].GetSize() == 0) {
				state.AddEmptyString();
			} else {
				throw FatalException("FSST: no encoder found even though there are values to encode");
			}
		}
		return;
	}

	// Compress buffers
	size_t compress_buffer_size = MaxValue<size_t>(total_size * 2 + 7, 1);
	vector<unsigned char *> strings_out(total_count, nullptr);
	vector<size_t> sizes_out(total_count, 0);
	vector<unsigned char> compress_buffer(compress_buffer_size, 0);

	auto res = duckdb_fsst_compress(
	    state.fsst_encoder,   /* IN: encoder obtained from duckdb_fsst_create(). */
	    total_count,          /* IN: number of strings in batch to compress. */
	    &sizes_in[0],         /* IN: byte-lengths of the inputs */
	    &strings_in[0],       /* IN: input string start pointers. */
	    compress_buffer_size, /* IN: byte-length of output buffer. */
	    &compress_buffer[0],  /* OUT: memory buffer to put the compressed strings in (one after the other). */
	    &sizes_out[0],        /* OUT: byte-lengths of the compressed strings. */
	    &strings_out[0]       /* OUT: output string start pointers. Will all point into [output,output+size). */
	);

	if (res != total_count) {
		throw FatalException("FSST compression failed to compress all strings");
	}

	// Push the compressed strings to the compression state one by one
	idx_t compressed_idx = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			state.AddNull();
		} else if (data[idx].GetSize() == 0) {
			state.AddEmptyString();
		} else {
			state.UpdateState(data[idx], strings_out[compressed_idx], sizes_out[compressed_idx]);
			compressed_idx++;
		}
	}
}

void FSSTStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<FSSTCompressionState>();
	state.Flush(true);
}

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

	void StoreLastDelta(uint32_t value, int64_t row) {
		last_known_index = value;
		last_known_row = row;
	}
	void ResetStoredDelta() {
		last_known_index = 0;
		last_known_row = -1;
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

	if (start == 0 || scan_state.last_known_row >= (int64_t)start) {
		scan_state.ResetStoredDelta();
	}

	auto offsets = CalculateBpDeltaOffsets(scan_state.last_known_row, start, scan_count);

	auto bitunpack_buffer = unique_ptr<uint32_t[]>(new uint32_t[offsets.total_bitunpack_count]);
	BitUnpackRange(base_data, data_ptr_cast(bitunpack_buffer.get()), offsets.total_bitunpack_count,
	               offsets.bitunpack_start_row, scan_state.current_width);
	auto delta_decode_buffer = unique_ptr<uint32_t[]>(new uint32_t[offsets.total_delta_decode_count]);
	DeltaDecodeIndices(bitunpack_buffer.get() + offsets.bitunpack_alignment_offset, delta_decode_buffer.get(),
	                   offsets.total_delta_decode_count, scan_state.last_known_index);

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
			uint32_t str_len = bitunpack_buffer[i + offsets.scan_offset];
			auto str_ptr = FSSTStorage::FetchStringPointer(
			    dict, baseptr,
			    UnsafeNumericCast<int32_t>(delta_decode_buffer[i + offsets.unused_delta_decoded_values]));

			if (str_len > 0) {
				result_data[i + result_offset] = FSSTPrimitives::DecompressValue(
				    scan_state.duckdb_fsst_decoder.get(), result, str_ptr, str_len, scan_state.decompress_buffer);
			} else {
				result_data[i + result_offset] = string_t(nullptr, 0);
			}
		}
	}

	scan_state.StoreLastDelta(delta_decode_buffer[scan_count + offsets.unused_delta_decoded_values - 1],
	                          UnsafeNumericCast<int64_t>(start + scan_count - 1));
}

void FSSTStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	StringScanPartial<true>(segment, state, scan_count, result, 0);
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

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction FSSTFun::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::VARCHAR);
	return CompressionFunction(
	    CompressionType::COMPRESSION_FSST, data_type, FSSTStorage::StringInitAnalyze, FSSTStorage::StringAnalyze,
	    FSSTStorage::StringFinalAnalyze, FSSTStorage::InitCompression, FSSTStorage::Compress,
	    FSSTStorage::FinalizeCompress, FSSTStorage::StringInitScan, FSSTStorage::StringScan,
	    FSSTStorage::StringScanPartial<false>, FSSTStorage::StringFetchRow, UncompressedFunctions::EmptySkip);
}

bool FSSTFun::TypeIsSupported(const CompressionInfo &info) {
	return info.GetPhysicalType() == PhysicalType::VARCHAR;
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
