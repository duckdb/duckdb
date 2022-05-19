#include "duckdb/common/bitpacking.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "fsst.h"

#include <iostream>

namespace duckdb {

struct StringHash {
	std::size_t operator()(const string &k) const {
		return Hash(k.c_str(), k.size());
	}
};

// Abstract class for keeping compression state either for compression or size analysis
class DictionaryCompressionState : public CompressionState {
public:
	bool UpdateState(Vector &scan_vector, idx_t count, Vector* compressed_vector = nullptr) {
		VectorData vdata;
		scan_vector.Orrify(count, vdata);
		auto data = (string_t *)vdata.data;

		VectorData vdata_compressed;
		string_t* data_compressed;
		if (compressed_vector) {
			compressed_vector->Orrify(count, vdata_compressed);
			data_compressed = (string_t *)vdata_compressed.data;
		}

		Verify();
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			size_t string_size = 0;
			bool new_string = false;
			auto row_is_valid = vdata.validity.RowIsValid(idx);

			if (row_is_valid) {
				if (compressed_vector) {
					string_size = data_compressed[idx].GetSize();
				} else {
					string_size = data[idx].GetSize();
				}
				if (string_size >= StringUncompressed::STRING_BLOCK_LIMIT) {
					// Big strings not implemented for dictionary compression
					return false;
				}
				new_string = !LookupString(data[idx]);
			}
			bool fits = HasEnoughSpace(new_string, string_size);
			if (!fits) {
				Flush();
				new_string = true;
				D_ASSERT(HasEnoughSpace(new_string, string_size));
			}

			if (!row_is_valid) {
				AddNull();
			} else if (new_string) {
				if (compressed_vector){
					AddNewString(data[idx], data_compressed[idx]);
				} else {
					AddNewString(data[idx], data[idx]);
				}

			} else {
				AddLastLookup();
			}

			Verify();
		}

		return true;
	}

protected:
	// Should verify the State
	virtual void Verify() = 0;
	// Performs a lookup of str, storing the result internally
	virtual bool LookupString(string_t str) = 0;
	// Add the most recently looked up str to compression state
	virtual void AddLastLookup() = 0;
	// Add string to the state that is known to not be seen yet
	virtual void AddNewString(string_t str, string_t str_compressed) = 0;
	// Add a null value to the compression state
	virtual void AddNull() = 0;
	// Check if we have enough space to add a string
	virtual bool HasEnoughSpace(bool new_string, size_t string_size) = 0;
	// Flush the segment to disk if compressing or reset the counters if analyzing
	virtual void Flush(bool final = false) = 0;
};

typedef struct {
	uint32_t dict_size;
	uint32_t dict_end;
	uint32_t index_buffer_offset;
	uint32_t index_buffer_count;
	uint32_t bitpacking_width;
	uint32_t fsst_symbol_table_offset;
} dictionary_compression_header_t;

struct DictionaryCompressionStorage {
	static constexpr float MINIMUM_COMPRESSION_RATIO = 1.2;
	static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(dictionary_compression_header_t);
	static constexpr size_t COMPACTION_FLUSH_LIMIT = (size_t)Storage::BLOCK_SIZE / 5 * 4;

	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
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

	static bool HasEnoughSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
	                           bitpacking_width_t packing_width);
	static idx_t RequiredSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
	                           bitpacking_width_t packing_width);

	static StringDictionaryContainer GetDictionary(ColumnSegment &segment, BufferHandle &handle);
	static void SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container);
	static string_t FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict, data_ptr_t baseptr,
	                                    int32_t dict_offset, uint16_t string_len);
	static uint16_t GetStringLength(uint32_t *index_buffer_ptr, sel_t index);
};

// Dictionary compression uses a combination of bitpacking and a dictionary to compress string segments. The data is
// stored across three buffers: the index buffer, the selection buffer and the dictionary. Firstly the Index buffer
// contains the offsets into the dictionary which are also used to determine the string lengths. Each value in the
// dictionary gets a single unique index in the index buffer. Secondly, the selection buffer maps the tuples to an index
// in the index buffer. The selection buffer is compressed with bitpacking. Finally, the dictionary contains simply all
// the unique strings without lenghts or null termination as we can deduce the lengths from the index buffer. The
// addition of the selection buffer is done for two reasons: firstly, to allow the scan to emit dictionary vectors by
// scanning the whole dictionary at once and then scanning the selection buffer for each emitted vector. Secondly, it
// allows for efficient bitpacking compression as the selection values should remain relatively small.
struct DictionaryCompressionCompressState : public DictionaryCompressionState {
	explicit DictionaryCompressionCompressState(ColumnDataCheckpointer &checkpointer) : checkpointer(checkpointer) {
		auto &db = checkpointer.GetDatabase();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_DICTIONARY, PhysicalType::VARCHAR);
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		current_segment = move(compressed_segment);

		current_segment->function = function;

		// Reset the buffers and string map
		current_string_map.clear();
		index_buffer.clear();
		index_buffer.push_back(0); // Reserve index 0 for null strings
		selection_buffer.clear();

		current_width = 0;
		next_width = 0;

		// Reset the pointers into the current segment
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		current_handle = buffer_manager.Pin(current_segment->block);
		current_dictionary = DictionaryCompressionStorage::GetDictionary(*current_segment, *current_handle);
		current_end_ptr = current_handle->node->buffer + current_dictionary.end;
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;

	// State regarding current segment
	unique_ptr<ColumnSegment> current_segment;
	unique_ptr<BufferHandle> current_handle;
	StringDictionaryContainer current_dictionary;
	data_ptr_t current_end_ptr;

	// Buffers and map for current segment
	std::unordered_map<string, uint32_t, StringHash> current_string_map;
	std::vector<uint32_t> index_buffer;
	std::vector<uint32_t> selection_buffer;

	bitpacking_width_t current_width = 0;
	bitpacking_width_t next_width = 0;

	fsst_encoder_t* fsst_encoder;
	unsigned char fsst_serialized_symbol_table[sizeof(fsst_decoder_t)];
	size_t fsst_serialized_symbol_table_size;

	// Result of latest LookupString call
	uint32_t latest_lookup_result;

	void Verify() override {
		current_dictionary.Verify();
		D_ASSERT(current_segment->count == selection_buffer.size());
		D_ASSERT(DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load(), index_buffer.size(),
		                                                      current_dictionary.size, current_width));
		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);
		D_ASSERT(index_buffer.size() == current_string_map.size() + 1); // +1 is for null value
	}

	bool LookupString(string_t str) override {
		auto search = current_string_map.find(str.GetString());
		auto has_result = search != current_string_map.end();

		if (has_result) {
			latest_lookup_result = search->second;
		}
		return has_result;
	}

	void AddNewString(string_t str, string_t str_compressed) override {
		UncompressedStringStorage::UpdateStringStats(current_segment->stats, str);

		// Copy string to dict
		current_dictionary.size += str_compressed.GetSize();
		auto dict_pos = current_end_ptr - current_dictionary.size;
		memcpy(dict_pos, str_compressed.GetDataUnsafe(), str_compressed.GetSize());
		current_dictionary.Verify();
		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);

		// Update buffers and map
		index_buffer.push_back(current_dictionary.size);
		selection_buffer.push_back(index_buffer.size() - 1);
		current_string_map.insert({str.GetString(), index_buffer.size() - 1});
		DictionaryCompressionStorage::SetDictionary(*current_segment, *current_handle, current_dictionary);

		current_width = next_width;
		current_segment->count++;
	}

	void AddNull() override {
		selection_buffer.push_back(0);
		current_segment->count++;
	}

	void AddLastLookup() override {
		selection_buffer.push_back(latest_lookup_result);
		current_segment->count++;
	}

	bool HasEnoughSpace(bool new_string, size_t string_size) override {
		if (new_string) {
			next_width = BitpackingPrimitives::MinimumBitWidth(index_buffer.size() - 1 + new_string);
			return DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load() + 1,
			                                                    index_buffer.size() + 1,
			                                                    fsst_serialized_symbol_table_size + current_dictionary.size + string_size, next_width);
		} else {
			return DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load() + 1, index_buffer.size(),
			                                                    fsst_serialized_symbol_table_size + current_dictionary.size, current_width);
		}
	}

	void Flush(bool final = false) override {
		auto next_start = current_segment->start + current_segment->count;

		auto segment_size = Finalize();
		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(move(current_segment), segment_size);

		if (!final) {
			CreateEmptySegment(next_start);
		}
	}

	idx_t Finalize() {
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		auto handle = buffer_manager.Pin(current_segment->block);
		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);

		// calculate sizes
		auto compressed_selection_buffer_size =
		    BitpackingPrimitives::GetRequiredSize<sel_t>(current_segment->count, current_width);
		auto index_buffer_size = index_buffer.size() * sizeof(uint32_t);
		auto total_size = DictionaryCompressionStorage::DICTIONARY_HEADER_SIZE + compressed_selection_buffer_size +
		                  index_buffer_size + current_dictionary.size + fsst_serialized_symbol_table_size;

		// calculate ptr and offsets
		auto base_ptr = handle->node->buffer;
		auto header_ptr = (dictionary_compression_header_t *)base_ptr;
		auto compressed_selection_buffer_offset = DictionaryCompressionStorage::DICTIONARY_HEADER_SIZE;
		auto index_buffer_offset = compressed_selection_buffer_offset + compressed_selection_buffer_size;
		auto symbol_table_offset = index_buffer_offset + index_buffer_size;

		// Write compressed selection buffer
		BitpackingPrimitives::PackBuffer<sel_t, false>(base_ptr + compressed_selection_buffer_offset,
		                                               (sel_t *)(selection_buffer.data()), current_segment->count,
		                                               current_width);

		// Write the index buffer
		memcpy(base_ptr + index_buffer_offset, index_buffer.data(), index_buffer_size);

		// Write the fsst symbol table
		memcpy(base_ptr + symbol_table_offset, &fsst_serialized_symbol_table[0], fsst_serialized_symbol_table_size);

		// Store sizes and offsets in segment header
		Store<uint32_t>(index_buffer_offset, (data_ptr_t)&header_ptr->index_buffer_offset);
		Store<uint32_t>(index_buffer.size(), (data_ptr_t)&header_ptr->index_buffer_count);
		Store<uint32_t>((uint32_t)current_width, (data_ptr_t)&header_ptr->bitpacking_width);
		Store<uint32_t>(symbol_table_offset, (data_ptr_t)&header_ptr->fsst_symbol_table_offset);

		D_ASSERT(current_width == BitpackingPrimitives::MinimumBitWidth(index_buffer.size() - 1));
		D_ASSERT(DictionaryCompressionStorage::HasEnoughSpace(current_segment->count, index_buffer.size(),
		                                                      current_dictionary.size, current_width));
		D_ASSERT((uint64_t)*max_element(std::begin(selection_buffer), std::end(selection_buffer)) ==
		         index_buffer.size() - 1);

		if (total_size >= DictionaryCompressionStorage::COMPACTION_FLUSH_LIMIT) {
			// the block is full enough, don't bother moving around the dictionary
			return Storage::BLOCK_SIZE;
		}
		// the block has space left: figure out how much space we can save
		auto move_amount = Storage::BLOCK_SIZE - total_size;
		// move the dictionary so it lines up exactly with the offsets
		auto new_dictionary_offset = symbol_table_offset + fsst_serialized_symbol_table_size;
		memmove(base_ptr + new_dictionary_offset, base_ptr + current_dictionary.end - current_dictionary.size,
		        current_dictionary.size);
		current_dictionary.end -= move_amount;
		D_ASSERT(current_dictionary.end == total_size);
		// write the new dictionary (with the updated "end")
		DictionaryCompressionStorage::SetDictionary(*current_segment, *handle, current_dictionary);

		return total_size;
	}
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct DictionaryCompressionAnalyzeState : public AnalyzeState, DictionaryCompressionState {
	DictionaryCompressionAnalyzeState()
	    : segment_count(0), current_tuple_count(0), current_unique_count(0), current_dict_size(0), current_width(0),
	      next_width(0) {
	}

	size_t segment_count;
	idx_t current_tuple_count;
	idx_t current_unique_count;
	size_t current_dict_size;
	std::unordered_set<string, StringHash> current_set;

	StringHeap fsst_strings;
	std::vector<size_t> fsst_string_sizes;
	std::vector<unsigned char*> fsst_string_ptrs;
	idx_t fsst_string_total_size = 0;
	fsst_encoder_t* fsst_encoder;

	bitpacking_width_t current_width;
	bitpacking_width_t next_width;

	bool LookupString(string_t str) override {
		return current_set.count(str.GetString());
	}

	void AddNewString(string_t str, string_t str_compressed) override {
		current_tuple_count++;
		current_unique_count++;
		current_dict_size += str.GetSize();
		current_set.insert(str.GetString());
		current_width = next_width;

		// fsst bookkeeping
		// get pointer to new string
		// copy to end of string vector, does this work?
		string_t string_added = fsst_strings.AddString(str);
		fsst_string_sizes.push_back(str.GetSize());
		fsst_string_total_size+=str.GetSize();
		fsst_string_ptrs.push_back((unsigned char*)string_added.GetDataUnsafe());
	}

	void AddLastLookup() override {
		current_tuple_count++;
	}

	void AddNull() override {
		current_tuple_count++;
	}

	bool HasEnoughSpace(bool new_string, size_t string_size) override {
		if (new_string) {
			next_width =
			    BitpackingPrimitives::MinimumBitWidth(current_unique_count + 2); // 1 for null, one for new string
			return DictionaryCompressionStorage::HasEnoughSpace(current_tuple_count + 1, current_unique_count + 1,
			                                                    current_dict_size + string_size, next_width);
		} else {
			return DictionaryCompressionStorage::HasEnoughSpace(current_tuple_count + 1, current_unique_count,
			                                                    current_dict_size, current_width);
		}
	}

	void Flush(bool final = false) override {
		segment_count++;
		current_tuple_count = 0;
		current_unique_count = 0;
		current_dict_size = 0;
		current_set.clear();
	}
	void Verify() override {};
};

unique_ptr<AnalyzeState> DictionaryCompressionStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<DictionaryCompressionAnalyzeState>();
}

bool DictionaryCompressionStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = (DictionaryCompressionAnalyzeState &)state_p;
	return state.UpdateState(input, count);
}

idx_t DictionaryCompressionStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = (DictionaryCompressionAnalyzeState &)state_p;

	auto n = state.fsst_string_sizes.size();
	size_t output_buffer_size = state.fsst_string_total_size * 5; // way too big see fsst api

	state.fsst_encoder = fsst_create(n, &state.fsst_string_sizes[0], &state.fsst_string_ptrs[0], 0);
	unsigned char fsst_symbol_table[sizeof(fsst_decoder_t)];
	auto serialized_symbol_table_size = fsst_export(state.fsst_encoder, &fsst_symbol_table[0]);

	auto compressed_ptrs = std::vector<unsigned char*>(n, 0);
	auto compressed_sizes = std::vector<size_t>(n, 0);
	auto compressed_buffer = std::vector<unsigned char>(output_buffer_size, 0);

	auto res = fsst_compress(state.fsst_encoder,
	                         n,
	                         &state.fsst_string_sizes[0],
	                         &state.fsst_string_ptrs[0],
	                         output_buffer_size,
							 &compressed_buffer[0],
	                         &compressed_sizes[0],
	                         &compressed_ptrs[0]
	                         );


	if (n != res) {
		std::cout << "Res: " << res << "\n";
		throw std::runtime_error("A not all strings were compressed!");
	}

//	std::cout << "\n";
//	std::cout << "Dictionary contains " << n << " strings (total " << state.fsst_string_total_size << " bytes)\n";
//	std::cout << "Compressed size is " << (compressed_ptrs[res-1] - compressed_ptrs[0]) + compressed_sizes[res-1] << "\n";
//	std::cout << "Symbol table size is " << serialized_symbol_table_size << "\n";

	// Quick check that size is correct
	size_t size_sum = 0;
	for (auto& size : compressed_sizes) {
		size_sum += size;
	}
	D_ASSERT(size_sum == (compressed_ptrs[res-1] - compressed_ptrs[0]) + compressed_sizes[res-1]);

	auto width = BitpackingPrimitives::MinimumBitWidth(state.current_unique_count + 1);
	auto req_space =
	    RequiredSpace(state.current_tuple_count, state.current_unique_count, state.current_dict_size, width);

	return MINIMUM_COMPRESSION_RATIO * (state.segment_count * Storage::BLOCK_SIZE + req_space);
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
unique_ptr<CompressionState> DictionaryCompressionStorage::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                                           unique_ptr<AnalyzeState> analyze_state_p) {
	auto analyze_state = dynamic_cast<DictionaryCompressionAnalyzeState*>(analyze_state_p.get());
	auto compression_state = make_unique<DictionaryCompressionCompressState>(checkpointer);
	compression_state->fsst_encoder = analyze_state->fsst_encoder;

	compression_state->fsst_serialized_symbol_table_size = fsst_export(
	    compression_state->fsst_encoder,
	    &compression_state->fsst_serialized_symbol_table[0]);

	return compression_state;
}

void DictionaryCompressionStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (DictionaryCompressionCompressState &)state_p;

	// Get vector data
	VectorData vdata;
	scan_vector.Orrify(count, vdata);
	auto data = (string_t *)vdata.data;

	// TODO this part below compresses a vector using fsst, note however that the compressed strings are not

	// Copy strings to compress
	vector<size_t> sizes_in;
	vector<unsigned char*> strings_in;

	// Transform string_t vector to char* array and size_t array
	size_t total_size = 0;
	idx_t total_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		auto row_is_valid = vdata.validity.RowIsValid(idx);
		if (row_is_valid) {
			total_count++;
			total_size += data[idx].GetSize();
			sizes_in.push_back(data[idx].GetSize());
			strings_in.push_back((unsigned char*) data[idx].GetDataUnsafe());
		}
	}

	// Compress buffer
	size_t compress_buffer_size = total_size * 3 + 7;
	vector<unsigned char*> strings_out(total_count, nullptr);
	vector<size_t> sizes_out(total_count, 0);
	vector<unsigned char> compress_buffer(compress_buffer_size, 0);

	auto res = fsst_compress(
	    state.fsst_encoder, 	/* IN: encoder obtained from fsst_create(). */
		total_count,         	/* IN: number of strings in batch to compress. */
		&sizes_in[0],          		/* IN: byte-lengths of the inputs */
	    &strings_in[0],  		/* IN: input string start pointers. */
		compress_buffer_size,   /* IN: byte-length of output buffer. */
	    &compress_buffer[0],   	/* OUT: memorxy buffer to put the compressed strings in (one after the other). */
	    &sizes_out[0],         	/* OUT: byte-lengths of the compressed strings. */
		&strings_out[0]  		/* OUT: output string start pointers. Will all point into [output,output+size). */
	);

	if (res != total_count) {
		throw std::runtime_error("Not all strings were compressed!");
	}

	// Create new vector for compressed data
	Vector compressed_vector(scan_vector.GetType(), true, false);
	compressed_vector.Slice(*vdata.sel, count);
	VectorData vdata_compressed;
	compressed_vector.Orrify(count, vdata_compressed);
	auto data_compressed = (string_t*)vdata_compressed.data;

	// Insert compressed strings into compressed vector
	size_t compressed_idx = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata_compressed.sel->get_index(i);
		auto row_is_valid = vdata.validity.RowIsValid(idx);
		if (row_is_valid) {
			data_compressed[idx] = string_t((const char*)strings_out[compressed_idx], sizes_out[compressed_idx]);
			compressed_idx++;
		}
	}

	state.UpdateState(scan_vector, count, &compressed_vector);
	//TODO: compressed strings go out of scope here. We can use StringVector::AddHandle() to achieve this if necessary
}

void DictionaryCompressionStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = (DictionaryCompressionCompressState &)state_p;
	state.Flush(true);
	fsst_destroy(state.fsst_encoder);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct CompressedStringScanState : public StringScanState {
	unique_ptr<BufferHandle> handle;
	buffer_ptr<Vector> dictionary;
	bitpacking_width_t current_width;
	buffer_ptr<SelectionVector> sel_vec;
	idx_t sel_vec_size = 0;
	buffer_ptr<fsst_decoder_t> fsst_decoder;
};

unique_ptr<SegmentScanState> DictionaryCompressionStorage::StringInitScan(ColumnSegment &segment) {
	auto state = make_unique<CompressedStringScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	state->handle = buffer_manager.Pin(segment.block);

	auto baseptr = state->handle->node->buffer + segment.GetBlockOffset();

	// Load header values
	auto dict = DictionaryCompressionStorage::GetDictionary(segment, *(state->handle));
	auto header_ptr = (dictionary_compression_header_t *)baseptr;
	auto index_buffer_offset = Load<uint32_t>((data_ptr_t)&header_ptr->index_buffer_offset);
	auto index_buffer_count = Load<uint32_t>((data_ptr_t)&header_ptr->index_buffer_count);
	state->current_width = (bitpacking_width_t)(Load<uint32_t>((data_ptr_t)&header_ptr->bitpacking_width));
	auto fsst_symbol_table_offset = Load<uint32_t>((data_ptr_t)&header_ptr->fsst_symbol_table_offset);

	// Import fsst decoder
	state->fsst_decoder = make_buffer<fsst_decoder_t>();
	auto retval = fsst_import(state->fsst_decoder.get(), baseptr + fsst_symbol_table_offset);
	if (retval == 0){
		throw std::runtime_error("Error decoding fsst symbol table");
	}

	auto index_buffer_ptr = (uint32_t *)(baseptr + index_buffer_offset);

	state->dictionary = make_buffer<Vector>(segment.type, index_buffer_count);
	auto dict_child_data = FlatVector::GetData<string_t>(*(state->dictionary));

	for (uint32_t i = 0; i < index_buffer_count; i++) {
		// NOTE: the passing of dict_child_vector, will not be used, its for big strings
		uint16_t str_len = GetStringLength(index_buffer_ptr, i);
		dict_child_data[i] = FetchStringFromDict(segment, dict, baseptr, index_buffer_ptr[i], str_len);
	}

	return move(state);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <bool ALLOW_DICT_VECTORS>
void DictionaryCompressionStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                                     Vector &result, idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = (CompressedStringScanState &)*state.scan_state;
	auto start = segment.GetRelativeIndex(state.row_index);

	auto baseptr = scan_state.handle->node->buffer + segment.GetBlockOffset();
	auto dict = DictionaryCompressionStorage::GetDictionary(segment, *scan_state.handle);

	auto header_ptr = (dictionary_compression_header_t *)baseptr;
	auto index_buffer_offset = Load<uint32_t>((data_ptr_t)&header_ptr->index_buffer_offset);
	auto index_buffer_ptr = (uint32_t *)(baseptr + index_buffer_offset);

	auto base_data = (data_ptr_t)(baseptr + DICTIONARY_HEADER_SIZE);
	result.SetVectorType(VectorType::FSST_VECTOR);
	auto result_data = FSSTVector::GetCompressedData<string_t>(result);

	if (true || !ALLOW_DICT_VECTORS || scan_count != STANDARD_VECTOR_SIZE ||
	    start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE != 0) {
		// Emit regular vector

		// Handling non-bitpacking-group-aligned start values;
		idx_t start_offset = start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		// We will scan in blocks of BITPACKING_ALGORITHM_GROUP_SIZE, so we may scan some extra values.
		idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count + start_offset);

		// Create a decompression buffer of sufficient size if we don't already have one.
		if (!scan_state.sel_vec || scan_state.sel_vec_size < decompress_count) {
			scan_state.sel_vec_size = decompress_count;
			scan_state.sel_vec = make_buffer<SelectionVector>(decompress_count);
		}

		data_ptr_t src = &base_data[((start - start_offset) * scan_state.current_width) / 8];
		sel_t *sel_vec_ptr = scan_state.sel_vec->data();

		BitpackingPrimitives::UnPackBuffer<sel_t>((data_ptr_t)sel_vec_ptr, src, decompress_count,
		                                          scan_state.current_width);

		for (idx_t i = 0; i < scan_count; i++) {
			// Lookup dict offset in index buffer
			auto string_number = scan_state.sel_vec->get_index(i + start_offset);
			auto dict_offset = index_buffer_ptr[string_number];
			uint16_t str_len = GetStringLength(index_buffer_ptr, string_number);
			result_data[result_offset + i] = FetchStringFromDict(segment, dict, baseptr, dict_offset, str_len);
		}

		FSSTVector::RegisterDecoder(result, scan_state.fsst_decoder);
	} else {
		D_ASSERT(start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE == 0);
		D_ASSERT(scan_count == STANDARD_VECTOR_SIZE);
		D_ASSERT(result_offset == 0);

		idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count);

		// Create a selection vector of sufficient size if we don't already have one.
		if (!scan_state.sel_vec || scan_state.sel_vec_size < decompress_count) {
			scan_state.sel_vec_size = decompress_count;
			scan_state.sel_vec = make_buffer<SelectionVector>(decompress_count);
		}

		// Scanning 1024 values, emitting a dict vector
		data_ptr_t dst = (data_ptr_t)(scan_state.sel_vec->data());
		data_ptr_t src = (data_ptr_t)&base_data[(start * scan_state.current_width) / 8];

		BitpackingPrimitives::UnPackBuffer<sel_t>(dst, src, scan_count, scan_state.current_width);

		result.Slice(*(scan_state.dictionary), *scan_state.sel_vec, scan_count);
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
	// first pin the main buffer if it is not already pinned
	auto primary_id = segment.block->BlockId();

	BufferHandle *handle_ptr;
	auto entry = state.handles.find(primary_id);
	if (entry == state.handles.end()) {
		// not pinned yet: pin it
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		auto handle = buffer_manager.Pin(segment.block);
		handle_ptr = handle.get();
		state.handles[primary_id] = move(handle);
	} else {
		// already pinned: use the pinned handle
		handle_ptr = entry->second.get();
	}

	auto baseptr = handle_ptr->node->buffer + segment.GetBlockOffset();
	auto header_ptr = (dictionary_compression_header_t *)baseptr;
	auto dict = DictionaryCompressionStorage::GetDictionary(segment, *handle_ptr);
	auto index_buffer_offset = Load<uint32_t>((data_ptr_t)&header_ptr->index_buffer_offset);
	auto width = (bitpacking_width_t)(Load<uint32_t>((data_ptr_t)&header_ptr->bitpacking_width));
	auto index_buffer_ptr = (uint32_t *)(baseptr + index_buffer_offset);
	auto base_data = (data_ptr_t)(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<string_t>(result);

	// Handling non-bitpacking-group-aligned start values;
	idx_t start_offset = row_id % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

	// Decompress part of selection buffer we need for this value.
	sel_t decompression_buffer[BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE];
	data_ptr_t src = (data_ptr_t)&base_data[((row_id - start_offset) * width) / 8];
	BitpackingPrimitives::UnPackBuffer<sel_t>((data_ptr_t)decompression_buffer, src,
	                                          BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE, width);

	auto selection_value = decompression_buffer[start_offset];
	auto dict_offset = index_buffer_ptr[selection_value];
	uint16_t str_len = GetStringLength(index_buffer_ptr, selection_value);

	result_data[result_idx] = FetchStringFromDict(segment, dict, baseptr, dict_offset, str_len);
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
bool DictionaryCompressionStorage::HasEnoughSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
                                                  bitpacking_width_t packing_width) {
	return RequiredSpace(current_count, index_count, dict_size, packing_width) <= Storage::BLOCK_SIZE;
}

idx_t DictionaryCompressionStorage::RequiredSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
                                                  bitpacking_width_t packing_width) {
	idx_t base_space = DICTIONARY_HEADER_SIZE + dict_size;
	idx_t string_number_space = BitpackingPrimitives::GetRequiredSize<sel_t>(current_count, packing_width);
	idx_t index_space = index_count * sizeof(uint32_t);

	idx_t used_space = base_space + index_space + string_number_space;

	return used_space;
}

StringDictionaryContainer DictionaryCompressionStorage::GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto header_ptr = (dictionary_compression_header_t *)(handle.node->buffer + segment.GetBlockOffset());
	StringDictionaryContainer container;
	container.size = Load<uint32_t>((data_ptr_t)&header_ptr->dict_size);
	container.end = Load<uint32_t>((data_ptr_t)&header_ptr->dict_end);
	return container;
}

void DictionaryCompressionStorage::SetDictionary(ColumnSegment &segment, BufferHandle &handle,
                                                 StringDictionaryContainer container) {
	auto header_ptr = (dictionary_compression_header_t *)(handle.node->buffer + segment.GetBlockOffset());
	Store<uint32_t>(container.size, (data_ptr_t)&header_ptr->dict_size);
	Store<uint32_t>(container.end, (data_ptr_t)&header_ptr->dict_end);
}

string_t DictionaryCompressionStorage::FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict,
                                                           data_ptr_t baseptr, int32_t dict_offset,
                                                           uint16_t string_len) {
	D_ASSERT(dict_offset >= 0 && dict_offset <= Storage::BLOCK_SIZE);

	if (dict_offset == 0) {
		return string_t(nullptr, 0);
	}
	// normal string: read string from this block
	auto dict_end = baseptr + dict.end;
	auto dict_pos = dict_end - dict_offset;

	auto str_ptr = (char *)(dict_pos);
	return string_t(str_ptr, string_len);
}

uint16_t DictionaryCompressionStorage::GetStringLength(uint32_t *index_buffer_ptr, sel_t index) {
	if (index == 0) {
		return 0;
	} else {
		return index_buffer_ptr[index] - index_buffer_ptr[index - 1];
	}
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction DictionaryCompressionFun::GetFunction(PhysicalType data_type) {
	return CompressionFunction(
	    CompressionType::COMPRESSION_DICTIONARY, data_type, DictionaryCompressionStorage ::StringInitAnalyze,
	    DictionaryCompressionStorage::StringAnalyze, DictionaryCompressionStorage::StringFinalAnalyze,
	    DictionaryCompressionStorage::InitCompression, DictionaryCompressionStorage::Compress,
	    DictionaryCompressionStorage::FinalizeCompress, DictionaryCompressionStorage::StringInitScan,
	    DictionaryCompressionStorage::StringScan, DictionaryCompressionStorage::StringScanPartial<false>,
	    DictionaryCompressionStorage::StringFetchRow, UncompressedFunctions::EmptySkip);
}

bool DictionaryCompressionFun::TypeIsSupported(PhysicalType type) {
	return type == PhysicalType::VARCHAR;
}
} // namespace duckdb