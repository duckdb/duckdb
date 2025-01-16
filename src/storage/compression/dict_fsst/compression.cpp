#include "duckdb/storage/compression/dict_fsst/compression.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "fsst.h"

namespace duckdb {
namespace dict_fsst {

DictFSSTCompressionCompressState::DictFSSTCompressionCompressState(ColumnDataCheckpointData &checkpoint_data_p,
                                                                   unique_ptr<DictFSSTAnalyzeState> &&analyze)
    : CompressionState(analyze->info), checkpoint_data(checkpoint_data_p),
      function(checkpoint_data.GetCompressionFunction(CompressionType::COMPRESSION_DICT_FSST)) {
	CreateEmptySegment(checkpoint_data.GetRowGroup().start);
}

DictFSSTCompressionCompressState::~DictFSSTCompressionCompressState() {
	if (encoder) {
		auto fsst_encoder = reinterpret_cast<duckdb_fsst_encoder_t *>(encoder);
		duckdb_fsst_destroy(fsst_encoder);
	}
}

static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(dict_fsst_compression_header_t);
static constexpr idx_t STRING_SIZE_LIMIT = 16384;
static constexpr uint16_t FSST_SYMBOL_TABLE_SIZE = sizeof(duckdb_fsst_decoder_t);

static inline bool IsEncoded(DictionaryAppendState state) {
	return state == DictionaryAppendState::ENCODED || state == DictionaryAppendState::ENCODED_ALL_UNIQUE;
}

void DictFSSTCompressionCompressState::Flush() {
	current_segment->count = tuple_count;

	// Reset the state
	append_state = DictionaryAppendState::REGULAR;
	string_lengths_width = 0;
	uncompressed_dictionary_copy.Destroy();
	//! This should already be empty at this point, otherwise that means that strings are not encoded / not added to the
	//! dictionary
	D_ASSERT(dictionary_encoding_buffer.empty());
	current_string_map.clear();

	string_lengths.clear();
	max_string_length = 0;

	dictionary_indices.clear();

	all_unique = true;
	auto fsst_encoder = reinterpret_cast<duckdb_fsst_encoder_t *>(encoder);
	duckdb_fsst_destroy(fsst_encoder);
	encoder = nullptr;
	symbol_table_size = DConstants::INVALID_INDEX;

	tuple_count = 0;
	dict_count = 0;

	string_lengths.push_back(0);
	dict_count++;
}

bool RequiresHigherBitWidth(bitpacking_width_t bitwidth, uint32_t other) {
	return other >= (1 << bitwidth);
}

static inline bool AddLookup(DictFSSTCompressionCompressState &state, idx_t lookup,
                             const bool recalculate_indices_space) {
	D_ASSERT(lookup != DConstants::INVALID_INDEX);

	//! This string exists in the dictionary
	idx_t new_dictionary_indices_space = state.dictionary_indices_space;
	if (recalculate_indices_space) {
		new_dictionary_indices_space =
		    BitpackingPrimitives::GetRequiredSize(state.tuple_count + 1, state.dictionary_indices_width);
	}

	idx_t required_space = 0;
	required_space += sizeof(dict_fsst_compression_header_t);
	required_space = AlignValue<idx_t>(required_space);
	required_space += state.current_dictionary.size;
	required_space = AlignValue<idx_t>(required_space);
	required_space += new_dictionary_indices_space;
	required_space = AlignValue<idx_t>(required_space);
	required_space += state.string_lengths_space;

	idx_t available_space = state.info.GetBlockSize();
	if (state.append_state == DictionaryAppendState::REGULAR) {
		available_space -= FSST_SYMBOL_TABLE_SIZE;
	}
	if (required_space > available_space) {
		return false;
	}

	state.all_unique = false;
	// Exists in the dictionary, add it
	state.dictionary_indices.push_back(lookup);
	state.tuple_count++;
	return true;
}

template <DictionaryAppendState APPEND_STATE>
static inline bool AddToDictionary(DictFSSTCompressionCompressState &state, const string_t &str,
                                   const bool recalculate_indices_space) {
	auto str_len = str.GetSize();
	if (APPEND_STATE == DictionaryAppendState::ENCODED) {
		//! We delay encoding of new entries.
		//  Encoding can increase the size of the string by 2x max, so we prepare for this worst case scenario.
		str_len *= 2;
	}

	const bool requires_higher_strlen_bitwidth = RequiresHigherBitWidth(state.string_lengths_width, str_len);
	const bool requires_higher_indices_bitwidth =
	    RequiresHigherBitWidth(state.dictionary_indices_width, state.dict_count + 1);
	// We round the required size up to bitpacking group sizes anyways, so we only have to recalculate every 32 values
	const bool recalculate_strlen_space =
	    ((state.dict_count + 1) % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) == 1;

	//! String Lengths
	bitpacking_width_t new_string_lengths_width = state.string_lengths_width;
	idx_t new_string_lengths_space = state.string_lengths_space;
	if (requires_higher_strlen_bitwidth) {
		new_string_lengths_width = BitpackingPrimitives::MinimumBitWidth(str_len);
	}
	if (requires_higher_strlen_bitwidth || recalculate_strlen_space) {
		new_string_lengths_space =
		    BitpackingPrimitives::GetRequiredSize(state.dict_count + 1, new_string_lengths_width);
	}

	//! Dictionary Indices
	bitpacking_width_t new_dictionary_indices_width = state.dictionary_indices_width;
	idx_t new_dictionary_indices_space = state.dictionary_indices_space;
	if (requires_higher_indices_bitwidth) {
		new_dictionary_indices_width = BitpackingPrimitives::MinimumBitWidth(state.dict_count + 1);
	}
	if (requires_higher_indices_bitwidth || recalculate_indices_space) {
		new_dictionary_indices_space =
		    BitpackingPrimitives::GetRequiredSize(state.tuple_count + 1, new_dictionary_indices_width);
	}

	idx_t required_space = 0;
	required_space += sizeof(dict_fsst_compression_header_t);
	required_space = AlignValue<idx_t>(required_space);
	if (APPEND_STATE == DictionaryAppendState::ENCODED) {
		required_space += state.current_dictionary.size + str_len;
		for (auto &uncompressed_string : state.dictionary_encoding_buffer) {
			required_space += uncompressed_string.GetSize() * 2;
		}
	} else {
		required_space += state.current_dictionary.size + str_len;
	}
	required_space = AlignValue<idx_t>(required_space);
	required_space += new_dictionary_indices_space;
	required_space = AlignValue<idx_t>(required_space);
	required_space += new_string_lengths_space;

	idx_t available_space = state.info.GetBlockSize();
	if (state.append_state == DictionaryAppendState::REGULAR) {
		available_space -= FSST_SYMBOL_TABLE_SIZE;
	}
	if (required_space > available_space) {
		return false;
	}

	// Add it to the dictionary
	state.dictionary_indices.push_back(state.dict_count);
	if (APPEND_STATE == DictionaryAppendState::ENCODED) {
		if (str.IsInlined()) {
			state.dictionary_encoding_buffer.push_back(str);
		} else {
			state.dictionary_encoding_buffer.push_back(state.uncompressed_dictionary_copy.AddString(str));
		}
		auto &uncompressed_string = state.dictionary_encoding_buffer.back();
		state.current_string_map[uncompressed_string] = state.dict_count;
	} else {
		state.string_lengths.push_back(str_len);
		auto baseptr =
		    AlignPointer<sizeof(data_ptr_t)>(state.current_handle.Ptr() + sizeof(dict_fsst_compression_header_t));
		memcpy(baseptr + state.dictionary_offset, str.GetData(), str_len);
		state.dictionary_offset += str_len;
		string_t dictionary_string((const char *)(baseptr + state.dictionary_offset), str_len); // NOLINT
		state.current_string_map[dictionary_string] = state.dict_count;
	}
	state.dict_count++;

	//! Update the state for serializing the dictionary_indices + string_lengths
	if (requires_higher_strlen_bitwidth) {
		D_ASSERT(str_len > state.max_string_length);
		state.string_lengths_width = new_string_lengths_width;
		state.max_string_length = str_len;
	}
	if (requires_higher_strlen_bitwidth || recalculate_strlen_space) {
		state.string_lengths_space = new_string_lengths_space;
	}

	if (requires_higher_indices_bitwidth) {
		state.dictionary_indices_width = new_dictionary_indices_width;
	}
	if (requires_higher_indices_bitwidth || recalculate_indices_space) {
		state.dictionary_indices_space = new_dictionary_indices_space;
	}
	state.tuple_count++;
	return true;
}

void DictFSSTCompressionCompressState::Compress(Vector &scan_vector, idx_t count) {
	UnifiedVectorFormat vector_format;
	scan_vector.ToUnifiedFormat(count, vector_format);
	auto strings = vector_format.GetData<string_t>(vector_format);
	//! If the append_mode is FSST_ONLY we will encode all input
	//  this memory is owned by a reusable buffer stored in the state
	vector<string_t> encoded_input;
	//! The index at which we started encoding the input
	//  in case we switch to FSST_ONLY in the middle, we can avoid encoding the previous input strings
	idx_t encoding_offset = 0;

	for (idx_t i = 0; i < count; i++) {
		auto idx = vector_format.sel->get_index(i);
		bool is_not_null = vector_format.validity.RowIsValid(idx);

		idx_t lookup = DConstants::INVALID_INDEX;
		auto &str = strings[idx];

		bool fits = false;
		for (idx_t check = 0; check < 3 && !fits; check++) {
			switch (append_state) {
			case DictionaryAppendState::NOT_ENCODED:
			case DictionaryAppendState::REGULAR: {
				if (is_not_null) {
					auto it = current_string_map.find(str);
					lookup = it == current_string_map.end() ? DConstants::INVALID_INDEX : it->second;
				} else {
					lookup = 0;
				}

				const bool recalculate_indices_space =
				    ((tuple_count + 1) % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) == 1;
				if (lookup != DConstants::INVALID_INDEX) {
					fits = AddLookup(*this, lookup, recalculate_indices_space);
				} else {
					//! This string does not exist in the dictionary, add it
					auto &str = strings[idx];
					// FIXME: if/else to call with NOT_ENCODED if needed??
					fits = AddToDictionary<DictionaryAppendState::REGULAR>(*this, str, recalculate_indices_space);
				}
				break;
			}
			case DictionaryAppendState::ENCODED: {
				// Don't encode the input, the 'current_string_map' is not encoded.
				// encoding of the dictionary is done lazily
				// we optimize for the case where the strings are *already* in the dictionary

				if (is_not_null) {
					auto it = current_string_map.find(str);
					lookup = it == current_string_map.end() ? DConstants::INVALID_INDEX : it->second;
				} else {
					lookup = 0;
				}

				const bool recalculate_indices_space =
				    ((tuple_count + 1) % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) == 1;
				if (lookup != DConstants::INVALID_INDEX) {
					fits = AddLookup(*this, lookup, recalculate_indices_space);
				} else {
					//! Not in the dictionary, add it
					fits = AddToDictionary<DictionaryAppendState::ENCODED>(*this, str, recalculate_indices_space);
				}

				break;
			}
			case DictionaryAppendState::ENCODED_ALL_UNIQUE: {
				// Encode the input upfront, the 'current_string_map' is also encoded.
				// no lookups are performed, everything is added.
				if (encoded_input.empty()) {
					encoding_offset = i;
					idx_t required_space = 0;
					vector<const char *> input_string_ptrs;
					vector<size_t> input_string_lengths;
					for (idx_t j = i; j < count; j++) {
						auto index = vector_format.sel->get_index(j);
#ifdef DEBUG
						//! We only choose FSST_ONLY if the rowgroup doesn't contain any nulls
						D_ASSERT(vector_format.validity.RowIsValid(index));
#endif
						auto &to_encode = strings[index];
						input_string_ptrs.push_back(to_encode.GetData());
						input_string_lengths.push_back(to_encode.GetSize());
					}

					// TODO: First encode the input
				}

				const bool recalculate_indices_space =
				    ((tuple_count + 1) % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) == 1;
				auto &string = encoded_input[i - encoding_offset];
				fits = AddToDictionary<DictionaryAppendState::ENCODED_ALL_UNIQUE>(*this, string,
				                                                                  recalculate_indices_space);
				break;
			}
			}
		}
		if (!fits) {
			throw InternalException("This should not happen, a possible infinite loop was prevented");
		}
	}
}

void DictFSSTCompressionCompressState::FinalizeCompress() {
	throw NotImplementedException("TODO");
}

// ------------------ OLD ----------------------------------------------------------------------

void DictFSSTCompressionCompressState::CreateEmptySegment(idx_t row_start) {
	auto &db = checkpoint_data.GetDatabase();
	auto &type = checkpoint_data.GetType();

	auto compressed_segment =
	    ColumnSegment::CreateTransientSegment(db, function, type, row_start, info.GetBlockSize(), info.GetBlockSize());
	current_segment = std::move(compressed_segment);

	// Reset the buffers and the string map.
	current_string_map.clear();
	string_lengths.clear();

	// Reserve index 0 for null strings.
	string_lengths.push_back(0);
	dict_count++;
	dictionary_indices.clear();

	// Reset the pointers into the current segment.
	auto &buffer_manager = BufferManager::GetBufferManager(checkpoint_data.GetDatabase());
	current_handle = buffer_manager.Pin(current_segment->block);

	dictionary_offset = 0;
	// current_dictionary = DictFSSTCompression::GetDictionary(*current_segment, current_handle);
}

void DictFSSTCompressionCompressState::Verify() {
	current_dictionary.Verify(info.GetBlockSize());
	D_ASSERT(current_segment->count == selection_buffer.size());
	D_ASSERT(DictFSSTCompression::HasEnoughSpace(current_segment->count.load(), dictionary_string_lengths.size(),
	                                             current_dictionary.size, current_width, string_length_bitwidth,
	                                             info.GetBlockSize()));

#ifdef DEBUG
	if (!IsEncoded()) {
		D_ASSERT(current_dictionary.end == info.GetBlockSize());
	}
#endif
	if (append_state != DictionaryAppendState::ENCODED_ALL_UNIQUE) {
		//! This is not true for ENCODED_ALL_UNIQUE (FSST_ONLY mode) because we will store duplicates in the dictionary
		//! as well
		D_ASSERT(dictionary_string_lengths.size() == current_string_map.size() + 1); // +1 is for null value
	}
}

optional_idx DictFSSTCompressionCompressState::LookupString(const string_t &str) {
	if (append_state == DictionaryAppendState::ENCODED_ALL_UNIQUE) {
		//! In this mode we omit the selection buffer, storing (possible) duplicates in the dictionary
		return optional_idx();
	}

	auto search = current_string_map.find(str);
	auto has_result = search != current_string_map.end();

	if (!has_result) {
		return optional_idx();
	}
	all_unique = false;
	return search->second;
}

void DictFSSTCompressionCompressState::AddNewString(const StringData &string_data) {
	//! Update the stats using the uncompressed string always!
	UncompressedStringStorage::UpdateStringStats(current_segment->stats, string_data.string);

	auto &str = IsEncoded() ? *string_data.encoded_string : string_data.string;
	// Copy string to dict
	// New entries are added to the start (growing backwards)
	// [............xxxxooooooooo]
	// x: new string
	// o: existing string
	// .: (currently) unused space
	current_dictionary.size += str.GetSize();
	auto dict_pos = current_end_ptr - current_dictionary.size;
	memcpy(dict_pos, str.GetData(), str.GetSize());
	current_dictionary.Verify(info.GetBlockSize());
#ifdef DEBUG
	if (!IsEncoded()) {
		D_ASSERT(current_dictionary.end == info.GetBlockSize());
	}
#endif
	DictFSSTCompression::SetDictionary(*current_segment, current_handle, current_dictionary);

	// Update buffers and map
	auto str_len = UnsafeNumericCast<uint32_t>(str.GetSize());
	dictionary_string_lengths.push_back(str_len);
	if (str_len > max_length) {
		string_length_bitwidth = BitpackingPrimitives::MinimumBitWidth(str_len);
		max_length = str_len;
	}

	selection_buffer.push_back(UnsafeNumericCast<uint32_t>(dictionary_string_lengths.size() - 1));
	if (append_state != DictionaryAppendState::ENCODED_ALL_UNIQUE) {
		if (str.IsInlined()) {
			current_string_map.insert({str, dictionary_string_lengths.size() - 1});
		} else {
			string_t dictionary_string((const char *)dict_pos, UnsafeNumericCast<uint32_t>(str_len)); // NOLINT
			D_ASSERT(!dictionary_string.IsInlined());
			current_string_map.insert({dictionary_string, dictionary_string_lengths.size() - 1});
		}

		current_width = next_width;
	}
	current_segment->count++;
}

void DictFSSTCompressionCompressState::AddNull() {
	selection_buffer.push_back(0);
	//! With FSST_ONLY we can't store validity, so we can only use this mode when no validity is required (all are
	//! non-null).
	all_unique = false;
	current_segment->count++;
}

void DictFSSTCompressionCompressState::AddLookup(uint32_t lookup_result) {
	selection_buffer.push_back(lookup_result);
	current_segment->count++;
}

idx_t DictFSSTCompressionCompressState::RequiredSpace(bool new_string, idx_t string_size) {
	idx_t required_space = 0;
	if (IsEncoded()) {
		required_space += symbol_table_size;
	}

	auto dict_count = dictionary_string_lengths.size();
	if (!new_string) {
		required_space +=
		    DictFSSTCompression::RequiredSpace(current_segment->count.load() + 1, dict_count, current_dictionary.size,
		                                       current_width, string_length_bitwidth);
	} else {
		next_width = BitpackingPrimitives::MinimumBitWidth(dict_count - 1 + new_string);
		if (append_state == DictionaryAppendState::ENCODED_ALL_UNIQUE) {
			next_width = 0;
		}
		required_space += DictFSSTCompression::RequiredSpace(current_segment->count.load() + 1, dict_count + 1,
		                                                     current_dictionary.size + string_size, next_width,
		                                                     string_length_bitwidth);
	}
	return required_space;
}

void DictFSSTCompressionCompressState::Flush(bool final) {
	auto next_start = current_segment->start + current_segment->count;

	auto segment_size = Finalize();
	append_state = DictionaryAppendState::REGULAR;
	all_unique = true;
	encoded_input.Reset();
	if (encoder) {
		auto fsst_encoder = reinterpret_cast<duckdb_fsst_encoder_t *>(encoder);
		duckdb_fsst_destroy(fsst_encoder);
		encoder = nullptr;
		symbol_table_size = DConstants::INVALID_INDEX;
	}
	max_length = 0;
	string_length_bitwidth = 0;

	auto &state = checkpoint_data.GetCheckpointState();
	state.FlushSegment(std::move(current_segment), std::move(current_handle), segment_size);

	if (!final) {
		CreateEmptySegment(next_start);
	}
}

void DictFSSTCompressionCompressState::EncodeInputStrings(UnifiedVectorFormat &input, idx_t count) {
	D_ASSERT(IsEncoded());

	encoded_input.Reset();
	D_ASSERT(encoder);
	auto fsst_encoder = reinterpret_cast<duckdb_fsst_encoder_t *>(encoder);

	vector<size_t> fsst_string_sizes;
	vector<unsigned char *> fsst_string_ptrs;
	//! We could use the 'current_string_map' but this won't be in-order
	// and we want to preserve the order of the dictionary after rewriting
	auto data = input.GetData<string_t>(input);
	idx_t total_size = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = input.sel->get_index(i);
		auto &str = data[idx];
		fsst_string_sizes.push_back(str.GetSize());
		fsst_string_ptrs.push_back((unsigned char *)str.GetData()); // NOLINT
		total_size += str.GetSize();
	}

	size_t output_buffer_size = 7 + 2 * total_size; // size as specified in fsst.h
	auto compressed_ptrs = vector<unsigned char *>(count, nullptr);
	auto compressed_sizes = vector<size_t>(count, 0);
	auto compressed_buffer = make_unsafe_uniq_array_uninitialized<unsigned char>(output_buffer_size);

	auto res =
	    duckdb_fsst_compress(fsst_encoder, count, &fsst_string_sizes[0], &fsst_string_ptrs[0], output_buffer_size,
	                         compressed_buffer.get(), &compressed_sizes[0], &compressed_ptrs[0]);
	if (res != count) {
		throw FatalException("FSST compression failed to compress all input strings");
	}

	auto &strings = encoded_input.input_data;
	auto &heap = encoded_input.heap;
	for (idx_t i = 0; i < count; i++) {
		uint32_t size = UnsafeNumericCast<uint32_t>(compressed_sizes[i]);
		string_t encoded_string((const char *)compressed_ptrs[i], size); // NOLINT;
		if (!encoded_string.IsInlined()) {
			strings.push_back(heap.AddBlob(encoded_string));
		} else {
			strings.push_back(encoded_string);
		}
	}
}

bool DictFSSTCompressionCompressState::EncodeDictionary() {
	if (current_dictionary.size < DICTIONARY_ENCODE_THRESHOLD) {
		append_state = DictionaryAppendState::NOT_ENCODED;
		return false;
	}

	vector<size_t> fsst_string_sizes;
	vector<unsigned char *> fsst_string_ptrs;
	//! We could use the 'current_string_map' but this won't be in-order
	// and we want to preserve the order of the dictionary after rewriting

	// Skip index 0, that's reserved for NULL
	uint32_t offset = 0;
	for (idx_t i = 1; i < dictionary_string_lengths.size(); i++) {
		auto length = dictionary_string_lengths[i];
		offset += length;
		auto start = current_end_ptr - offset;
		fsst_string_sizes.push_back(length);
		fsst_string_ptrs.push_back((unsigned char *)start); // NOLINT
	}

	// Create the encoder
	auto string_count = dictionary_string_lengths.size() - 1;
	encoder =
	    reinterpret_cast<void *>(duckdb_fsst_create(string_count, &fsst_string_sizes[0], &fsst_string_ptrs[0], 0));
	auto fsst_encoder = reinterpret_cast<duckdb_fsst_encoder_t *>(encoder);

	size_t output_buffer_size = 7 + 2 * current_dictionary.size; // size as specified in fsst.h
	auto compressed_ptrs = vector<unsigned char *>(string_count, nullptr);
	auto compressed_sizes = vector<size_t>(string_count, 0);
	auto compressed_buffer = make_unsafe_uniq_array_uninitialized<unsigned char>(output_buffer_size);

	// Compress the dictionary
	auto res =
	    duckdb_fsst_compress(fsst_encoder, string_count, &fsst_string_sizes[0], &fsst_string_ptrs[0],
	                         output_buffer_size, compressed_buffer.get(), &compressed_sizes[0], &compressed_ptrs[0]);
	if (res != string_count) {
		throw FatalException("FSST compression failed to compress all dictionary strings");
	}

	idx_t new_size = 0;
	for (idx_t i = 0; i < string_count; i++) {
		new_size += compressed_sizes[i];
	}
	if (new_size > current_dictionary.size + DICTIONARY_ENCODE_THRESHOLD) {
		// The dictionary does not compress well enough to use FSST
		// continue filling the remaining bytes without encoding
		duckdb_fsst_destroy(fsst_encoder);
		encoder = nullptr;
		append_state = DictionaryAppendState::NOT_ENCODED;
		return false;
	}

	if (all_unique) {
		append_state = DictionaryAppendState::ENCODED_ALL_UNIQUE;
		//! We omit the selection buffer in this mode, setting the width to 0 makes the RequiredSpace result not include
		//! the selection buffer space.
		current_width = 0;
	} else {
		append_state = DictionaryAppendState::ENCODED;
	}

	// Write the exported symbol table to the end of the segment
	unsigned char fsst_serialized_symbol_table[sizeof(duckdb_fsst_decoder_t)];
	symbol_table_size = duckdb_fsst_export(fsst_encoder, fsst_serialized_symbol_table);
	current_end_ptr -= symbol_table_size;
	memcpy(current_end_ptr, (void *)fsst_serialized_symbol_table, symbol_table_size);

	// Rewrite the dictionary
	current_string_map.clear();
	offset = 0;
	max_length = 0;
	for (idx_t i = 0; i < string_count; i++) {
		auto &start = compressed_ptrs[i];
		auto size = UnsafeNumericCast<uint32_t>(compressed_sizes[i]);
		offset += size;
		if (size > max_length) {
			max_length = size;
		}
		// Skip index 0, reserved for NULL
		uint32_t dictionary_index = UnsafeNumericCast<uint32_t>(i + 1);
		dictionary_string_lengths[dictionary_index] = size;
		auto dest = current_end_ptr - offset;
		memcpy(dest, start, size);
		string_t dictionary_string((const char *)dest, UnsafeNumericCast<uint32_t>(size)); // NOLINT
		current_string_map.insert({dictionary_string, dictionary_index});
	}

	string_length_bitwidth = BitpackingPrimitives::MinimumBitWidth(max_length);

	current_dictionary.size = offset;
	current_dictionary.end -= symbol_table_size;
	DictFSSTCompression::SetDictionary(*current_segment, current_handle, current_dictionary);
	return true;
}

static DictFSSTMode ConvertToMode(DictionaryAppendState &state) {
	switch (state) {
	case DictionaryAppendState::ENCODED:
		return DictFSSTMode::DICT_FSST;
	case DictionaryAppendState::REGULAR:
	case DictionaryAppendState::NOT_ENCODED:
		return DictFSSTMode::DICTIONARY;
	case DictionaryAppendState::ENCODED_ALL_UNIQUE:
		return DictFSSTMode::FSST_ONLY;
	}
}

StringData DictFSSTCompressionCompressState::GetString(const string_t *strings, idx_t index, idx_t raw_index) {
	StringData result(strings[index]);
	if (IsEncoded()) {
		result.encoded_string = encoded_input.input_data[raw_index];
	}
	return result;
}

idx_t DictFSSTCompressionCompressState::Finalize() {
	auto &buffer_manager = BufferManager::GetBufferManager(checkpoint_data.GetDatabase());
	auto handle = buffer_manager.Pin(current_segment->block);

#ifdef DEBUG
	if (!IsEncoded()) {
		D_ASSERT(current_dictionary.end == info.GetBlockSize());
	}
#endif
	const bool is_fsst_encoded = IsEncoded();

	// calculate sizes
	auto compressed_selection_buffer_size =
	    BitpackingPrimitives::GetRequiredSize(current_segment->count, current_width);
	auto dict_count = dictionary_string_lengths.size();
	auto dictionary_string_lengths_size = BitpackingPrimitives::GetRequiredSize(dict_count, string_length_bitwidth);
	auto total_size = DictFSSTCompression::DICTIONARY_HEADER_SIZE + compressed_selection_buffer_size +
	                  dictionary_string_lengths_size + current_dictionary.size;
	if (is_fsst_encoded) {
		total_size += symbol_table_size;
	}

	// calculate ptr and offsets
	auto base_ptr = handle.Ptr();
	auto header_ptr = reinterpret_cast<dict_fsst_compression_header_t *>(base_ptr);
	auto compressed_selection_buffer_offset = DictFSSTCompression::DICTIONARY_HEADER_SIZE;
	auto string_lengths_offset = compressed_selection_buffer_offset + compressed_selection_buffer_size;
	header_ptr->mode = ConvertToMode(append_state);

	// Write compressed selection buffer
	BitpackingPrimitives::PackBuffer<sel_t, false>(base_ptr + compressed_selection_buffer_offset,
	                                               (sel_t *)(selection_buffer.data()), current_segment->count,
	                                               current_width);
	BitpackingPrimitives::PackBuffer<uint32_t, false>(
	    base_ptr + string_lengths_offset, dictionary_string_lengths.data(), dict_count, string_length_bitwidth);

	// Store sizes and offsets in segment header
	Store<uint32_t>(NumericCast<uint32_t>(string_lengths_offset), data_ptr_cast(&header_ptr->string_lengths_offset));
	Store<uint32_t>(NumericCast<uint32_t>(dict_count), data_ptr_cast(&header_ptr->dict_count));
	Store<uint32_t>((uint32_t)current_width, data_ptr_cast(&header_ptr->bitpacking_width));
	Store<uint32_t>((uint32_t)string_length_bitwidth, data_ptr_cast(&header_ptr->string_lengths_width));

	if (append_state != DictionaryAppendState::ENCODED_ALL_UNIQUE) {
		D_ASSERT(current_width == BitpackingPrimitives::MinimumBitWidth(dict_count - 1));
	}
	D_ASSERT(DictFSSTCompression::HasEnoughSpace(current_segment->count, dict_count, current_dictionary.size,
	                                             current_width, string_length_bitwidth, info.GetBlockSize()));
	D_ASSERT((uint64_t)*max_element(std::begin(selection_buffer), std::end(selection_buffer)) == dict_count - 1);

	// Early-out, if the block is sufficiently full.
	if (total_size >= info.GetCompactionFlushLimit()) {
		return info.GetBlockSize();
	}

	// Sufficient space: calculate how much space we can save.
	auto space_left = info.GetBlockSize() - total_size;

	// Move the dictionary to align it with the offsets.
	auto new_dictionary_offset = string_lengths_offset + dictionary_string_lengths_size;
	idx_t bytes_to_move = current_dictionary.size;
	if (is_fsst_encoded) {
		D_ASSERT(symbol_table_size != DConstants::INVALID_INDEX);
		// Also move the symbol table located directly behind the dictionary
		bytes_to_move += symbol_table_size;
	}
	memmove(base_ptr + new_dictionary_offset, base_ptr + current_dictionary.end - current_dictionary.size,
	        bytes_to_move);
	current_dictionary.end -= space_left;
#ifdef DEBUG
	if (is_fsst_encoded) {
		D_ASSERT(current_dictionary.end + symbol_table_size == total_size);
	} else {
		D_ASSERT(current_dictionary.end == total_size);
	}

#endif

	// Write the new dictionary with the updated "end".
	DictFSSTCompression::SetDictionary(*current_segment, handle, current_dictionary);
	return total_size;
}

} // namespace dict_fsst
} // namespace duckdb
