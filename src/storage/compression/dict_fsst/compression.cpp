#include "duckdb/storage/compression/dict_fsst/compression.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/common/typedefs.hpp"
#include "fsst.h"
#include "duckdb/common/fsst.hpp"

namespace duckdb {
namespace dict_fsst {

DictFSSTCompressionState::DictFSSTCompressionState(ColumnDataCheckpointData &checkpoint_data_p,
                                                   unique_ptr<DictFSSTAnalyzeState> &&analyze_p)
    : CompressionState(analyze_p->info), checkpoint_data(checkpoint_data_p),
      function(checkpoint_data.GetCompressionFunction(CompressionType::COMPRESSION_DICT_FSST)),
      analyze(std::move(analyze_p)) {
	CreateEmptySegment(checkpoint_data.GetRowGroup().start);
}

DictFSSTCompressionState::~DictFSSTCompressionState() {
	if (encoder) {
		auto fsst_encoder = reinterpret_cast<duckdb_fsst_encoder_t *>(encoder);
		duckdb_fsst_destroy(fsst_encoder);
	}
}

static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(dict_fsst_compression_header_t);
static constexpr uint16_t FSST_SYMBOL_TABLE_SIZE = sizeof(duckdb_fsst_decoder_t);
static constexpr idx_t DICTIONARY_ENCODE_THRESHOLD = 4096;

static inline bool IsEncoded(DictionaryAppendState state) {
	return state == DictionaryAppendState::ENCODED || state == DictionaryAppendState::ENCODED_ALL_UNIQUE;
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

idx_t DictFSSTCompressionState::Finalize() {
	const bool is_fsst_encoded = IsEncoded(append_state);

// calculate sizes
#ifdef DEBUG
	{
		auto dictionary_indices_space = BitpackingPrimitives::GetRequiredSize(tuple_count, dictionary_indices_width);
		D_ASSERT(dictionary_indices_space == this->dictionary_indices_space);

		auto string_lengths_space = BitpackingPrimitives::GetRequiredSize(dict_count, string_lengths_width);
		D_ASSERT(string_lengths_space == this->string_lengths_space);
	}
#endif

	if (!is_fsst_encoded) {
		symbol_table_size = 0;
	}

	D_ASSERT(to_encode_string_sum == 0);

	idx_t required_space = 0;
	required_space += sizeof(dict_fsst_compression_header_t);
	required_space = AlignValue<idx_t>(required_space);
	required_space += dictionary_offset;
	required_space = AlignValue<idx_t>(required_space);
	if (is_fsst_encoded) {
		required_space += symbol_table_size;
		required_space = AlignValue<idx_t>(required_space);
	}
	required_space += string_lengths_space;
	required_space = AlignValue<idx_t>(required_space);
	required_space += dictionary_indices_space;

	D_ASSERT(info.GetBlockSize() >= required_space);

	// calculate ptr and offsets
	auto base_ptr = current_handle.Ptr();
	auto header_ptr = reinterpret_cast<dict_fsst_compression_header_t *>(base_ptr);
	auto dictionary_dest = AlignValue<idx_t>(DictFSSTCompression::DICTIONARY_HEADER_SIZE);
	auto symbol_table_dest = AlignValue<idx_t>(dictionary_dest + dictionary_offset);
	auto string_lengths_dest = AlignValue<idx_t>(symbol_table_dest + symbol_table_size);
	auto dictionary_indices_dest = AlignValue<idx_t>(string_lengths_dest + string_lengths_space);

	header_ptr->mode = ConvertToMode(append_state);
	header_ptr->symbol_table_size = symbol_table_size;
	header_ptr->dict_size = dictionary_offset;
	header_ptr->dict_count = dict_count;
	header_ptr->dictionary_indices_width = dictionary_indices_width;
	header_ptr->string_lengths_width = string_lengths_width;

	// Write the symbol table
	if (is_fsst_encoded) {
		D_ASSERT(symbol_table_size != DConstants::INVALID_INDEX);
		memcpy(base_ptr + symbol_table_dest, fsst_serialized_symbol_table.get(), symbol_table_size);
	}

	// Write the string lengths of the dictionary
	BitpackingPrimitives::PackBuffer<uint32_t, false>(base_ptr + string_lengths_dest, string_lengths.data(), dict_count,
	                                                  string_lengths_width);
	// Write the dictionary indices (selection vector)
	BitpackingPrimitives::PackBuffer<sel_t, false>(base_ptr + dictionary_indices_dest,
	                                               (sel_t *)(dictionary_indices.data()), tuple_count,
	                                               dictionary_indices_width);

	if (append_state != DictionaryAppendState::ENCODED_ALL_UNIQUE) {
		auto expected_bitwidth = BitpackingPrimitives::MinimumBitWidth(dict_count - 1);
		D_ASSERT(dictionary_indices_width == expected_bitwidth);
	}
	D_ASSERT(base_ptr + required_space == base_ptr + dictionary_indices_dest + dictionary_indices_space);
	D_ASSERT((uint64_t)*max_element(std::begin(dictionary_indices), std::end(dictionary_indices)) == dict_count - 1);
	return required_space;
}

void DictFSSTCompressionState::FlushEncodingBuffer() {
	if (dictionary_encoding_buffer.empty()) {
		D_ASSERT(to_encode_string_sum == 0);
		return;
	}

	vector<size_t> fsst_string_sizes;
	vector<unsigned char *> fsst_string_ptrs;

	data_ptr_t dictionary_start =
	    AlignPointer<sizeof(void *)>(current_handle.Ptr() + sizeof(dict_fsst_compression_header_t));
	D_ASSERT(dictionary_encoding_buffer.size() == dict_count - string_lengths.size());
	auto string_count = dictionary_encoding_buffer.size();
	for (auto &to_encode : dictionary_encoding_buffer) {
		fsst_string_sizes.push_back(to_encode.GetSize());
		fsst_string_ptrs.push_back((unsigned char *)to_encode.GetData()); // NOLINT
	}

	auto compressed_ptrs = vector<unsigned char *>(string_count, nullptr);
	auto compressed_sizes = vector<size_t>(string_count, 0);

	// Compress the dictionary, straight to the segment
	idx_t available_space = info.GetBlockSize();
	available_space -= AlignValue<idx_t>(sizeof(dict_fsst_compression_header_t));
	available_space -= dictionary_offset;
	available_space -= symbol_table_size;

	auto fsst_encoder = reinterpret_cast<duckdb_fsst_encoder_t *>(encoder);
	auto res = duckdb_fsst_compress(fsst_encoder, string_count, fsst_string_sizes.data(), fsst_string_ptrs.data(),
	                                available_space, (unsigned char *)dictionary_start + dictionary_offset,
	                                compressed_sizes.data(), compressed_ptrs.data());
	if (res != string_count) {
		throw FatalException("Somehow we did not have enough room in the segment to store the encoded strings");
	}
	string_lengths_width = real_string_lengths_width;
	//! The first value where string_lengths_width+1 is needed
	uint32_t biggest_strlen = 1 << string_lengths_width;
	for (idx_t i = 0; i < string_count; i++) {
		auto str_len = UnsafeNumericCast<uint32_t>(compressed_sizes[i]);
		if (str_len >= biggest_strlen) {
			biggest_strlen = str_len;
		}
		string_lengths.push_back(str_len);
		dictionary_offset += str_len;
	}
	if (biggest_strlen >= 1 << string_lengths_width) {
		string_lengths_width = BitpackingPrimitives::MinimumBitWidth(biggest_strlen);
	}
	real_string_lengths_width = string_lengths_width;
	string_lengths_space = BitpackingPrimitives::GetRequiredSize(dict_count, string_lengths_width);
	D_ASSERT(string_lengths_space != 0);
	to_encode_string_sum = 0;
	dictionary_encoding_buffer.clear();
}

void DictFSSTCompressionState::CreateEmptySegment(idx_t row_start) {
	auto &db = checkpoint_data.GetDatabase();
	auto &type = checkpoint_data.GetType();

	auto compressed_segment =
	    ColumnSegment::CreateTransientSegment(db, function, type, row_start, info.GetBlockSize(), info.GetBlockSize());
	current_segment = std::move(compressed_segment);

	// Reset the pointers into the current segment.
	auto &buffer_manager = BufferManager::GetBufferManager(checkpoint_data.GetDatabase());
	current_handle = buffer_manager.Pin(current_segment->block);

	append_state = DictionaryAppendState::REGULAR;
	string_lengths_width = 0;
	real_string_lengths_width = 0;
	dictionary_indices_width = 0;
	string_lengths_space = 0;
	D_ASSERT(dictionary_indices.empty());
	dictionary_indices_space = 0;
	tuple_count = 0;
	D_ASSERT(string_lengths.empty());
	string_lengths.push_back(0);
	dict_count = 1;
	D_ASSERT(current_string_map.empty());
	symbol_table_size = DConstants::INVALID_INDEX;

	dictionary_offset = 0;
	current_dictionary.end = 0;
	current_dictionary.size = 0;
}

void DictFSSTCompressionState::Flush(bool final) {
	if (final) {
		FlushEncodingBuffer();
	}

	if (!tuple_count) {
		return;
	}

	current_segment->count = tuple_count;
	current_dictionary.size = dictionary_offset;
	current_dictionary.end = dictionary_offset;

	auto next_start = current_segment->start + current_segment->count;
	auto segment_size = Finalize();
	auto &state = checkpoint_data.GetCheckpointState();
	state.FlushSegment(std::move(current_segment), std::move(current_handle), segment_size);

	// Reset the state
	uncompressed_dictionary_copy.Destroy();
	//! This should already be empty at this point, otherwise that means that strings are not encoded / not added to the
	//! dictionary
	D_ASSERT(dictionary_encoding_buffer.empty());
	D_ASSERT(to_encode_string_sum == 0);

	current_string_map.clear();
	string_lengths.clear();
	dictionary_indices.clear();
	if (encoder) {
		auto fsst_encoder = reinterpret_cast<duckdb_fsst_encoder_t *>(encoder);
		duckdb_fsst_destroy(fsst_encoder);
		encoder = nullptr;
		symbol_table_size = DConstants::INVALID_INDEX;
	}
	total_tuple_count += tuple_count;

	if (!final) {
		CreateEmptySegment(next_start);
	}
}

static inline bool RequiresHigherBitWidth(bitpacking_width_t bitwidth, uint32_t other) {
	return other >= (1 << bitwidth);
}

template <DictionaryAppendState APPEND_STATE>
static inline bool AddLookup(DictFSSTCompressionState &state, idx_t lookup, const bool recalculate_indices_space) {
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
	if (APPEND_STATE == DictionaryAppendState::ENCODED) {
		required_space += state.dictionary_offset;
		required_space += state.to_encode_string_sum;
	} else {
		required_space += state.dictionary_offset;
	}
	required_space = AlignValue<idx_t>(required_space);
	if (IsEncoded(APPEND_STATE)) {
		required_space += state.symbol_table_size;
		required_space = AlignValue<idx_t>(required_space);
	}
	required_space += new_dictionary_indices_space;
	required_space = AlignValue<idx_t>(required_space);
	required_space += state.string_lengths_space;

	idx_t available_space = state.info.GetBlockSize();
	if (APPEND_STATE == DictionaryAppendState::REGULAR) {
		available_space -= FSST_SYMBOL_TABLE_SIZE;
	}
	if (required_space > available_space) {
		return false;
	}

	if (recalculate_indices_space) {
		state.dictionary_indices_space = new_dictionary_indices_space;
	}
	// Exists in the dictionary, add it
	state.dictionary_indices.push_back(lookup);
	state.tuple_count++;
	return true;
}

template <DictionaryAppendState APPEND_STATE>
static inline bool AddToDictionary(DictFSSTCompressionState &state, const string_t &str,
                                   const bool recalculate_indices_space) {
	auto str_len = str.GetSize();
	if (APPEND_STATE == DictionaryAppendState::ENCODED) {
		//! We delay encoding of new entries.
		//  Encoding can increase the size of the string by 2x max, so we prepare for this worst case scenario.
		str_len *= 2;
	}

	const bool requires_higher_strlen_bitwidth = RequiresHigherBitWidth(state.string_lengths_width, str_len);
	const bool requires_higher_indices_bitwidth =
	    RequiresHigherBitWidth(state.dictionary_indices_width, state.dict_count);
	// We round the required size up to bitpacking group sizes anyways, so we only have to recalculate every 32 values
	const bool recalculate_strlen_space =
	    (state.dict_count % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) == 0;

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
		new_dictionary_indices_width = BitpackingPrimitives::MinimumBitWidth(state.dict_count);
	}
	if (requires_higher_indices_bitwidth || recalculate_indices_space) {
		new_dictionary_indices_space =
		    BitpackingPrimitives::GetRequiredSize(state.tuple_count + 1, new_dictionary_indices_width);
	}

	idx_t required_space = 0;
	required_space += sizeof(dict_fsst_compression_header_t);
	required_space = AlignValue<idx_t>(required_space);
	if (APPEND_STATE == DictionaryAppendState::ENCODED) {
		required_space += state.dictionary_offset + str_len;
		required_space += state.to_encode_string_sum;
	} else {
		required_space += state.dictionary_offset + str_len;
	}
	required_space = AlignValue<idx_t>(required_space);
	if (IsEncoded(APPEND_STATE)) {
		required_space += state.symbol_table_size;
		required_space = AlignValue<idx_t>(required_space);
	}
	required_space += new_dictionary_indices_space;
	required_space = AlignValue<idx_t>(required_space);
	required_space += new_string_lengths_space;

	idx_t available_space = state.info.GetBlockSize();
	if (APPEND_STATE == DictionaryAppendState::REGULAR) {
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
		state.to_encode_string_sum += str_len;
		auto &uncompressed_string = state.dictionary_encoding_buffer.back();
		state.current_string_map[uncompressed_string] = state.dict_count;
	} else {
		state.string_lengths.push_back(str_len);
		auto baseptr =
		    AlignPointer<sizeof(data_ptr_t)>(state.current_handle.Ptr() + sizeof(dict_fsst_compression_header_t));
		memcpy(baseptr + state.dictionary_offset, str.GetData(), str_len);
		string_t dictionary_string((const char *)(baseptr + state.dictionary_offset), str_len); // NOLINT
		state.dictionary_offset += str_len;
		state.current_string_map[dictionary_string] = state.dict_count;
	}
	state.dict_count++;

	//! Update the state for serializing the dictionary_indices + string_lengths
	if (requires_higher_strlen_bitwidth) {
		state.string_lengths_width = new_string_lengths_width;
	}
	if (requires_higher_strlen_bitwidth || recalculate_strlen_space) {
		state.string_lengths_space = new_string_lengths_space;
		D_ASSERT(state.string_lengths_space != 0);
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

bool DictFSSTCompressionState::CompressInternal(UnifiedVectorFormat &vector_format, EncodedInput &encoded_input,
                                                const idx_t i, idx_t count) {
	auto idx = vector_format.sel->get_index(i);
	bool is_not_null = vector_format.validity.RowIsValid(idx);
	auto strings = vector_format.GetData<string_t>(vector_format);

	idx_t lookup = DConstants::INVALID_INDEX;
	auto &str = strings[idx];

	//! In GetRequiredSize we will round up to ALGORITHM_GROUP_SIZE anyways
	//  so we can avoid recalculating for every tuple
	const bool recalculate_indices_space = ((tuple_count % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) == 0);

	switch (append_state) {
	case DictionaryAppendState::NOT_ENCODED:
	case DictionaryAppendState::REGULAR: {
		string_map_t<uint32_t>::iterator it;
		if (is_not_null) {
			it = current_string_map.find(str);
			lookup = it == current_string_map.end() ? DConstants::INVALID_INDEX : it->second;
		} else {
			lookup = 0;
		}

		if (append_state == DictionaryAppendState::REGULAR) {
			if (lookup != DConstants::INVALID_INDEX) {
				return AddLookup<DictionaryAppendState::REGULAR>(*this, lookup, recalculate_indices_space);
			} else {
				//! This string does not exist in the dictionary, add it
				auto &str = strings[idx];
				return AddToDictionary<DictionaryAppendState::REGULAR>(*this, str, recalculate_indices_space);
			}
		} else {
			if (lookup != DConstants::INVALID_INDEX) {
				return AddLookup<DictionaryAppendState::NOT_ENCODED>(*this, lookup, recalculate_indices_space);
			} else {
				//! This string does not exist in the dictionary, add it
				auto &str = strings[idx];
				return AddToDictionary<DictionaryAppendState::NOT_ENCODED>(*this, str, recalculate_indices_space);
			}
		}
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

		bool fits;
		if (lookup != DConstants::INVALID_INDEX) {
			fits = AddLookup<DictionaryAppendState::ENCODED>(*this, lookup, recalculate_indices_space);
		} else {
			//! Not in the dictionary, add it
			fits = AddToDictionary<DictionaryAppendState::ENCODED>(*this, str, recalculate_indices_space);
		}
		if (fits || dictionary_encoding_buffer.empty()) {
			return fits;
		}

		// We lazily encode the new entries, if we're full but have entries in the buffer
		// we flush these and try again to see if the size went down enough
		FlushEncodingBuffer();
		if (lookup != DConstants::INVALID_INDEX) {
			return AddLookup<DictionaryAppendState::ENCODED>(*this, lookup, recalculate_indices_space);
		} else {
			//! Not in the dictionary, add it
			return AddToDictionary<DictionaryAppendState::ENCODED>(*this, str, recalculate_indices_space);
		}
	}
	case DictionaryAppendState::ENCODED_ALL_UNIQUE: {
		// Encode the input upfront, the 'current_string_map' is also encoded.
		// no lookups are performed, everything is added.

#ifdef DEBUG
		auto temp_decoder = alloca(sizeof(duckdb_fsst_decoder_t));
		duckdb_fsst_import((duckdb_fsst_decoder_t *)temp_decoder, fsst_serialized_symbol_table.get());

		vector<unsigned char> decompress_buffer;
#endif

		if (encoded_input.data.empty()) {
			encoded_input.offset = i;
			vector<unsigned char *> input_string_ptrs;
			vector<size_t> input_string_lengths;
			idx_t total_size = 0;
			for (idx_t j = i; j < count; j++) {
				auto index = vector_format.sel->get_index(j);
#ifdef DEBUG
				//! We only choose FSST_ONLY if the rowgroup doesn't contain any nulls
				D_ASSERT(vector_format.validity.RowIsValid(index));
#endif
				auto &to_encode = strings[index];
				input_string_ptrs.push_back((unsigned char *)to_encode.GetData()); // NOLINT
				input_string_lengths.push_back(to_encode.GetSize());
				total_size += to_encode.GetSize();
			}

			size_t output_buffer_size = 7 + 2 * total_size; // size as specified in fsst.h
			auto compressed_ptrs = vector<unsigned char *>(input_string_lengths.size(), nullptr);
			auto compressed_sizes = vector<size_t>(input_string_lengths.size(), 0);
			if (output_buffer_size > encoding_buffer_size) {
				encoding_buffer = make_unsafe_uniq_array_uninitialized<unsigned char>(output_buffer_size);
				encoding_buffer_size = output_buffer_size;
			}

			// FIXME: can we compress directly to the segment? that would save a copy
			// I think yes?
			// We can give the segment as destination, and limit the size
			// it will tell us when it can't fit everything
			// worst case we can just check if the rest of the metadata fits when we remove the last string that it was
			// able to encode I believe 'duckdb_fsst_compress' tells us how many of the input strings it was able to
			// compress We can work backwards from there to see how many strings actually fit (probably worst case ret-1
			// ??)
			auto fsst_encoder = reinterpret_cast<duckdb_fsst_encoder_t *>(encoder);
			auto res = duckdb_fsst_compress(fsst_encoder, input_string_lengths.size(), input_string_lengths.data(),
			                                input_string_ptrs.data(), output_buffer_size, encoding_buffer.get(),
			                                compressed_sizes.data(), compressed_ptrs.data());
			if (res != input_string_lengths.size()) {
				throw FatalException("FSST compression failed to compress all input strings");
			}

			for (idx_t j = 0; j < input_string_lengths.size(); j++) {
				uint32_t size = UnsafeNumericCast<uint32_t>(compressed_sizes[j]);
				string_t encoded_string((const char *)compressed_ptrs[j], size); // NOLINT;

#ifdef DEBUG
				//! Verify that we can decompress the string
				auto &uncompressed_str = strings[encoded_input.offset + j];
				decompress_buffer.resize(uncompressed_str.GetSize() + 1 + 100);
				auto decoded_std_string =
				    FSSTPrimitives::DecompressValue((void *)temp_decoder, (const char *)compressed_ptrs[j],
				                                    (idx_t)compressed_sizes[j], decompress_buffer);

				D_ASSERT(decoded_std_string.size() == uncompressed_str.GetSize());
				string_t decompressed_string((const char *)decompress_buffer.data(), uncompressed_str.GetSize());
				D_ASSERT(decompressed_string == uncompressed_str);
#endif

				encoded_input.data.push_back(encoded_string);
			}
		}

#ifdef DEBUG
		//! Verify that we can decompress the strings (nothing weird happened to them)
		for (idx_t j = encoded_input.offset; j < count; j++) {
			auto &uncompressed_string = strings[j];
			auto &compressed_string = encoded_input.data[j - encoded_input.offset];

			decompress_buffer.resize(uncompressed_string.GetSize() + 1 + 100);
			auto decoded_std_string =
			    FSSTPrimitives::DecompressValue((void *)temp_decoder, (const char *)compressed_string.GetData(),
			                                    compressed_string.GetSize(), decompress_buffer);

			D_ASSERT(decoded_std_string.size() == uncompressed_string.GetSize());
			string_t decompressed_string((const char *)decompress_buffer.data(), uncompressed_string.GetSize());
			D_ASSERT(decompressed_string == uncompressed_string);
		}

#endif
		auto &string = encoded_input.data[i - encoded_input.offset];
		return AddToDictionary<DictionaryAppendState::ENCODED_ALL_UNIQUE>(*this, string, recalculate_indices_space);
	}
	};
	throw InternalException("Unreachable");
}

bool DictFSSTCompressionState::AllUnique() const {
	//! 1 is added for NULL always
	return string_lengths.size() - 1 == tuple_count;
}

DictionaryAppendState DictFSSTCompressionState::SwitchAppendState() {
	//! We were appending normally, the segment is full

	if (dictionary_offset < DICTIONARY_ENCODE_THRESHOLD) {
		return DictionaryAppendState::NOT_ENCODED;
	}

	DictionaryAppendState new_state;
	if (!analyze->contains_nulls && AllUnique()) {
		new_state = DictionaryAppendState::ENCODED_ALL_UNIQUE;
	} else {
		new_state = DictionaryAppendState::ENCODED;
	}

	vector<size_t> fsst_string_sizes;
	vector<unsigned char *> fsst_string_ptrs;

	uint32_t offset = 0;
	data_ptr_t dictionary_start =
	    AlignPointer<sizeof(void *)>(current_handle.Ptr() + sizeof(dict_fsst_compression_header_t));
	D_ASSERT(dictionary_offset > string_t::INLINE_BYTES && dictionary_offset <= string_t::MAX_STRING_SIZE);
	auto dict_copy = uncompressed_dictionary_copy.EmptyString(dictionary_offset);
	memcpy((void *)dict_copy.GetData(), (void *)dictionary_start, dictionary_offset);
	auto uncompressed_start = dict_copy.GetData();
	// Skip index 0, that's reserved for NULL
	for (idx_t i = 1; i < string_lengths.size(); i++) {
		auto length = string_lengths[i];
		auto start = uncompressed_start + offset;
		fsst_string_sizes.push_back(length);
		fsst_string_ptrs.push_back((unsigned char *)start); // NOLINT
		offset += length;
	}
	D_ASSERT(offset == dictionary_offset);

	// Create the encoder
	auto string_count = string_lengths.size() - 1;
	encoder = reinterpret_cast<void *>(
	    duckdb_fsst_create(string_count, fsst_string_sizes.data(), fsst_string_ptrs.data(), 0));
	auto fsst_encoder = reinterpret_cast<duckdb_fsst_encoder_t *>(encoder);

	auto compressed_ptrs = vector<unsigned char *>(string_count, nullptr);
	auto compressed_sizes = vector<size_t>(string_count, 0);

	// Compress the dictionary, straight to the segment
	auto res = duckdb_fsst_compress(fsst_encoder, string_count, fsst_string_sizes.data(), fsst_string_ptrs.data(),
	                                dictionary_offset, (unsigned char *)dictionary_start, compressed_sizes.data(),
	                                compressed_ptrs.data());
	if (res != string_count) {
		// The dictionary does not compress well enough to use FSST
		// continue filling the remaining bytes without encoding

		// We compressed directly to the segment, in the hopes this would fit and decrease in size
		// which it sadly didn't, so now we need to undo a bunch of things

		memcpy(dictionary_start, dict_copy.GetData(), dictionary_offset);
		uncompressed_dictionary_copy.Destroy();
		duckdb_fsst_destroy(fsst_encoder);
		encoder = nullptr;
		return DictionaryAppendState::NOT_ENCODED;
	}

	idx_t new_size = 0;
	for (idx_t i = 0; i < string_count; i++) {
		new_size += compressed_sizes[i];
	}

	if (new_state == DictionaryAppendState::ENCODED_ALL_UNIQUE) {
		//! We omit the selection buffer in this mode, setting the width to 0 makes the RequiredSpace result not include
		//! the selection buffer space.
		dictionary_indices_width = 0;
	}

	// Export the symbol table, so we get an accurate measurement of the size
	if (!fsst_serialized_symbol_table) {
		fsst_serialized_symbol_table =
		    make_unsafe_uniq_array_uninitialized<unsigned char>(sizeof(duckdb_fsst_decoder_t));
	}
	symbol_table_size = duckdb_fsst_export(fsst_encoder, fsst_serialized_symbol_table.get());

#ifdef DEBUG
	auto temp_decoder = alloca(sizeof(duckdb_fsst_decoder_t));
	duckdb_fsst_import((duckdb_fsst_decoder_t *)temp_decoder, fsst_serialized_symbol_table.get());

	vector<unsigned char> decompress_buffer;
#endif

	// Rewrite the dictionary
	current_string_map.clear();
	uint32_t max_length = 0;
	if (new_state == DictionaryAppendState::ENCODED) {
		offset = 0;
		auto uncompressed_dictionary_ptr = dict_copy.GetData();
		for (idx_t i = 0; i < string_count; i++) {
			auto size = UnsafeNumericCast<uint32_t>(compressed_sizes[i]);
			if (size > max_length) {
				max_length = size;
			}
			// Skip index 0, reserved for NULL
			uint32_t dictionary_index = UnsafeNumericCast<uint32_t>(i + 1);
			auto uncompressed_str_len = string_lengths[dictionary_index];

			string_t dictionary_string(uncompressed_dictionary_ptr + offset, uncompressed_str_len);
			current_string_map.insert({dictionary_string, dictionary_index});

#ifdef DEBUG
			//! Verify that we can decompress the string
			decompress_buffer.resize(uncompressed_str_len + 1 + 100);
			FSSTPrimitives::DecompressValue((void *)temp_decoder, (const char *)compressed_ptrs[i],
			                                (idx_t)compressed_sizes[i], decompress_buffer);

			string_t decompressed_string((const char *)decompress_buffer.data(), uncompressed_str_len);
			D_ASSERT(decompressed_string == dictionary_string);
#endif

			string_lengths[dictionary_index] = size;
			offset += uncompressed_str_len;
		}
	} else {
		D_ASSERT(new_state == DictionaryAppendState::ENCODED_ALL_UNIQUE);
		for (idx_t i = 0; i < string_count; i++) {
			auto &start = compressed_ptrs[i];
			auto size = UnsafeNumericCast<uint32_t>(compressed_sizes[i]);
			if (size > max_length) {
				max_length = size;
			}
			// Skip index 0, reserved for NULL
			uint32_t dictionary_index = UnsafeNumericCast<uint32_t>(i + 1);
			string_lengths[dictionary_index] = size;
			string_t dictionary_string((const char *)start, UnsafeNumericCast<uint32_t>(size)); // NOLINT

			current_string_map.insert({dictionary_string, dictionary_index});
		}
	}
	dictionary_offset = new_size;
	string_lengths_width = BitpackingPrimitives::MinimumBitWidth(max_length);
	string_lengths_space = BitpackingPrimitives::GetRequiredSize(dict_count, string_lengths_width);
	real_string_lengths_width = string_lengths_width;
	return new_state;
}

void DictFSSTCompressionState::Compress(Vector &scan_vector, idx_t count) {
	UnifiedVectorFormat vector_format;
	scan_vector.ToUnifiedFormat(count, vector_format);

	EncodedInput encoded_input;
	for (idx_t i = 0; i < count; i++) {
		bool fits = CompressInternal(vector_format, encoded_input, i, count);
		if (fits) {
			continue;
		}
		if (append_state == DictionaryAppendState::REGULAR) {
			append_state = SwitchAppendState();
			D_ASSERT(append_state != DictionaryAppendState::REGULAR);
			fits = CompressInternal(vector_format, encoded_input, i, count);
			if (fits) {
				continue;
			}
		}
		Flush(false);
		encoded_input.data.clear();
		encoded_input.offset = 0;
		fits = CompressInternal(vector_format, encoded_input, i, count);
		if (!fits) {
			throw FatalException("Compressing directly after Flush doesn't fit");
		}
	}
}

void DictFSSTCompressionState::FinalizeCompress() {
	Flush(true);
}

} // namespace dict_fsst
} // namespace duckdb
