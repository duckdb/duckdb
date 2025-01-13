#include "duckdb/storage/compression/dict_fsst/analyze.hpp"
#include "fsst.h"

namespace duckdb {
namespace dict_fsst {

DictFSSTAnalyzeState::DictFSSTAnalyzeState(const CompressionInfo &info)
    : DictFSSTCompressionState(info), segment_count(0), current_tuple_count(0), current_unique_count(0),
      current_dict_size(0), current_width(0), next_width(0) {
}

DictFSSTAnalyzeState::~DictFSSTAnalyzeState() {
	if (encoder) {
		auto fsst_encoder = reinterpret_cast<duckdb_fsst_encoder_t *>(encoder);
		duckdb_fsst_destroy(fsst_encoder);
	}
}

optional_idx DictFSSTAnalyzeState::LookupString(const string_t &str) {
	return current_string_map.count(str) ? 1 : optional_idx();
}

void DictFSSTAnalyzeState::AddNewString(const StringData &string_data) {
	D_ASSERT(!string_data.encoded_string);
	auto &str = string_data.string;

	current_tuple_count++;
	current_unique_count++;
	current_dict_size += str.GetSize();

	uint32_t string_length = UnsafeNumericCast<uint32_t>(string_data.string.GetSize());
	if (append_state == DictionaryAppendState::ENCODED) {
		//! Optimistic assumption about the compressed length;
		string_length /= 2;
	}

	if (str.IsInlined()) {
		current_string_map.emplace(str, string_length);
	} else {
		current_string_map.emplace(heap.AddBlob(str), string_length);
	}
	current_width = next_width;
}

void DictFSSTAnalyzeState::AddLookup(uint32_t) {
	current_tuple_count++;
}

void DictFSSTAnalyzeState::AddNull() {
	current_tuple_count++;
}

void DictFSSTAnalyzeState::EncodeInputStrings(UnifiedVectorFormat &input, idx_t count) {
	return;
}

bool DictFSSTAnalyzeState::EncodeDictionary() {
	if (current_dict_size < DICTIONARY_ENCODE_THRESHOLD) {
		append_state = DictionaryAppendState::NOT_ENCODED;
		return false;
	}

	vector<size_t> fsst_string_sizes;
	vector<unsigned char *> fsst_string_ptrs;

	// Skip index 0, that's reserved for NULL
	for (auto &pair : current_string_map) {
		auto &str = pair.first;
		fsst_string_sizes.push_back(str.GetSize());
		fsst_string_ptrs.push_back((unsigned char *)str.GetData()); // NOLINT
	}

	// Create the encoder
	auto string_count = current_string_map.size();
	encoder =
	    reinterpret_cast<void *>(duckdb_fsst_create(string_count, &fsst_string_sizes[0], &fsst_string_ptrs[0], 0));
	auto fsst_encoder = reinterpret_cast<duckdb_fsst_encoder_t *>(encoder);

	size_t output_buffer_size = 7 + 2 * current_dict_size; // size as specified in fsst.h
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
	if (new_size > current_dict_size + DICTIONARY_ENCODE_THRESHOLD) {
		// The dictionary does not compress well enough to use FSST
		// continue filling the remaining bytes without encoding
		duckdb_fsst_destroy(fsst_encoder);
		encoder = nullptr;
		append_state = DictionaryAppendState::NOT_ENCODED;
		return false;
	}

	for (idx_t i = 0; i < string_count; i++) {
		uint32_t size = UnsafeNumericCast<uint32_t>(compressed_sizes[i]);
		auto uncompressed_str = string_t((const char *)fsst_string_ptrs[i], (uint32_t)fsst_string_sizes[i]); // NOLINT
		current_string_map[uncompressed_str] = size;
	}
	current_dict_size = new_size;

	append_state = DictionaryAppendState::ENCODED;
	return true;
}

StringData DictFSSTAnalyzeState::GetString(const string_t *strings, idx_t index, idx_t raw_index) {
	return StringData(strings[index]);
}

idx_t DictFSSTAnalyzeState::RequiredSpace(bool new_string, idx_t string_size) {
	idx_t required_space = 0;
	if (append_state == DictionaryAppendState::ENCODED) {
		required_space += symbol_table_size;
	}

	if (!new_string) {
		required_space += DictFSSTCompression::RequiredSpace(current_tuple_count + 1, current_unique_count,
		                                                     current_dict_size, current_width);
	} else {
		next_width = BitpackingPrimitives::MinimumBitWidth(current_unique_count + 2); // 1 for null, one for new string
		required_space += DictFSSTCompression::RequiredSpace(current_tuple_count + 1, current_unique_count + 1,
		                                                     current_dict_size + string_size, next_width);
	}
	return required_space;
}

void DictFSSTAnalyzeState::Flush(bool final) {
	if (!current_tuple_count) {
		D_ASSERT(!current_unique_count);
		D_ASSERT(!current_dict_size);
		D_ASSERT(current_string_map.empty());
		return;
	}
	idx_t required_space = 0;
	if (append_state == DictionaryAppendState::ENCODED) {
		required_space += symbol_table_size;
	}
	auto width = BitpackingPrimitives::MinimumBitWidth(current_unique_count + 1);
	required_space +=
	    DictFSSTCompression::RequiredSpace(current_tuple_count, current_unique_count, current_dict_size, width);

	total_space += required_space;

	segment_count++;
	current_tuple_count = 0;
	current_unique_count = 0;
	current_dict_size = 0;
	current_string_map.clear();
	heap.Destroy();
}

void DictFSSTAnalyzeState::Verify() {
}

} // namespace dict_fsst
} // namespace duckdb
