#include "duckdb/storage/compression/dict_fsst/analyze.hpp"

namespace duckdb {
namespace dict_fsst {

DictFSSTAnalyzeState::DictFSSTAnalyzeState(const CompressionInfo &info)
    : DictFSSTCompressionState(info), segment_count(0), current_tuple_count(0), current_unique_count(0),
      current_dict_size(0), current_width(0), next_width(0) {
}

bool DictFSSTAnalyzeState::LookupString(string_t str) {
	return current_set.count(str);
}

void DictFSSTAnalyzeState::AddNewString(string_t str) {
	current_tuple_count++;
	current_unique_count++;
	current_dict_size += str.GetSize();
	if (str.IsInlined()) {
		current_set.insert(str);
	} else {
		current_set.insert(heap.AddBlob(str));
	}
	current_width = next_width;
}

void DictFSSTAnalyzeState::AddLastLookup() {
	current_tuple_count++;
}

void DictFSSTAnalyzeState::AddNull() {
	current_tuple_count++;
}

bool DictFSSTAnalyzeState::CalculateSpaceRequirements(bool new_string, idx_t string_size) {
	if (!new_string) {
		return DictFSSTCompression::HasEnoughSpace(current_tuple_count + 1, current_unique_count, current_dict_size,
		                                           current_width, info.GetBlockSize());
	}
	next_width = BitpackingPrimitives::MinimumBitWidth(current_unique_count + 2); // 1 for null, one for new string
	return DictFSSTCompression::HasEnoughSpace(current_tuple_count + 1, current_unique_count + 1,
	                                           current_dict_size + string_size, next_width, info.GetBlockSize());
}

void DictFSSTAnalyzeState::Flush(bool final) {
	segment_count++;
	current_tuple_count = 0;
	current_unique_count = 0;
	current_dict_size = 0;
	current_set.clear();
}
void DictFSSTAnalyzeState::Verify() {
}

} // namespace dict_fsst
} // namespace duckdb
