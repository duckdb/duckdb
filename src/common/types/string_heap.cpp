
#include "common/types/string_heap.hpp"

using namespace duckdb;
using namespace std;

#define MINIMUM_HEAP_SIZE 4096

StringHeap::StringHeap() {}

const char *StringHeap::AddString(const char *data, size_t len) {
	if (head.size() == 0 ||
	    head.back().current_position + len >= head.back().maximum_size) {
		// have to make a new entry
		head.push_back(
		    StringChunk(std::max(len + 1, (size_t)MINIMUM_HEAP_SIZE)));
	}
	auto &entry = head.back();
	auto insert_pos = entry.data.get() + entry.current_position;
	strcpy(insert_pos, data);
	entry.current_position += len + 1;
	return insert_pos;
}

const char *StringHeap::AddString(const char *data) {
	return AddString(data, strlen(data));
}

const char *StringHeap::AddString(const std::string &data) {
	return AddString(data.c_str(), data.size());
}
