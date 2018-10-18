
#include "common/types/string_heap.hpp"

using namespace duckdb;
using namespace std;

#define MINIMUM_HEAP_SIZE 4096

StringHeap::StringHeap() : tail(nullptr) {}

const char *StringHeap::AddString(const char *data, size_t len) {
	if (!chunk || chunk->current_position + len >= chunk->maximum_size) {
		// have to make a new entry
		auto new_chunk = make_unique<StringChunk>(
		    std::max(len + 1, (size_t)MINIMUM_HEAP_SIZE));
		new_chunk->prev = move(chunk);
		chunk = move(new_chunk);
		if (!tail) {
			tail = chunk.get();
		}
	}
	auto insert_pos = chunk->data.get() + chunk->current_position;
	strcpy(insert_pos, data);
	chunk->current_position += len + 1;
	return insert_pos;
}

const char *StringHeap::AddString(const char *data) {
	return AddString(data, strlen(data));
}

const char *StringHeap::AddString(const std::string &data) {
	return AddString(data.c_str(), data.size());
}

void StringHeap::MergeHeap(StringHeap &other) {
	if (!other.tail) {
		return;
	}
	other.tail->prev = move(chunk);
	this->chunk = move(other.chunk);
	if (!tail) {
		tail = this->chunk.get();
	}
	other.tail = nullptr;
}