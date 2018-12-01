//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/types/string_heap.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/value.hpp"

namespace duckdb {
//! A string heap is the owner of a set of strings, strings can be inserted into
//! it On every insert, a pointer to the inserted string is returned The
//! returned pointer will remain valid until the StringHeap is destroyed
class StringHeap {
  public:
	StringHeap();

	void Destroy() {
		tail = nullptr;
		chunk = nullptr;
	}

	void Move(StringHeap &other) {
		assert(!other.chunk);
		other.tail = tail;
		other.chunk = move(chunk);
		tail = nullptr;
	}

	//! Add a string to the string heap, returns a pointer to the string
	const char *AddString(const char *data, size_t len);
	//! Add a string to the string heap, returns a pointer to the string
	const char *AddString(const char *data);
	//! Add a string to the string heap, returns a pointer to the string
	const char *AddString(const std::string &data);
	//! Add all strings from a different string heap to this string heap
	void MergeHeap(StringHeap &heap);

  private:
	struct StringChunk {
		StringChunk(size_t size) : current_position(0), maximum_size(size) {
			data = std::unique_ptr<char[]>(new char[maximum_size]);
		}

		std::unique_ptr<char[]> data;
		size_t current_position;
		size_t maximum_size;
		std::unique_ptr<StringChunk> prev;
	};
	StringChunk *tail;
	std::unique_ptr<StringChunk> chunk;
};

} // namespace duckdb