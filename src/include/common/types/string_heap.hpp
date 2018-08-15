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

#include "common/internal_types.hpp"
#include "common/types/value.hpp"

#include <list>

namespace duckdb {
//! A string heap is the owner of a set of strings, strings can be inserted into
//! it On every insert, a pointer to the inserted string is returned The
//! returned pointer will remain valid until the StringHeap is destroyed
class StringHeap {
  public:
	StringHeap();

	//! Add a string to the string heap, returns a pointer to the string
	const char *AddString(const char *data, size_t len);
	//! Add a string to the string heap, returns a pointer to the string
	const char *AddString(const char *data);
	//! Add a string to the string heap, returns a pointer to the string
	const char *AddString(const std::string &data);

  private:
	struct StringChunk {
		StringChunk(size_t size) : current_position(0), maximum_size(size) {
			data = std::unique_ptr<char[]>(new char[maximum_size]);
		}

		std::unique_ptr<char[]> data;
		size_t current_position;
		size_t maximum_size;
	};

	std::list<StringChunk> head;
};

} // namespace duckdb