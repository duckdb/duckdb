//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/arena_string.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/arena_allocator.hpp"

namespace duckdb {

class ArenaString {
public:
	ArenaString(ArenaAllocator &arena_p, const char *data_p, size_t size_p) : size(size_p), arena(arena_p) {
		// copy the data into the arena
		const auto data_ptr = arena.AllocateAligned(sizeof(char) * GetSize());
		memcpy(data_ptr, data_p, GetSize());

		data = (char *)data_ptr; // NOLINT
	};

	ArenaString(ArenaAllocator &arena, const char *data_p) // NOLINT: Allow implicit conversion from `const char*`
	    : ArenaString(arena, data_p, UnsafeNumericCast<uint32_t>(strlen(data_p))) {
	}

	ArenaString(ArenaAllocator &arena, const string &data_p) // NOLINT: Allow implicit conversion from `const char*`
	    : ArenaString(arena, data_p.c_str(), UnsafeNumericCast<uint32_t>(data_p.size())) {
	}

	const char *GetData() const {
		return data;
	}

	idx_t GetSize() const {
		return size;
	}

private:
	const char *data;
	const idx_t size;
	ArenaAllocator &arena;
};

} // namespace duckdb
