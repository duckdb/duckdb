#pragma once

#include "duckdb/storage/arena_allocator.hpp"

#include <type_traits>

#include "types/string_type.hpp"

namespace duckdb {

class arena_string_t {
public:
	explicit arena_string_t(ArenaAllocator &arena) : arena(arena) {
	}

	arena_string_t(const arena_string_t &other) = delete;
	arena_string_t &operator=(const arena_string_t &other) = delete;

private:
	ArenaAllocator &arena;
	string_t value;
};

} // namespace duckdb
