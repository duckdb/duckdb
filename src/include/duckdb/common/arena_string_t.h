#pragma once

#include "duckdb/storage/arena_allocator.hpp"

#include <type_traits>

namespace duckdb {

class arena_string_t {

private:
	ArenaAllocator &arena;
};

} // namespace duckdb
