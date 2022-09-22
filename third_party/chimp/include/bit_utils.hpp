//===----------------------------------------------------------------------===//
//                         DuckDB
//
// third_party/chimp/include/bit_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb_chimp {

template <typename R>
static constexpr R bitmask(unsigned int const bits)
{
	return (((uint64_t) (bits < (sizeof(R) * 8))) << (bits & ((sizeof(R) * 8) - 1))) - 1U;
}

} //namespace duckdb_chimp
