//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/algorithm/bit_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

template <class R>
struct BitUtils {
	static constexpr R Mask(unsigned int const bits) {
		return (((uint64_t)(bits < (sizeof(R) * 8))) << (bits & ((sizeof(R) * 8) - 1))) - 1U;
	}
};

} // namespace duckdb
