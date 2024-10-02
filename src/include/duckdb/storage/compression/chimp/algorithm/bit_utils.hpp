//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/algorithm/bit_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

template <class R>
struct BitUtils {
	static constexpr R Mask(unsigned int const bits) {
		return UnsafeNumericCast<R>((((uint64_t)(bits < (sizeof(R) * 8))) << (bits & ((sizeof(R) * 8) - 1))) - 1U);
	}
};

} // namespace duckdb
