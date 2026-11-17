//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/prefetch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! Prefetch a future address, computing it only when supported and within bounds.
template <class GET_ADDRESS>
inline void Prefetch(idx_t index, idx_t count, idx_t distance, GET_ADDRESS get_address) {
	D_ASSERT(index <= count);
#if defined(__GNUC__) || defined(__clang__)
	if (count - index > distance) {
		__builtin_prefetch(get_address(index + distance), 0, 3);
	}
#endif
}

} // namespace duckdb
