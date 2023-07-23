//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/typedefs.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace duckdb {

//! a saner size_t for loop indices etc
typedef uint64_t idx_t;

//! The type used for row identifiers
typedef int64_t row_t;

//! The type used for hashes
typedef uint64_t hash_t;

//! data pointers
typedef uint8_t data_t;
typedef data_t *data_ptr_t;
typedef const data_t *const_data_ptr_t;

//! Type used for the selection vector
typedef uint32_t sel_t;
//! Type used for transaction timestamps
typedef idx_t transaction_t;

//! Type used for column identifiers
typedef idx_t column_t;
//! Type used for storage (column) identifiers
typedef idx_t storage_t;

template <class SRC>
data_ptr_t data_ptr_cast(SRC *src) {
	return reinterpret_cast<data_ptr_t>(src);
}

template <class SRC>
const_data_ptr_t const_data_ptr_cast(const SRC *src) {
	return reinterpret_cast<const_data_ptr_t>(src);
}

template <class SRC>
char *char_ptr_cast(SRC *src) {
	return reinterpret_cast<char *>(src);
}

template <class SRC>
const char *const_char_ptr_cast(const SRC *src) {
	return reinterpret_cast<const char *>(src);
}

template <class SRC>
const unsigned char *const_uchar_ptr_cast(const SRC *src) {
	return reinterpret_cast<const unsigned char *>(src);
}

template <class SRC>
uintptr_t CastPointerToValue(SRC *src) {
	return uintptr_t(src);
}

} // namespace duckdb
