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

template <class DEST, class SRC>
DEST *data_ptr_cast(SRC *src) {
	static_assert(sizeof(DEST) == 1, "data_ptr_cast should only be used to cast to char, data_t, unsigned char");
	return (DEST *)src;
}

template <class DEST, class SRC>
const DEST *const_data_ptr_cast(SRC *src) {
	static_assert(sizeof(DEST) == 1, "data_ptr_cast should only be used to cast to char, data_t, unsigned char");
	return (DEST *)src;
}

} // namespace duckdb
