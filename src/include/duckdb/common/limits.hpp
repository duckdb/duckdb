//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/limits.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

#include <limits>

namespace duckdb {

//! Returns the minimum value that can be stored in a given type
template <class T> int64_t MinimumValue() {
	assert(IsIntegerType<T>());
	if (std::is_same<T, int8_t>()) {
		return std::numeric_limits<int8_t>::min() + 1;
	} else if (std::is_same<T, int16_t>()) {
		return std::numeric_limits<int16_t>::min() + 1;
	} else if (std::is_same<T, int32_t>()) {
		return std::numeric_limits<int32_t>::min() + 1;
	} else if (std::is_same<T, int64_t>()) {
		return std::numeric_limits<int64_t>::min() + 1;
	} else if (std::is_same<T, uint64_t>()) {
		return std::numeric_limits<uint64_t>::min();
	} else if (std::is_same<T, uintptr_t>()) {
		return std::numeric_limits<uintptr_t>::min();
	} else {
		assert(0);
		return 0;
	}
}

//! Returns the maximum value that can be stored in a given type
template <class T> int64_t MaximumValue() {
	assert(IsIntegerType<T>());
	if (std::is_same<T, int8_t>()) {
		return std::numeric_limits<int8_t>::max();
	} else if (std::is_same<T, int16_t>()) {
		return std::numeric_limits<int16_t>::max();
	} else if (std::is_same<T, int32_t>()) {
		return std::numeric_limits<int32_t>::max();
	} else if (std::is_same<T, int64_t>()) {
		return std::numeric_limits<int64_t>::max();
	} else if (std::is_same<T, uint64_t>()) {
		return std::numeric_limits<int64_t>::max();
	} else if (std::is_same<T, uintptr_t>()) {
		return std::numeric_limits<uintptr_t>::max();
	} else {
		assert(0);
		return 0;
	}
}

//! Returns the minimum value that can be stored in a given type
int64_t MinimumValue(TypeId type);
//! Returns the maximum value that can be stored in a given type
uint64_t MaximumValue(TypeId type);
//! Returns the minimal type that guarantees an integer value from not
//! overflowing
TypeId MinimalType(int64_t value);

} // namespace duckdb
