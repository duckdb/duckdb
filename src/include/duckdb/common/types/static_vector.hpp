//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/static_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

#include <type_traits>

namespace duckdb {

//! The StaticVector is an alias for creating a vector of a specific type
template <class T> class StaticVector : public Vector {
public:
	StaticVector() {
		owned_data = unique_ptr<data_t[]>(new data_t[sizeof(T) * STANDARD_VECTOR_SIZE]);
		data = owned_data.get();
		if (std::is_same<T, bool>::value) {
			type = TypeId::BOOLEAN;
		} else if (std::is_same<T, int8_t>::value) {
			type = TypeId::TINYINT;
		} else if (std::is_same<T, int16_t>::value) {
			type = TypeId::SMALLINT;
		} else if (std::is_same<T, int>::value) {
			type = TypeId::INTEGER;
		} else if (std::is_same<T, int64_t>::value) {
			type = TypeId::BIGINT;
		} else if (std::is_same<T, uint64_t>::value) {
			type = TypeId::HASH;
		} else if (std::is_same<T, double>::value) {
			type = TypeId::DOUBLE;
		} else {
			// unsupported type!
			assert(0);
		}
	}
};

// this exists because the is_same check used above is somewhat unpredictable
class StaticPointerVector : public Vector {
public:
	StaticPointerVector() {
		owned_data = unique_ptr<data_t[]>(new data_t[sizeof(uintptr_t) * STANDARD_VECTOR_SIZE]);
		data = owned_data.get();
		type = TypeId::POINTER;
	}
};

} // namespace duckdb
