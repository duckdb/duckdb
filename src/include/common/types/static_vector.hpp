//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/types/static_vector.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/vector.hpp"

#include <type_traits>

namespace duckdb {

//! The StaticVector is a vector that stores its elements on the heap
template <class T> class StaticVector : public Vector {
  public:
	StaticVector() {
		data = (char *)elements;
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
			type = TypeId::POINTER;
		} else if (std::is_same<T, double>::value) {
			type = TypeId::DECIMAL;
		} else {
			// unsupported!
			assert(0);
		}
	}

  private:
	T elements[STANDARD_VECTOR_SIZE];
};

} // namespace duckdb
