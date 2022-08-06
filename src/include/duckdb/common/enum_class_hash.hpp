//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enum_class_hash.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>

namespace duckdb {
/* For compatibility with older C++ STL, an explicit hash class
   is required for enums with C++ sets and maps */
struct EnumClassHash {
	template <typename T>
	std::size_t operator()(T t) const {
		return static_cast<std::size_t>(t);
	}
};
} // namespace duckdb
