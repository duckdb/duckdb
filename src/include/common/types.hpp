//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/assert.hpp"
#include "common/constants.hpp"
#include "common/enums.hpp"

#include <type_traits>

namespace duckdb {

template <class T> constexpr bool IsValidType() {
	return std::is_same<T, bool>() || std::is_same<T, int8_t>() || std::is_same<T, int16_t>() ||
	       std::is_same<T, int32_t>() || std::is_same<T, int64_t>() || std::is_same<T, uint64_t>() ||
	       std::is_same<T, double>() || std::is_same<T, const char *>();
}

//! Returns the TypeId for the given type
template <class T> TypeId GetTypeId() {
	static_assert(IsValidType<T>(), "Invalid type for GetTypeId");
	if (std::is_same<T, bool>()) {
		return TypeId::BOOLEAN;
	} else if (std::is_same<T, int8_t>()) {
		return TypeId::TINYINT;
	} else if (std::is_same<T, int16_t>()) {
		return TypeId::SMALLINT;
	} else if (std::is_same<T, int32_t>()) {
		return TypeId::INTEGER;
	} else if (std::is_same<T, int64_t>()) {
		return TypeId::BIGINT;
	} else if (std::is_same<T, uint64_t>()) {
		return TypeId::POINTER;
	} else if (std::is_same<T, double>()) {
		return TypeId::DECIMAL;
	} else if (std::is_same<T, const char *>()) {
		return TypeId::VARCHAR;
	} else {
		assert(0);
		return TypeId::INVALID;
	}
}

string TypeIdToString(TypeId type);
size_t GetTypeIdSize(TypeId type);
bool TypeIsConstantSize(TypeId type);
bool TypeIsIntegral(TypeId type);
bool TypeIsNumeric(TypeId type);
bool TypeIsInteger(TypeId type);

template <class T> bool IsIntegerType() {
	return TypeIsIntegral(GetTypeId<T>());
}

}; // namespace duckdb
