//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/type_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/types/double_na_equal.hpp"

namespace duckdb {
struct bignum_t;

//! Returns the PhysicalType for the given type
template <class T>
PhysicalType GetTypeId() {
	using TYPE = typename std::remove_cv<T>::type;

	if (std::is_same<TYPE, bool>()) {
		return PhysicalType::BOOL;
	} else if (std::is_same<TYPE, int8_t>()) {
		return PhysicalType::INT8;
	} else if (std::is_same<TYPE, int16_t>()) {
		return PhysicalType::INT16;
	} else if (std::is_same<TYPE, int32_t>()) {
		return PhysicalType::INT32;
	} else if (std::is_same<TYPE, int64_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<TYPE, uint8_t>()) {
		return PhysicalType::UINT8;
	} else if (std::is_same<TYPE, uint16_t>()) {
		return PhysicalType::UINT16;
	} else if (std::is_same<TYPE, uint32_t>()) {
		return PhysicalType::UINT32;
	} else if (std::is_same<TYPE, uint64_t>()) {
		return PhysicalType::UINT64;
	} else if (std::is_same<TYPE, idx_t>() || std::is_same<TYPE, const idx_t>()) {
		return PhysicalType::UINT64;
	} else if (std::is_same<TYPE, hugeint_t>()) {
		return PhysicalType::INT128;
	} else if (std::is_same<TYPE, uhugeint_t>()) {
		return PhysicalType::UINT128;
	} else if (std::is_same<TYPE, date_t>()) {
		return PhysicalType::INT32;
	} else if (std::is_same<TYPE, dtime_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<TYPE, dtime_tz_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<TYPE, dtime_ns_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<TYPE, timestamp_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<TYPE, timestamp_sec_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<TYPE, timestamp_ms_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<TYPE, timestamp_ns_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<TYPE, timestamp_tz_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<TYPE, float>() || std::is_same<TYPE, float_na_equal>()) {
		return PhysicalType::FLOAT;
	} else if (std::is_same<TYPE, double>() || std::is_same<TYPE, double_na_equal>()) {
		return PhysicalType::DOUBLE;
	} else if (std::is_same<TYPE, const char *>() || std::is_same<TYPE, char *>() || std::is_same<TYPE, string_t>() ||
	           std::is_same<TYPE, bignum_t>()) {
		return PhysicalType::VARCHAR;
	} else if (std::is_same<TYPE, interval_t>()) {
		return PhysicalType::INTERVAL;
	} else if (std::is_same<TYPE, list_entry_t>()) {
		return PhysicalType::LIST;
	} else if (std::is_pointer<TYPE>() || std::is_same<TYPE, uintptr_t>()) {
		if (sizeof(uintptr_t) == sizeof(uint32_t)) {
			return PhysicalType::UINT32;
		} else if (sizeof(uintptr_t) == sizeof(uint64_t)) {
			return PhysicalType::UINT64;
		} else {
			throw InternalException("Unsupported pointer size in GetTypeId");
		}
	} else {
		throw InternalException("Unsupported type in GetTypeId");
	}
}

template <class T>
bool StorageTypeCompatible(PhysicalType type) {
	using TYPE = typename std::remove_cv<T>::type;

	if (std::is_same<TYPE, int8_t>()) {
		return type == PhysicalType::INT8 || type == PhysicalType::BOOL;
	}
	if (std::is_same<TYPE, uint8_t>()) {
		return type == PhysicalType::UINT8 || type == PhysicalType::BOOL;
	}
	return type == GetTypeId<T>();
}

template <class T>
bool TypeIsNumber() {
	using TYPE = typename std::remove_cv<T>::type;

	return std::is_integral<TYPE>() || std::is_floating_point<TYPE>() || std::is_same<TYPE, hugeint_t>() ||
	       std::is_same<TYPE, uhugeint_t>();
}

template <class T>
bool IsValidType() {
	return GetTypeId<T>() != PhysicalType::INVALID;
}

template <class T>
bool IsIntegerType() {
	return TypeIsIntegral(GetTypeId<T>());
}

} // namespace duckdb
