//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/for_vector_helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

#define FOR_SWITCH_LOGICAL(TYPE, TYPE_NAME, ...)                                                                       \
	switch (TYPE) {                                                                                                    \
	case PhysicalType::INT16: {                                                                                        \
		typedef int16_t TYPE_NAME;                                                                                     \
		__VA_ARGS__;                                                                                                   \
		break;                                                                                                         \
	}                                                                                                                  \
	case PhysicalType::INT32: {                                                                                        \
		typedef int32_t TYPE_NAME;                                                                                     \
		__VA_ARGS__;                                                                                                   \
		break;                                                                                                         \
	}                                                                                                                  \
	case PhysicalType::INT64: {                                                                                        \
		typedef int64_t TYPE_NAME;                                                                                     \
		__VA_ARGS__;                                                                                                   \
		break;                                                                                                         \
	}                                                                                                                  \
	case PhysicalType::INT128: {                                                                                       \
		typedef hugeint_t TYPE_NAME;                                                                                   \
		__VA_ARGS__;                                                                                                   \
		break;                                                                                                         \
	}                                                                                                                  \
	case PhysicalType::UINT16: {                                                                                       \
		typedef uint16_t TYPE_NAME;                                                                                    \
		__VA_ARGS__;                                                                                                   \
		break;                                                                                                         \
	}                                                                                                                  \
	case PhysicalType::UINT32: {                                                                                       \
		typedef uint32_t TYPE_NAME;                                                                                    \
		__VA_ARGS__;                                                                                                   \
		break;                                                                                                         \
	}                                                                                                                  \
	case PhysicalType::UINT64: {                                                                                       \
		typedef uint64_t TYPE_NAME;                                                                                    \
		__VA_ARGS__;                                                                                                   \
		break;                                                                                                         \
	}                                                                                                                  \
	case PhysicalType::UINT128: {                                                                                      \
		typedef uhugeint_t TYPE_NAME;                                                                                  \
		__VA_ARGS__;                                                                                                   \
		break;                                                                                                         \
	}                                                                                                                  \
	default:                                                                                                           \
		throw InternalException("Unsupported logical type for FOR vector dispatch: %s", TypeIdToString(TYPE));         \
	}

#define FOR_SWITCH_STORED(TYPE, TYPE_NAME, ...)                                                                        \
	switch (TYPE) {                                                                                                    \
	case PhysicalType::UINT8: {                                                                                        \
		typedef uint8_t TYPE_NAME;                                                                                     \
		__VA_ARGS__;                                                                                                   \
		break;                                                                                                         \
	}                                                                                                                  \
	case PhysicalType::UINT16: {                                                                                       \
		typedef uint16_t TYPE_NAME;                                                                                    \
		__VA_ARGS__;                                                                                                   \
		break;                                                                                                         \
	}                                                                                                                  \
	case PhysicalType::UINT32: {                                                                                       \
		typedef uint32_t TYPE_NAME;                                                                                    \
		__VA_ARGS__;                                                                                                   \
		break;                                                                                                         \
	}                                                                                                                  \
	case PhysicalType::UINT64: {                                                                                       \
		typedef uint64_t TYPE_NAME;                                                                                    \
		__VA_ARGS__;                                                                                                   \
		break;                                                                                                         \
	}                                                                                                                  \
	default:                                                                                                           \
		throw InternalException("Unsupported stored type for FOR vector dispatch: %s", TypeIdToString(TYPE));          \
	}

template <class T>
struct FORUnsignedType {
	using type = typename MakeUnsigned<T>::type;
};
template <>
struct FORUnsignedType<hugeint_t> {
	using type = uhugeint_t;
};

template <class T, bool IS_SIGNED = NumericLimits<T>::IsSigned()>
struct FORValueOps;

template <class T>
struct FORValueOps<T, false> {
	static inline uhugeint_t ToUnsignedStorage(T value) {
		return uhugeint_t(UnsafeNumericCast<uint64_t>(value));
	}
	static inline T FromUnsignedStorage(const uhugeint_t &value) {
		return UnsafeNumericCast<T>(value.lower);
	}
};

template <class T>
struct FORValueOps<T, true> {
	static inline uhugeint_t ToUnsignedStorage(T value) {
		return static_cast<uhugeint_t>(Hugeint::Convert(value));
	}
	static inline T FromUnsignedStorage(const uhugeint_t &value) {
		return UnsafeNumericCast<T>(static_cast<hugeint_t>(value));
	}
};

template <>
struct FORValueOps<hugeint_t, true> {
	static inline uhugeint_t ToUnsignedStorage(hugeint_t value) {
		return static_cast<uhugeint_t>(value);
	}
	static inline hugeint_t FromUnsignedStorage(const uhugeint_t &value) {
		return static_cast<hugeint_t>(value);
	}
};

template <>
struct FORValueOps<uhugeint_t, false> {
	static inline uhugeint_t ToUnsignedStorage(uhugeint_t value) {
		return value;
	}
	static inline uhugeint_t FromUnsignedStorage(const uhugeint_t &value) {
		return value;
	}
};

} // namespace duckdb
