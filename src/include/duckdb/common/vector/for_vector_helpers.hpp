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

//! Type tag for compile-time type dispatch in DispatchLogicalType.
template <class T>
struct FORTypeTag {
	using type = T;
};

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
	static inline T AddDelta(T min_value, uint64_t delta) {
		return UnsafeNumericCast<T>(min_value + UnsafeNumericCast<T>(delta));
	}
	static inline bool TryGetDelta(T value, T min_value, uint64_t &delta) {
		if (value < min_value) {
			return false;
		}
		delta = UnsafeNumericCast<uint64_t>(value - min_value);
		return true;
	}
	static inline uint64_t GetDeltaUnsafe(T value, T min_value) {
		return UnsafeNumericCast<uint64_t>(value - min_value);
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
	static inline T AddDelta(T min_value, uint64_t delta) {
		using UNSIGNED_T = typename MakeUnsigned<T>::type;
		return static_cast<T>(static_cast<UNSIGNED_T>(min_value) + static_cast<UNSIGNED_T>(delta));
	}
	static inline bool TryGetDelta(T value, T min_value, uint64_t &delta) {
		if (value < min_value) {
			return false;
		}
		using UNSIGNED_T = typename MakeUnsigned<T>::type;
		delta = static_cast<uint64_t>(static_cast<UNSIGNED_T>(value) - static_cast<UNSIGNED_T>(min_value));
		return true;
	}
	static inline uint64_t GetDeltaUnsafe(T value, T min_value) {
		using UNSIGNED_T = typename MakeUnsigned<T>::type;
		return static_cast<uint64_t>(static_cast<UNSIGNED_T>(value) - static_cast<UNSIGNED_T>(min_value));
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
	static inline hugeint_t AddDelta(hugeint_t min_value, uint64_t delta) {
		return min_value + hugeint_t(0, delta);
	}
	static inline bool TryGetDelta(hugeint_t value, hugeint_t min_value, uint64_t &delta) {
		if (value < min_value) {
			return false;
		}
		auto udiff = static_cast<uhugeint_t>(value - min_value);
		if (udiff.upper != 0) {
			return false;
		}
		delta = udiff.lower;
		return true;
	}
	static inline uint64_t GetDeltaUnsafe(hugeint_t value, hugeint_t min_value) {
		auto udiff = static_cast<uhugeint_t>(value - min_value);
		return udiff.lower;
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
	static inline uhugeint_t AddDelta(uhugeint_t min_value, uint64_t delta) {
		return min_value + uhugeint_t(delta);
	}
	static inline bool TryGetDelta(uhugeint_t value, uhugeint_t min_value, uint64_t &delta) {
		if (value < min_value) {
			return false;
		}
		auto diff = value - min_value;
		if (diff.upper != 0) {
			return false;
		}
		delta = diff.lower;
		return true;
	}
	static inline uint64_t GetDeltaUnsafe(uhugeint_t value, uhugeint_t min_value) {
		auto diff = value - min_value;
		return diff.lower;
	}
};

} // namespace duckdb
