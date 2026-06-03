//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/numeric_stats_traits.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/common/types/null_value.hpp"

#include <limits>

namespace duckdb {

struct FloatMinMaxKey {
public:
	using WORD = uint32_t;

	static inline FloatMinMaxKey FromBits(uint32_t bits) {
		return FloatMinMaxKey(Radix::EncodeFloatMinMaxKeyBits(bits));
	}

	static inline FloatMinMaxKey MinInitialValue() {
		return FloatMinMaxKey(std::numeric_limits<WORD>::max());
	}

	static inline FloatMinMaxKey MaxInitialValue() {
		return FloatMinMaxKey(std::numeric_limits<WORD>::min());
	}

	inline float Decode() const {
		return Radix::DecodeFloatMinMaxKey(key);
	}

	inline bool LessThan(const FloatMinMaxKey &other) const {
		return key < other.key;
	}

	inline bool GreaterThan(const FloatMinMaxKey &other) const {
		return key > other.key;
	}

private:
	explicit FloatMinMaxKey(WORD key_p) : key(key_p) {
	}

private:
	WORD key;
};

struct DoubleMinMaxKey {
public:
	using WORD = uint64_t;

	static inline DoubleMinMaxKey FromBits(uint64_t bits) {
		return DoubleMinMaxKey(Radix::EncodeDoubleMinMaxKeyBits(bits));
	}

	static inline DoubleMinMaxKey MinInitialValue() {
		return DoubleMinMaxKey(std::numeric_limits<WORD>::max());
	}

	static inline DoubleMinMaxKey MaxInitialValue() {
		return DoubleMinMaxKey(std::numeric_limits<WORD>::min());
	}

	inline double Decode() const {
		return Radix::DecodeDoubleMinMaxKey(key);
	}

	inline bool LessThan(const DoubleMinMaxKey &other) const {
		return key < other.key;
	}

	inline bool GreaterThan(const DoubleMinMaxKey &other) const {
		return key > other.key;
	}

private:
	explicit DoubleMinMaxKey(WORD key_p) : key(key_p) {
	}

private:
	WORD key;
};

//! Float/double load/store integer words so appends copy values without floating-point operations.
//! Stats reduction treats those words as raw floating-point bits and maps them to sortable min/max keys.
//! The min/max keys keep -0.0 and +0.0 distinct while reducing, canonicalise zeros on decode, and canonicalise NaNs.
//! This allows the zonemap stats loops to autovectorise well.
template <class T>
struct NumericStatsTraits {
	using INPUT = T;
	using KEY = T;

	static inline INPUT LoadInput(const T *source) {
		return *source;
	}

	static inline void StoreInput(T *target, INPUT input) {
		*target = input;
	}

	static inline INPUT NullInput() {
		return NullValue<T>();
	}

	static inline KEY EncodeInput(INPUT input) {
		return input;
	}

	static inline T Decode(KEY key) {
		return key;
	}

	static inline KEY MinInitialValue() {
		return NumericLimits<T>::Maximum();
	}

	static inline KEY MaxInitialValue() {
		return NumericLimits<T>::Minimum();
	}

	static inline bool LessThan(KEY left, KEY right) {
		return duckdb::LessThan::Operation(left, right);
	}

	static inline bool GreaterThan(KEY left, KEY right) {
		return duckdb::GreaterThan::Operation(left, right);
	}
};

template <>
struct NumericStatsTraits<float> {
	using INPUT = uint32_t;
	using KEY = FloatMinMaxKey;

	static inline INPUT LoadInput(const float *source) {
		return Load<INPUT>(const_data_ptr_cast(source));
	}

	static inline void StoreInput(float *target, INPUT input) {
		Store<INPUT>(input, data_ptr_cast(target));
	}

	static inline INPUT NullInput() {
		float null_value = NullValue<float>();
		return LoadInput(&null_value);
	}

	static inline KEY EncodeInput(INPUT input) {
		return KEY::FromBits(input);
	}

	static inline float Decode(KEY key) {
		return key.Decode();
	}

	static inline KEY MinInitialValue() {
		return KEY::MinInitialValue();
	}

	static inline KEY MaxInitialValue() {
		return KEY::MaxInitialValue();
	}

	static inline bool LessThan(const KEY &left, const KEY &right) {
		return left.LessThan(right);
	}

	static inline bool GreaterThan(const KEY &left, const KEY &right) {
		return left.GreaterThan(right);
	}
};

template <>
struct NumericStatsTraits<double> {
	using INPUT = uint64_t;
	using KEY = DoubleMinMaxKey;

	static inline INPUT LoadInput(const double *source) {
		return Load<INPUT>(const_data_ptr_cast(source));
	}

	static inline void StoreInput(double *target, INPUT input) {
		Store<INPUT>(input, data_ptr_cast(target));
	}

	static inline INPUT NullInput() {
		double null_value = NullValue<double>();
		return LoadInput(&null_value);
	}

	static inline KEY EncodeInput(INPUT input) {
		return KEY::FromBits(input);
	}

	static inline double Decode(KEY key) {
		return key.Decode();
	}

	static inline KEY MinInitialValue() {
		return KEY::MinInitialValue();
	}

	static inline KEY MaxInitialValue() {
		return KEY::MaxInitialValue();
	}

	static inline bool LessThan(const KEY &left, const KEY &right) {
		return left.LessThan(right);
	}

	static inline bool GreaterThan(const KEY &left, const KEY &right) {
		return left.GreaterThan(right);
	}
};

} // namespace duckdb
