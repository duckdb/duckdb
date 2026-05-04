//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/checked_integer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include <atomic>
#include <functional>
#include <limits>
#include <type_traits>

namespace duckdb {

//! CheckedInteger is a templated wrapper around integer types.
//!
//! A few key features:
//! - On underflow and overflow, it throws a templated exception type (by default InternalException).
//! - It's by default initialized to 0, which avoids accessing uninitialized memory.
//! - It serves as drop-in replacement for the underlying type in most cases.
//!
//! Known and intentional incompatibilities:
//! - For all the operations, only integral template arguments are accepted.
//! - It doesn't support bit operations.
template <typename T, typename ExceptionT = InternalException>
class CheckedInteger {
	static_assert(std::is_integral_v<T>, "CheckedInteger only supports integral types");

private:
	T value;

public:
	using value_type = T;

	constexpr CheckedInteger() : value(0) {
	}
	constexpr CheckedInteger(T v) : value(v) { // NOLINT
	}

	template <typename U, std::enable_if_t<std::is_integral_v<U> && !std::is_same_v<U, T>, int> = 0>
	CheckedInteger(U v) : value(ValidateAndCast<U>(v)) { // NOLINT
	}

	constexpr operator T() const { // NOLINT
		return value;
	}

	constexpr T GetValue() const {
		return value;
	}

	CheckedInteger &operator++() {
		T result;
		if (!TryAddOperator::Operation(value, T(1), result)) {
			throw ExceptionT("Overflow in increment of CheckedInteger: %d + 1 exceeds maximum %d", FormatValue(value),
			                 FormatValue(NumericLimits<T>::Maximum()));
		}
		value = result;
		return *this;
	}

	CheckedInteger operator++(int) {
		CheckedInteger tmp(*this);
		++(*this);
		return tmp;
	}

	CheckedInteger &operator--() {
		T result;
		if (!TrySubtractOperator::Operation(value, T(1), result)) {
			throw ExceptionT("Underflow in decrement of CheckedInteger: %d - 1 below minimum %d", FormatValue(value),
			                 FormatValue(NumericLimits<T>::Minimum()));
		}
		value = result;
		return *this;
	}

	CheckedInteger operator--(int) {
		CheckedInteger tmp(*this);
		--(*this);
		return tmp;
	}

	CheckedInteger &operator+=(CheckedInteger rhs) {
		return operator+=(rhs.value);
	}
	CheckedInteger &operator+=(T rhs) {
		T result;
		if (!TryAddOperator::Operation(value, rhs, result)) {
			throw ExceptionT("Overflow in addition for CheckedInteger: %d + %d (valid range [%d, %d])",
			                 FormatValue(value), FormatValue(rhs), FormatValue(NumericLimits<T>::Minimum()),
			                 FormatValue(NumericLimits<T>::Maximum()));
		}
		value = result;
		return *this;
	}
	template <typename U, std::enable_if_t<std::is_integral_v<U> && !std::is_same_v<U, T>, int> = 0>
	CheckedInteger &operator+=(U rhs) {
		const hugeint_t wide = ToHuge(value) + ToHuge(rhs);
		value =
		    NarrowFromHuge(wide, value, rhs, "Overflow in addition for CheckedInteger: %d + %d (valid range [%d, %d])");
		return *this;
	}

	CheckedInteger &operator-=(CheckedInteger rhs) {
		return operator-=(rhs.value);
	}
	CheckedInteger &operator-=(T rhs) {
		T result;
		if (!TrySubtractOperator::Operation(value, rhs, result)) {
			throw ExceptionT("Underflow in subtraction for CheckedInteger: %d - %d (valid range [%d, %d])",
			                 FormatValue(value), FormatValue(rhs), FormatValue(NumericLimits<T>::Minimum()),
			                 FormatValue(NumericLimits<T>::Maximum()));
		}
		value = result;
		return *this;
	}
	template <typename U, std::enable_if_t<std::is_integral_v<U> && !std::is_same_v<U, T>, int> = 0>
	CheckedInteger &operator-=(U rhs) {
		const hugeint_t wide = ToHuge(value) - ToHuge(rhs);
		value = NarrowFromHuge(wide, value, rhs,
		                       "Underflow in subtraction for CheckedInteger: %d - %d (valid range [%d, %d])");
		return *this;
	}

	CheckedInteger &operator*=(CheckedInteger rhs) {
		return operator*=(rhs.value);
	}
	CheckedInteger &operator*=(T rhs) {
		T result;
		if (!TryMultiplyOperator::Operation(value, rhs, result)) {
			throw ExceptionT("Overflow in multiplication for CheckedInteger: %d * %d (valid range [%d, %d])",
			                 FormatValue(value), FormatValue(rhs), FormatValue(NumericLimits<T>::Minimum()),
			                 FormatValue(NumericLimits<T>::Maximum()));
		}
		value = result;
		return *this;
	}
	template <typename U, std::enable_if_t<std::is_integral_v<U> && !std::is_same_v<U, T>, int> = 0>
	CheckedInteger &operator*=(U rhs) {
		const hugeint_t wide = ToHuge(value) * ToHuge(rhs);
		value = NarrowFromHuge(wide, value, rhs,
		                       "Overflow in multiplication for CheckedInteger: %d * %d (valid range [%d, %d])");
		return *this;
	}

	CheckedInteger &operator/=(CheckedInteger rhs) {
		return operator/=(rhs.value);
	}
	CheckedInteger &operator/=(T rhs) {
		if (rhs == 0) {
			throw ExceptionT("Division by zero in CheckedInteger: %d / 0", FormatValue(value));
		}
		if (NumericLimits<T>::IsSigned() && value == NumericLimits<T>::Minimum() && rhs == T(-1)) {
			throw ExceptionT("Overflow in division for CheckedInteger: %d / %d", FormatValue(value), FormatValue(rhs));
		}
		value /= rhs;
		return *this;
	}
	template <typename U, std::enable_if_t<std::is_integral_v<U> && !std::is_same_v<U, T>, int> = 0>
	CheckedInteger &operator/=(U rhs) {
		if (rhs == 0) {
			throw ExceptionT("Division by zero in CheckedInteger: %d / 0", FormatValue(value));
		}
		const hugeint_t wide = ToHuge(value) / ToHuge(rhs);
		value =
		    NarrowFromHuge(wide, value, rhs, "Overflow in division for CheckedInteger: %d / %d (valid range [%d, %d])");
		return *this;
	}

	CheckedInteger operator+(CheckedInteger rhs) const {
		return operator+(rhs.value);
	}
	CheckedInteger operator+(T rhs) const {
		T result;
		if (!TryAddOperator::Operation(value, rhs, result)) {
			throw ExceptionT("Overflow in addition for CheckedInteger: %d + %d (valid range [%d, %d])",
			                 FormatValue(value), FormatValue(rhs), FormatValue(NumericLimits<T>::Minimum()),
			                 FormatValue(NumericLimits<T>::Maximum()));
		}
		return CheckedInteger(result);
	}
	template <typename U, std::enable_if_t<std::is_integral_v<U> && !std::is_same_v<U, T>, int> = 0>
	CheckedInteger operator+(U rhs) const {
		const hugeint_t wide = ToHuge(value) + ToHuge(rhs);
		return CheckedInteger(NarrowFromHuge(
		    wide, value, rhs, "Overflow in addition for CheckedInteger: %d + %d (valid range [%d, %d])"));
	}

	CheckedInteger operator-(CheckedInteger rhs) const {
		return operator-(rhs.value);
	}
	CheckedInteger operator-(T rhs) const {
		T result;
		if (!TrySubtractOperator::Operation(value, rhs, result)) {
			throw ExceptionT("Underflow in subtraction for CheckedInteger: %d - %d (valid range [%d, %d])",
			                 FormatValue(value), FormatValue(rhs), FormatValue(NumericLimits<T>::Minimum()),
			                 FormatValue(NumericLimits<T>::Maximum()));
		}
		return CheckedInteger(result);
	}
	template <typename U, std::enable_if_t<std::is_integral_v<U> && !std::is_same_v<U, T>, int> = 0>
	CheckedInteger operator-(U rhs) const {
		const hugeint_t wide = ToHuge(value) - ToHuge(rhs);
		return CheckedInteger(NarrowFromHuge(
		    wide, value, rhs, "Underflow in subtraction for CheckedInteger: %d - %d (valid range [%d, %d])"));
	}

	CheckedInteger operator*(CheckedInteger rhs) const {
		return operator*(rhs.value);
	}
	CheckedInteger operator*(T rhs) const {
		T result;
		if (!TryMultiplyOperator::Operation(value, rhs, result)) {
			throw ExceptionT("Overflow in multiplication for CheckedInteger: %d * %d (valid range [%d, %d])",
			                 FormatValue(value), FormatValue(rhs), FormatValue(NumericLimits<T>::Minimum()),
			                 FormatValue(NumericLimits<T>::Maximum()));
		}
		return CheckedInteger(result);
	}
	template <typename U, std::enable_if_t<std::is_integral_v<U> && !std::is_same_v<U, T>, int> = 0>
	CheckedInteger operator*(U rhs) const {
		const hugeint_t wide = ToHuge(value) * ToHuge(rhs);
		return CheckedInteger(NarrowFromHuge(
		    wide, value, rhs, "Overflow in multiplication for CheckedInteger: %d * %d (valid range [%d, %d])"));
	}

	CheckedInteger operator/(CheckedInteger rhs) const {
		return operator/(rhs.value);
	}
	CheckedInteger operator/(T rhs) const {
		if (rhs == 0) {
			throw ExceptionT("Division by zero in CheckedInteger: %d / 0", FormatValue(value));
		}
		if (NumericLimits<T>::IsSigned() && value == NumericLimits<T>::Minimum() && rhs == T(-1)) {
			throw ExceptionT("Overflow in division for CheckedInteger: %d / %d", FormatValue(value), FormatValue(rhs));
		}
		return CheckedInteger(value / rhs);
	}
	template <typename U, std::enable_if_t<std::is_integral_v<U> && !std::is_same_v<U, T>, int> = 0>
	CheckedInteger operator/(U rhs) const {
		if (rhs == 0) {
			throw ExceptionT("Division by zero in CheckedInteger: %d / 0", FormatValue(value));
		}
		const hugeint_t wide = ToHuge(value) / ToHuge(rhs);
		return CheckedInteger(NarrowFromHuge(
		    wide, value, rhs, "Overflow in division for CheckedInteger: %d / %d (valid range [%d, %d])"));
	}

	bool operator==(const CheckedInteger &other) const {
		return value == other.value;
	}
	bool operator!=(const CheckedInteger &other) const {
		return value != other.value;
	}
	bool operator<(const CheckedInteger &other) const {
		return value < other.value;
	}
	bool operator>(const CheckedInteger &other) const {
		return value > other.value;
	}
	bool operator<=(const CheckedInteger &other) const {
		return value <= other.value;
	}
	bool operator>=(const CheckedInteger &other) const {
		return value >= other.value;
	}

	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	bool operator==(U other) const {
		return CmpEqual(value, other);
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	bool operator!=(U other) const {
		return !CmpEqual(value, other);
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	bool operator<(U other) const {
		return CmpLess(value, other);
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	bool operator>(U other) const {
		return CmpLess(other, value);
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	bool operator<=(U other) const {
		return !CmpLess(other, value);
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	bool operator>=(U other) const {
		return !CmpLess(value, other);
	}

	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	friend bool operator==(U lhs, const CheckedInteger &rhs) {
		return CmpEqual(rhs.value, lhs);
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	friend bool operator!=(U lhs, const CheckedInteger &rhs) {
		return !CmpEqual(rhs.value, lhs);
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	friend bool operator<(U lhs, const CheckedInteger &rhs) {
		return CmpLess(lhs, rhs.value);
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	friend bool operator>(U lhs, const CheckedInteger &rhs) {
		return CmpLess(rhs.value, lhs);
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	friend bool operator<=(U lhs, const CheckedInteger &rhs) {
		return !CmpLess(rhs.value, lhs);
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	friend bool operator>=(U lhs, const CheckedInteger &rhs) {
		return !CmpLess(lhs, rhs.value);
	}

private:
	template <typename A, typename B>
	static bool CmpEqual(A a, B b) {
		static_assert(std::is_integral_v<A> && std::is_integral_v<B>, "CheckedInteger only supports integral types");
		if constexpr (std::is_signed_v<A> == std::is_signed_v<B>) {
			return a == b;
		} else if constexpr (std::is_signed_v<A>) {
			return a >= 0 && static_cast<typename std::make_unsigned<A>::type>(a) == b;
		} else {
			return b >= 0 && a == static_cast<typename std::make_unsigned<B>::type>(b);
		}
	}

	template <typename A, typename B>
	static bool CmpLess(A a, B b) {
		static_assert(std::is_integral_v<A> && std::is_integral_v<B>, "CheckedInteger only supports integral types");
		if constexpr (std::is_signed_v<A> == std::is_signed_v<B>) {
			return a < b;
		} else if constexpr (std::is_signed_v<A>) {
			return a < 0 || static_cast<typename std::make_unsigned<A>::type>(a) < b;
		} else {
			return b >= 0 && a < static_cast<typename std::make_unsigned<B>::type>(b);
		}
	}

	// Validate `v` could be assigned to `T` with no overflow or underflow.
	template <typename U>
	static T ValidateAndCast(U v) {
		static_assert(std::is_integral_v<U>, "CheckedInteger only supports integral types");
		if constexpr (std::is_signed_v<U> && std::is_unsigned_v<T>) {
			if (v < U(0)) {
				throw ExceptionT("Value %d out of range for CheckedInteger (valid range [%d, %d])", FormatValue(v),
				                 FormatValue(NumericLimits<T>::Minimum()), FormatValue(NumericLimits<T>::Maximum()));
			}
		}
		T result = static_cast<T>(v);
		if constexpr (std::is_unsigned_v<U> && std::is_signed_v<T>) {
			if (result < T(0)) {
				throw ExceptionT("Value %d out of range for CheckedInteger (valid range [%d, %d])", FormatValue(v),
				                 FormatValue(NumericLimits<T>::Minimum()), FormatValue(NumericLimits<T>::Maximum()));
			}
		}
		if (static_cast<U>(result) != v) {
			throw ExceptionT("Value %d out of range for CheckedInteger (valid range [%d, %d])", FormatValue(v),
			                 FormatValue(NumericLimits<T>::Minimum()), FormatValue(NumericLimits<T>::Maximum()));
		}
		return result;
	}

	template <typename V>
	static auto FormatValue(V v) {
		static_assert(std::is_integral_v<V>, "CheckedInteger only supports integral types");
		if constexpr (std::is_unsigned_v<V>) {
			return static_cast<idx_t>(v);
		} else {
			return static_cast<int64_t>(v);
		}
	}

	template <typename V>
	static hugeint_t ToHuge(V v) {
		static_assert(std::is_integral_v<V>, "CheckedInteger only supports integral types");
		if constexpr (std::is_unsigned_v<V>) {
			return uhugeint_t(static_cast<uint64_t>(v));
		} else {
			return hugeint_t(static_cast<int64_t>(v));
		}
	}

	//! Narrow a 128-bit wide arithmetic result back to T, throwing ExceptionT if underflow/overflow.
	template <typename V>
	static T NarrowFromHuge(const hugeint_t wide, T lhs, V rhs, const char *op_msg) {
		if (wide < ToHuge(NumericLimits<T>::Minimum()) || wide > ToHuge(NumericLimits<T>::Maximum())) {
			throw ExceptionT(op_msg, FormatValue(lhs), FormatValue(rhs), FormatValue(NumericLimits<T>::Minimum()),
			                 FormatValue(NumericLimits<T>::Maximum()));
		}
		return static_cast<T>(wide);
	}
};

template <typename TL, typename TR, typename E, std::enable_if_t<std::is_integral_v<TL>, int> = 0>
CheckedInteger<TR, E> operator+(TL lhs, const CheckedInteger<TR, E> &rhs) {
	return CheckedInteger<TR, E>(lhs) + rhs.GetValue();
}

template <typename TL, typename TR, typename E, std::enable_if_t<std::is_integral_v<TL>, int> = 0>
CheckedInteger<TR, E> operator-(TL lhs, const CheckedInteger<TR, E> &rhs) {
	return CheckedInteger<TR, E>(lhs) - rhs.GetValue();
}

template <typename TL, typename TR, typename E, std::enable_if_t<std::is_integral_v<TL>, int> = 0>
CheckedInteger<TR, E> operator*(TL lhs, const CheckedInteger<TR, E> &rhs) {
	return CheckedInteger<TR, E>(lhs) * rhs.GetValue();
}

template <typename TL, typename TR, typename E, std::enable_if_t<std::is_integral_v<TL>, int> = 0>
CheckedInteger<TR, E> operator/(TL lhs, const CheckedInteger<TR, E> &rhs) {
	return CheckedInteger<TR, E>(lhs) / rhs.GetValue();
}

} // namespace duckdb

namespace std { // NOLINT

template <typename T, typename ExceptionT>
struct atomic<duckdb::CheckedInteger<T, ExceptionT>> {
	static_assert(std::is_integral_v<T>, "CheckedInteger only supports integral types");

private:
	std::atomic<T> val;

public:
	using value_type = duckdb::CheckedInteger<T, ExceptionT>;

	atomic() noexcept : val(T(0)) {
	}
	constexpr atomic(value_type desired) noexcept : val(desired.GetValue()) { // NOLINT
	}
	constexpr atomic(T desired) noexcept : val(desired) { // NOLINT
	}
	template <typename U, std::enable_if_t<std::is_integral_v<U> && !std::is_same_v<U, T>, int> = 0>
	atomic(U desired) : val(value_type(desired).GetValue()) { // NOLINT
	}

	atomic(const atomic &) = delete;
	atomic &operator=(const atomic &) = delete;
	atomic &operator=(const atomic &) volatile = delete;

	value_type load(std::memory_order order = std::memory_order_seq_cst) const noexcept {
		return value_type(val.load(order));
	}

	void store(value_type desired, std::memory_order order = std::memory_order_seq_cst) noexcept {
		val.store(desired.GetValue(), order);
	}

	operator value_type() const noexcept { // NOLINT
		return load();
	}

	value_type operator=(value_type desired) noexcept { // NOLINT
		store(desired);
		return desired;
	}

	template <typename U, std::enable_if_t<std::is_integral_v<U> && !std::is_same_v<U, value_type>, int> = 0>
	value_type operator=(U desired) {
		value_type checked(desired);
		store(checked);
		return checked;
	}

	value_type fetch_add(value_type arg, std::memory_order order = std::memory_order_seq_cst) {
		T current = val.load(std::memory_order_relaxed);
		T next;
		do {
			next = (value_type(current) + arg).GetValue();
		} while (!val.compare_exchange_weak(current, next, order, std::memory_order_relaxed));
		return value_type(current);
	}

	value_type fetch_sub(value_type arg, std::memory_order order = std::memory_order_seq_cst) {
		T current = val.load(std::memory_order_relaxed);
		T next;
		do {
			next = (value_type(current) - arg).GetValue();
		} while (!val.compare_exchange_weak(current, next, order, std::memory_order_relaxed));
		return value_type(current);
	}

	template <typename U>
	value_type operator+=(U arg) {
		static_assert(std::is_integral_v<U>, "CheckedInteger only supports integral types");
		value_type checked_arg(arg);
		value_type old = fetch_add(checked_arg);
		return old + checked_arg;
	}

	template <typename U>
	value_type operator-=(U arg) {
		static_assert(std::is_integral_v<U>, "CheckedInteger only supports integral types");
		value_type checked_arg(arg);
		value_type old = fetch_sub(checked_arg);
		return old - checked_arg;
	}

	bool operator==(value_type other) const noexcept {
		return load() == other;
	}
	bool operator!=(value_type other) const noexcept {
		return load() != other;
	}
	bool operator<(value_type other) const noexcept {
		return load() < other;
	}
	bool operator>(value_type other) const noexcept {
		return load() > other;
	}
	bool operator<=(value_type other) const noexcept {
		return load() <= other;
	}
	bool operator>=(value_type other) const noexcept {
		return load() >= other;
	}

	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	bool operator==(U other) const noexcept {
		return load() == other;
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	bool operator!=(U other) const noexcept {
		return load() != other;
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	bool operator<(U other) const noexcept {
		return load() < other;
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	bool operator>(U other) const noexcept {
		return load() > other;
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	bool operator<=(U other) const noexcept {
		return load() <= other;
	}
	template <typename U, typename = std::enable_if_t<std::is_integral_v<U>>>
	bool operator>=(U other) const noexcept {
		return load() >= other;
	}
};

template <typename T, typename ExceptionT>
struct numeric_limits<duckdb::CheckedInteger<T, ExceptionT>> : public numeric_limits<T> {
	using value_type = duckdb::CheckedInteger<T, ExceptionT>;

	static constexpr value_type min() noexcept { // NOLINT: mimic std casing
		return value_type(numeric_limits<T>::min());
	}
	static constexpr value_type max() noexcept { // NOLINT: mimic std casing
		return value_type(numeric_limits<T>::max());
	}
	static constexpr value_type lowest() noexcept {
		return value_type(numeric_limits<T>::lowest());
	}
	static constexpr value_type epsilon() noexcept {
		return value_type(numeric_limits<T>::epsilon());
	}
	static constexpr value_type round_error() noexcept { // NOLINT: mimic std casing
		return value_type(numeric_limits<T>::round_error());
	}
	static constexpr value_type infinity() noexcept {
		return value_type(numeric_limits<T>::infinity());
	}
	static constexpr value_type quiet_NaN() noexcept { // NOLINT: mimic std casing
		return value_type(numeric_limits<T>::quiet_NaN());
	}
	static constexpr value_type signaling_NaN() noexcept { // NOLINT: mimic std casing
		return value_type(numeric_limits<T>::signaling_NaN());
	}
	static constexpr value_type denorm_min() noexcept { // NOLINT: mimic std casing
		return value_type(numeric_limits<T>::denorm_min());
	}
};

template <typename T, typename ExceptionT>
struct hash<duckdb::CheckedInteger<T, ExceptionT>> {
	size_t operator()(const duckdb::CheckedInteger<T, ExceptionT> &v) const noexcept {
		return hash<T> {}(v.GetValue());
	}
};

} // namespace std
