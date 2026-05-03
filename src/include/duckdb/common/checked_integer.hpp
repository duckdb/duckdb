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
#include <type_traits>

namespace duckdb {

//! CheckedInteger is a templated wrapper around integer types.
//! 
//! A few key features:
//! - On underflow and overflow, it throws a templated exception type (by default InternalException).
//! - It's by default initialized to 0, which avoid accessing uninitialized memory.
//! - It served as drop-in replacement for the underlying type in most cases.
//!
//! For all the operations, only integral template arguments are accepted.
template <typename T, typename ExceptionT = InternalException>
class CheckedInteger {
	static_assert(std::is_integral_v<T>, "CheckedInteger only supports integral types");

private:
	T value;

public:
	using value_type = T;

	CheckedInteger() : value(0) {
	}
	CheckedInteger(T v) : value(v) {
	} // NOLINT

	template <typename U,
	          typename std::enable_if<std::is_integral_v<U> && !std::is_same_v<U, T>, int>::type = 0>
	CheckedInteger(U v) : value(ValidateAndCast<U>(v)) { // NOLINT
	}

	explicit operator T() const {
		return value;
	}

	T GetValue() const {
		return value;
	}

	CheckedInteger &operator++() {
		T result;
		if (!TryAddOperator::Operation(value, T(1), result)) {
			throw ExceptionT("Overflow in increment of CheckedInteger");
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
			throw ExceptionT("Underflow in decrement of CheckedInteger");
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
			throw ExceptionT("Overflow in addition for CheckedInteger");
		}
		value = result;
		return *this;
	}
	template <typename U,
	          typename std::enable_if<std::is_integral_v<U> && !std::is_same_v<U, T>, int>::type = 0>
	CheckedInteger &operator+=(U rhs) {
		static_assert(std::is_integral_v<U>, "CheckedInteger only supports integral types");
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_same_v<Promoted, T>) {
			if constexpr (std::is_unsigned_v<T> && std::is_signed_v<U>) {
				if (rhs < 0) {
					return operator-=(ToAbsoluteT(rhs));
				}
			}
			return operator+=(static_cast<T>(rhs));
		} else {
			Promoted result = static_cast<Promoted>(value) + static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw ExceptionT("Overflow in addition for CheckedInteger");
			}
			value = static_cast<T>(result);
			return *this;
		}
	}

	CheckedInteger &operator-=(CheckedInteger rhs) {
		return operator-=(rhs.value);
	}
	CheckedInteger &operator-=(T rhs) {
		T result;
		if (!TrySubtractOperator::Operation(value, rhs, result)) {
			throw ExceptionT("Underflow in subtraction for CheckedInteger");
		}
		value = result;
		return *this;
	}
	template <typename U,
	          typename std::enable_if<std::is_integral_v<U> && !std::is_same_v<U, T>, int>::type = 0>
	CheckedInteger &operator-=(U rhs) {
		static_assert(std::is_integral_v<U>, "CheckedInteger only supports integral types");
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_same_v<Promoted, T>) {
			if constexpr (std::is_unsigned_v<T> && std::is_signed_v<U>) {
				if (rhs < 0) {
					return operator+=(ToAbsoluteT(rhs));
				}
			}
			return operator-=(static_cast<T>(rhs));
		} else {
			Promoted result = static_cast<Promoted>(value) - static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw ExceptionT("Underflow in subtraction for CheckedInteger");
			}
			value = static_cast<T>(result);
			return *this;
		}
	}

	CheckedInteger &operator*=(CheckedInteger rhs) {
		return operator*=(rhs.value);
	}
	CheckedInteger &operator*=(T rhs) {
		T result;
		if (!TryMultiplyOperator::Operation(value, rhs, result)) {
			throw ExceptionT("Overflow in multiplication for CheckedInteger");
		}
		value = result;
		return *this;
	}
	template <typename U,
	          typename std::enable_if<std::is_integral_v<U> && !std::is_same_v<U, T>, int>::type = 0>
	CheckedInteger &operator*=(U rhs) {
		static_assert(std::is_integral_v<U>, "CheckedInteger only supports integral types");
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_same_v<Promoted, T>) {
			if constexpr (std::is_unsigned_v<T> && std::is_signed_v<U>) {
				if (rhs < 0) {
					throw ExceptionT("Cannot multiply unsigned CheckedInteger by negative value");
				}
			}
			return operator*=(static_cast<T>(rhs));
		} else {
			Promoted result = static_cast<Promoted>(value) * static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw ExceptionT("Overflow in multiplication for CheckedInteger");
			}
			value = static_cast<T>(result);
			return *this;
		}
	}

	CheckedInteger &operator/=(CheckedInteger rhs) {
		return operator/=(rhs.value);
	}
	CheckedInteger &operator/=(T rhs) {
		if (rhs == 0) {
			throw ExceptionT("Division by zero in CheckedInteger");
		}
		if (NumericLimits<T>::IsSigned() && value == NumericLimits<T>::Minimum() && rhs == T(-1)) {
			throw ExceptionT("Overflow in division for CheckedInteger");
		}
		value /= rhs;
		return *this;
	}
	template <typename U,
	          typename std::enable_if<std::is_integral_v<U> && !std::is_same_v<U, T>, int>::type = 0>
	CheckedInteger &operator/=(U rhs) {
		static_assert(std::is_integral_v<U>, "CheckedInteger only supports integral types");
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_same_v<Promoted, T>) {
			if constexpr (std::is_unsigned_v<T> && std::is_signed_v<U>) {
				if (rhs < 0) {
					throw ExceptionT("Cannot divide unsigned CheckedInteger by negative value");
				}
			}
			return operator/=(static_cast<T>(rhs));
		} else {
			if (rhs == 0) {
				throw ExceptionT("Division by zero in CheckedInteger");
			}
			Promoted result = static_cast<Promoted>(value) / static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw ExceptionT("Overflow in division for CheckedInteger");
			}
			value = static_cast<T>(result);
			return *this;
		}
	}

	CheckedInteger operator+(CheckedInteger rhs) const {
		return operator+(rhs.value);
	}
	CheckedInteger operator+(T rhs) const {
		T result;
		if (!TryAddOperator::Operation(value, rhs, result)) {
			throw ExceptionT("Overflow in addition for CheckedInteger");
		}
		return CheckedInteger(result);
	}
	template <typename U,
	          typename std::enable_if<std::is_integral_v<U> && !std::is_same_v<U, T>, int>::type = 0>
	CheckedInteger operator+(U rhs) const {
		static_assert(std::is_integral_v<U>, "CheckedInteger only supports integral types");
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_same_v<Promoted, T>) {
			if constexpr (std::is_unsigned_v<T> && std::is_signed_v<U>) {
				if (rhs < 0) {
					return operator-(ToAbsoluteT(rhs));
				}
			}
			return operator+(static_cast<T>(rhs));
		} else {
			Promoted result = static_cast<Promoted>(value) + static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw ExceptionT("Overflow in addition for CheckedInteger");
			}
			return CheckedInteger(static_cast<T>(result));
		}
	}

	CheckedInteger operator-(CheckedInteger rhs) const {
		return operator-(rhs.value);
	}
	CheckedInteger operator-(T rhs) const {
		T result;
		if (!TrySubtractOperator::Operation(value, rhs, result)) {
			throw ExceptionT("Underflow in subtraction for CheckedInteger");
		}
		return CheckedInteger(result);
	}
	template <typename U,
	          typename std::enable_if<std::is_integral_v<U> && !std::is_same_v<U, T>, int>::type = 0>
	CheckedInteger operator-(U rhs) const {
		static_assert(std::is_integral_v<U>, "CheckedInteger only supports integral types");
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_same_v<Promoted, T>) {
			if constexpr (std::is_unsigned_v<T> && std::is_signed_v<U>) {
				if (rhs < 0) {
					return operator+(ToAbsoluteT(rhs));
				}
			}
			return operator-(static_cast<T>(rhs));
		} else {
			Promoted result = static_cast<Promoted>(value) - static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw ExceptionT("Underflow in subtraction for CheckedInteger");
			}
			return CheckedInteger(static_cast<T>(result));
		}
	}

	CheckedInteger operator*(CheckedInteger rhs) const {
		return operator*(rhs.value);
	}
	CheckedInteger operator*(T rhs) const {
		T result;
		if (!TryMultiplyOperator::Operation(value, rhs, result)) {
			throw ExceptionT("Overflow in multiplication for CheckedInteger");
		}
		return CheckedInteger(result);
	}
	template <typename U,
	          typename std::enable_if<std::is_integral_v<U> && !std::is_same_v<U, T>, int>::type = 0>
	CheckedInteger operator*(U rhs) const {
		static_assert(std::is_integral_v<U>, "CheckedInteger only supports integral types");
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_same_v<Promoted, T>) {
			if constexpr (std::is_unsigned_v<T> && std::is_signed_v<U>) {
				if (rhs < 0) {
					throw ExceptionT("Cannot multiply unsigned CheckedInteger by negative value");
				}
			}
			return operator*(static_cast<T>(rhs));
		} else {
			Promoted result = static_cast<Promoted>(value) * static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw ExceptionT("Overflow in multiplication for CheckedInteger");
			}
			return CheckedInteger(static_cast<T>(result));
		}
	}

	CheckedInteger operator/(CheckedInteger rhs) const {
		return operator/(rhs.value);
	}
	CheckedInteger operator/(T rhs) const {
		if (rhs == 0) {
			throw ExceptionT("Division by zero in CheckedInteger");
		}
		if (NumericLimits<T>::IsSigned() && value == NumericLimits<T>::Minimum() && rhs == T(-1)) {
			throw ExceptionT("Overflow in division for CheckedInteger");
		}
		return CheckedInteger(value / rhs);
	}
	template <typename U,
	          typename std::enable_if<std::is_integral_v<U> && !std::is_same_v<U, T>, int>::type = 0>
	CheckedInteger operator/(U rhs) const {
		static_assert(std::is_integral_v<U>, "CheckedInteger only supports integral types");
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_same_v<Promoted, T>) {
			if constexpr (std::is_unsigned_v<T> && std::is_signed_v<U>) {
				if (rhs < 0) {
					throw ExceptionT("Cannot divide unsigned CheckedInteger by negative value");
				}
			}
			return operator/(static_cast<T>(rhs));
		} else {
			if (rhs == 0) {
				throw ExceptionT("Division by zero in CheckedInteger");
			}
			Promoted result = static_cast<Promoted>(value) / static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw ExceptionT("Overflow in division for CheckedInteger");
			}
			return CheckedInteger(static_cast<T>(result));
		}
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

private:
	// Validate `v` could be assigned to `T` with no overflow or underflow.
	template <typename U>
	static T ValidateAndCast(U v) {
		static_assert(std::is_integral_v<U>, "CheckedInteger only supports integral types");
		if constexpr (std::is_signed_v<U> && std::is_unsigned_v<T>) {
			if (v < U(0)) {
				throw ExceptionT("Value out of range for CheckedInteger");
			}
		}
		T result = static_cast<T>(v);
		if constexpr (std::is_unsigned_v<U> && std::is_signed_v<T>) {
			if (result < T(0)) {
				throw ExceptionT("Value out of range for CheckedInteger");
			}
		}
		if (static_cast<U>(result) != v) {
			throw ExceptionT("Value out of range for CheckedInteger");
		}
		return result;
	}

	//! Compute |rhs| as type T without signed-overflow UB.
	//! Only valid when T is unsigned and sizeof(T) >= sizeof(U).
	template <typename U>
	static T ToAbsoluteT(U rhs) {
		static_assert(std::is_integral_v<U>, "CheckedInteger only supports integral types");
		using UnsignedU = typename std::make_unsigned<U>::type;
		return static_cast<T>(static_cast<UnsignedU>(-static_cast<UnsignedU>(rhs)));
	}
};

template <typename TL, typename TR, typename E, typename std::enable_if<std::is_integral_v<TL>, int>::type = 0>
CheckedInteger<TR, E> operator+(TL lhs, const CheckedInteger<TR, E> &rhs) {
	return CheckedInteger<TR, E>(lhs) + rhs.GetValue();
}

template <typename TL, typename TR, typename E, typename std::enable_if<std::is_integral_v<TL>, int>::type = 0>
CheckedInteger<TR, E> operator-(TL lhs, const CheckedInteger<TR, E> &rhs) {
	return CheckedInteger<TR, E>(lhs) - rhs.GetValue();
}

template <typename TL, typename TR, typename E, typename std::enable_if<std::is_integral_v<TL>, int>::type = 0>
CheckedInteger<TR, E> operator*(TL lhs, const CheckedInteger<TR, E> &rhs) {
	return CheckedInteger<TR, E>(lhs) * rhs.GetValue();
}

template <typename TL, typename TR, typename E, typename std::enable_if<std::is_integral_v<TL>, int>::type = 0>
CheckedInteger<TR, E> operator/(TL lhs, const CheckedInteger<TR, E> &rhs) {
	return CheckedInteger<TR, E>(lhs) / rhs.GetValue();
}

using i8_t = CheckedInteger<int8_t>;
using i16_t = CheckedInteger<int16_t>;
using i32_t = CheckedInteger<int32_t>;
using i64_t = CheckedInteger<int64_t>;
using u8_t = CheckedInteger<uint8_t>;
using u16_t = CheckedInteger<uint16_t>;
using u32_t = CheckedInteger<uint32_t>;
using u64_t = CheckedInteger<uint64_t>;

} // namespace duckdb

namespace std { // NOLINT

//! std::atomic specialization for duckdb::CheckedInteger<T, ExceptionT>.
//! Uses a CAS loop to provide fetch_add/fetch_sub with overflow/underflow detection via CheckedInteger arithmetic.
template <typename T, typename ExceptionT>
struct atomic<duckdb::CheckedInteger<T, ExceptionT>> {
	static_assert(std::is_integral_v<T>, "CheckedInteger only supports integral types");

private:
	std::atomic<T> val;

public:
	using value_type = duckdb::CheckedInteger<T, ExceptionT>;

	atomic() noexcept = default;
	constexpr atomic(value_type desired) noexcept : val(desired.GetValue()) { // NOLINT
	}
	constexpr atomic(T desired) noexcept : val(desired) { // NOLINT
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

	value_type operator=(value_type desired) noexcept { // NOLINT
		store(desired);
		return desired;
	}

	//! Templated assignment from any integral value. Constructs a CheckedInteger first (which performs
	//! sign / range validation) and then stores it. Disambiguates `atomic_x = 0` against the deleted
	//! copy-assignment when the literal is not already value_type.
	template <typename U,
	          typename std::enable_if<std::is_integral_v<U> && !std::is_same_v<U, value_type>, int>::type = 0>
	value_type operator=(U desired) {
		static_assert(std::is_integral_v<U>, "CheckedInteger only supports integral types");
		value_type checked(desired);
		store(checked);
		return checked;
	}

	//! Atomically adds arg to the stored value using a CAS loop; throws ExceptionT on overflow.
	//! Returns the value before the addition.
	value_type fetch_add(value_type arg, std::memory_order order = std::memory_order_seq_cst) {
		T current = val.load(std::memory_order_relaxed);
		T next;
		do {
			next = (value_type(current) + arg).GetValue();
		} while (!val.compare_exchange_weak(current, next, order, std::memory_order_relaxed));
		return value_type(current);
	}

	//! Atomically subtracts arg from the stored value using a CAS loop; throws ExceptionT on underflow.
	//! Returns the value before the subtraction.
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
};

} // namespace std
