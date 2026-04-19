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

//! CheckedInteger is a templated wrapper around integer types (signed or unsigned) that throws an InternalException on overflow or underflow for arithmetic operations.
template <class T>
class CheckedInteger {
	static_assert(std::is_integral<T>::value, "CheckedInteger only supports integral types");

private:
	T value;

public:
	using value_type = T;

	CheckedInteger() : value(0) {}
	CheckedInteger(T v) : value(v) {} // NOLINT

	template <class U, typename std::enable_if<std::is_arithmetic<U>::value, int>::type = 0>
	CheckedInteger(U v) : value(ValidateAndCast<U>(v)) {} // NOLINT

	explicit operator T() const {
		return value;
	}

	T GetValue() const {
		return value;
	}

	CheckedInteger &operator++() {
		T result;
		if (!TryAddOperator::Operation(value, T(1), result)) {
			throw InternalException("Overflow in increment of CheckedInteger");
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
			throw InternalException("Underflow in decrement of CheckedInteger");
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
			throw InternalException("Overflow in addition for CheckedInteger");
		}
		value = result;
		return *this;
	}
	template <class U, typename std::enable_if<std::is_arithmetic<U>::value, int>::type = 0>
	CheckedInteger &operator+=(U rhs) {
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_floating_point<U>::value) {
			ValidateAssignment(rhs);
			return operator+=(static_cast<T>(rhs));
		} else if constexpr (std::is_same<Promoted, T>::value) {
			if constexpr (std::is_unsigned<T>::value && std::is_signed<U>::value) {
				if (rhs < 0) {
					return operator-=(ToAbsoluteT(rhs));
				}
			}
			return operator+=(static_cast<T>(rhs));
		} else {
			Promoted result = static_cast<Promoted>(value) + static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw InternalException("Overflow in addition for CheckedInteger");
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
			throw InternalException("Underflow in subtraction for CheckedInteger");
		}
		value = result;
		return *this;
	}
	template <class U, typename std::enable_if<std::is_arithmetic<U>::value, int>::type = 0>
	CheckedInteger &operator-=(U rhs) {
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_floating_point<U>::value) {
			ValidateAssignment(rhs);
			return operator-=(static_cast<T>(rhs));
		} else if constexpr (std::is_same<Promoted, T>::value) {
			if constexpr (std::is_unsigned<T>::value && std::is_signed<U>::value) {
				if (rhs < 0) {
					return operator+=(ToAbsoluteT(rhs));
				}
			}
			return operator-=(static_cast<T>(rhs));
		} else {
			Promoted result = static_cast<Promoted>(value) - static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw InternalException("Underflow in subtraction for CheckedInteger");
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
			throw InternalException("Overflow in multiplication for CheckedInteger");
		}
		value = result;
		return *this;
	}
	template <class U, typename std::enable_if<std::is_arithmetic<U>::value, int>::type = 0>
	CheckedInteger &operator*=(U rhs) {
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_floating_point<U>::value) {
			ValidateAssignment(rhs);
			return operator*=(static_cast<T>(rhs));
		} else if constexpr (std::is_same<Promoted, T>::value) {
			if constexpr (std::is_unsigned<T>::value && std::is_signed<U>::value) {
				if (rhs < 0) {
					throw InternalException("Cannot multiply unsigned CheckedInteger by negative value");
				}
			}
			return operator*=(static_cast<T>(rhs));
		} else {
			Promoted result = static_cast<Promoted>(value) * static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw InternalException("Overflow in multiplication for CheckedInteger");
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
			throw InternalException("Division by zero in CheckedInteger");
		}
		if (NumericLimits<T>::IsSigned() && value == NumericLimits<T>::Minimum() && rhs == T(-1)) {
			throw InternalException("Overflow in division for CheckedInteger");
		}
		value /= rhs;
		return *this;
	}
	template <class U, typename std::enable_if<std::is_arithmetic<U>::value, int>::type = 0>
	CheckedInteger &operator/=(U rhs) {
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_floating_point<U>::value) {
			ValidateAssignment(rhs);
			return operator/=(static_cast<T>(rhs));
		} else if constexpr (std::is_same<Promoted, T>::value) {
			if constexpr (std::is_unsigned<T>::value && std::is_signed<U>::value) {
				if (rhs < 0) {
					throw InternalException("Cannot divide unsigned CheckedInteger by negative value");
				}
			}
			return operator/=(static_cast<T>(rhs));
		} else {
			if (rhs == 0) {
				throw InternalException("Division by zero in CheckedInteger");
			}
			Promoted result = static_cast<Promoted>(value) / static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw InternalException("Overflow in division for CheckedInteger");
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
			throw InternalException("Overflow in addition for CheckedInteger");
		}
		return CheckedInteger(result);
	}
	template <class U, typename std::enable_if<std::is_arithmetic<U>::value, int>::type = 0>
	CheckedInteger operator+(U rhs) const {
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_floating_point<U>::value) {
			ValidateAssignment(rhs);
			return operator+(static_cast<T>(rhs));
		} else if constexpr (std::is_same<Promoted, T>::value) {
			if constexpr (std::is_unsigned<T>::value && std::is_signed<U>::value) {
				if (rhs < 0) {
					return operator-(ToAbsoluteT(rhs));
				}
			}
			return operator+(static_cast<T>(rhs));
		} else {
			Promoted result = static_cast<Promoted>(value) + static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw InternalException("Overflow in addition for CheckedInteger");
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
			throw InternalException("Underflow in subtraction for CheckedInteger");
		}
		return CheckedInteger(result);
	}
	template <class U, typename std::enable_if<std::is_arithmetic<U>::value, int>::type = 0>
	CheckedInteger operator-(U rhs) const {
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_floating_point<U>::value) {
			ValidateAssignment(rhs);
			return operator-(static_cast<T>(rhs));
		} else if constexpr (std::is_same<Promoted, T>::value) {
			if constexpr (std::is_unsigned<T>::value && std::is_signed<U>::value) {
				if (rhs < 0) {
					return operator+(ToAbsoluteT(rhs));
				}
			}
			return operator-(static_cast<T>(rhs));
		} else {
			Promoted result = static_cast<Promoted>(value) - static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw InternalException("Underflow in subtraction for CheckedInteger");
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
			throw InternalException("Overflow in multiplication for CheckedInteger");
		}
		return CheckedInteger(result);
	}
	template <class U, typename std::enable_if<std::is_arithmetic<U>::value, int>::type = 0>
	CheckedInteger operator*(U rhs) const {
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_floating_point<U>::value) {
			ValidateAssignment(rhs);
			return operator*(static_cast<T>(rhs));
		} else if constexpr (std::is_same<Promoted, T>::value) {
			if constexpr (std::is_unsigned<T>::value && std::is_signed<U>::value) {
				if (rhs < 0) {
					throw InternalException("Cannot multiply unsigned CheckedInteger by negative value");
				}
			}
			return operator*(static_cast<T>(rhs));
		} else {
			Promoted result = static_cast<Promoted>(value) * static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw InternalException("Overflow in multiplication for CheckedInteger");
			}
			return CheckedInteger(static_cast<T>(result));
		}
	}

	CheckedInteger operator/(CheckedInteger rhs) const {
		return operator/(rhs.value);
	}
	CheckedInteger operator/(T rhs) const {
		if (rhs == 0) {
			throw InternalException("Division by zero in CheckedInteger");
		}
		if (NumericLimits<T>::IsSigned() && value == NumericLimits<T>::Minimum() && rhs == T(-1)) {
			throw InternalException("Overflow in division for CheckedInteger");
		}
		return CheckedInteger(value / rhs);
	}
	template <class U, typename std::enable_if<std::is_arithmetic<U>::value, int>::type = 0>
	CheckedInteger operator/(U rhs) const {
		using Promoted = typename std::common_type<T, U>::type;
		if constexpr (std::is_floating_point<U>::value) {
			ValidateAssignment(rhs);
			return operator/(static_cast<T>(rhs));
		} else if constexpr (std::is_same<Promoted, T>::value) {
			if constexpr (std::is_unsigned<T>::value && std::is_signed<U>::value) {
				if (rhs < 0) {
					throw InternalException("Cannot divide unsigned CheckedInteger by negative value");
				}
			}
			return operator/(static_cast<T>(rhs));
		} else {
			if (rhs == 0) {
				throw InternalException("Division by zero in CheckedInteger");
			}
			Promoted result = static_cast<Promoted>(value) / static_cast<Promoted>(rhs);
			if (static_cast<Promoted>(static_cast<T>(result)) != result) {
				throw InternalException("Overflow in division for CheckedInteger");
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
	// Validate negative values cannot be assigned to unsigned types
	template <class U>
	static T ValidateAndCast(U v) {
		if (std::is_unsigned<T>::value && std::is_signed<U>::value) {
			if (v < 0) {
				throw InternalException("Cannot assign negative value to unsigned CheckedInteger");
			}
		}
		return static_cast<T>(v);
	}

	template <class U>
	static void ValidateAssignment(U v) {
		if (std::is_unsigned<T>::value && std::is_signed<U>::value && v < 0) {
			throw InternalException("Cannot assign negative value to unsigned CheckedInteger");
		}
	}

	//! Compute |rhs| as type T without signed-overflow UB.
	//! Only valid when T is unsigned and sizeof(T) >= sizeof(U).
	template <class U>
	static T ToAbsoluteT(U rhs) {
		using UnsignedU = typename std::make_unsigned<U>::type;
		return static_cast<T>(static_cast<UnsignedU>(-static_cast<UnsignedU>(rhs)));
	}
};

template <class TL, class TR>
CheckedInteger<TR> operator+(TL lhs, const CheckedInteger<TR> &rhs) {
	return CheckedInteger<TR>(lhs) + rhs.GetValue();
}

template <class TL, class TR>
CheckedInteger<TR> operator-(TL lhs, const CheckedInteger<TR> &rhs) {
	return CheckedInteger<TR>(lhs) - rhs.GetValue();
}

template <class TL, class TR>
CheckedInteger<TR> operator*(TL lhs, const CheckedInteger<TR> &rhs) {
	return CheckedInteger<TR>(lhs) * rhs.GetValue();
}

template <class TL, class TR>
CheckedInteger<TR> operator/(TL lhs, const CheckedInteger<TR> &rhs) {
	return CheckedInteger<TR>(lhs) / rhs.GetValue();
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

//! std::atomic specialization for duckdb::CheckedInteger<T>.
//! Uses a CAS loop to provide fetch_add/fetch_sub with overflow/underflow detection via CheckedInteger arithmetic.
template <class T>
struct atomic<duckdb::CheckedInteger<T>> {
	static_assert(std::is_integral<T>::value, "CheckedInteger only supports integral types");

private:
	std::atomic<T> val;

public:
	using value_type = duckdb::CheckedInteger<T>;

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

	//! Atomically adds arg to the stored value using a CAS loop; throws InternalException on overflow.
	//! Returns the value before the addition.
	value_type fetch_add(value_type arg, std::memory_order order = std::memory_order_seq_cst) {
		T current = val.load(std::memory_order_relaxed);
		T next;
		do {
			next = (value_type(current) + arg).GetValue();
		} while (!val.compare_exchange_weak(current, next, order, std::memory_order_relaxed));
		return value_type(current);
	}

	//! Atomically subtracts arg from the stored value using a CAS loop; throws InternalException on underflow.
	//! Returns the value before the subtraction.
	value_type fetch_sub(value_type arg, std::memory_order order = std::memory_order_seq_cst) {
		T current = val.load(std::memory_order_relaxed);
		T next;
		do {
			next = (value_type(current) - arg).GetValue();
		} while (!val.compare_exchange_weak(current, next, order, std::memory_order_relaxed));
		return value_type(current);
	}

	template <class U>
	value_type operator+=(U arg) {
		value_type checked_arg(arg);
		value_type old = fetch_add(checked_arg);
		return old + checked_arg;
	}

	template <class U>
	value_type operator-=(U arg) {
		value_type checked_arg(arg);
		value_type old = fetch_sub(checked_arg);
		return old - checked_arg;
	}
};

} // namespace std
