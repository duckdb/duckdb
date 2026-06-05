#pragma once

#include "duckdb/common/limits.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct NegateOperator {
	template <class T>
	static bool CanNegate(T input) {
		using Limits = NumericLimits<T>;
		return !(Limits::IsSigned() && Limits::Minimum() == input);
	}

	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (!CanNegate<TA>(input)) {
			throw OutOfRangeException("Overflow in negation of numeric value!");
		}
		return -(TR)input;
	}
};

// Specialization for floating point (always negatable)
template <>
inline bool NegateOperator::CanNegate(float input) {
	return true;
}
template <>
inline bool NegateOperator::CanNegate(double input) {
	return true;
}

} // namespace duckdb
