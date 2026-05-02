//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/arg_properties.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

//! Monotonicity of a function in one argument (other args held constant).
enum class Monotonicity : uint8_t {
	UNKNOWN = 0,
	CONSTANT,
	NON_DECREASING,
	STRICTLY_INCREASING,
	NON_INCREASING,
	STRICTLY_DECREASING,
};

//! Per-argument metadata for a scalar function.
struct ArgProperties {
	Monotonicity monotonicity = Monotonicity::UNKNOWN;
	//! True if the function may return NULL on +/-inf or NaN inputs (e.g. year(infinity) -> NULL).
	//! The MIN/MAX peel skips wrappers with this flag unless the caller explicitly opts in via
	//! `allow_finite_only=true` (Tier-2), since the wrapper might evaluate to NULL on a stat
	//! boundary that isn't NULL on the underlying column.
	bool requires_finite_input = false;

	ArgProperties &StrictlyIncreasing() {
		monotonicity = Monotonicity::STRICTLY_INCREASING;
		return *this;
	}
	ArgProperties &NonDecreasing() {
		monotonicity = Monotonicity::NON_DECREASING;
		return *this;
	}
	ArgProperties &StrictlyDecreasing() {
		monotonicity = Monotonicity::STRICTLY_DECREASING;
		return *this;
	}
	ArgProperties &NonIncreasing() {
		monotonicity = Monotonicity::NON_INCREASING;
		return *this;
	}
	ArgProperties &Constant() {
		monotonicity = Monotonicity::CONSTANT;
		return *this;
	}
	ArgProperties &RequiresFinite() {
		requires_finite_input = true;
		return *this;
	}
};

constexpr bool IsMonotonicIncreasing(Monotonicity m) {
	return m == Monotonicity::NON_DECREASING || m == Monotonicity::STRICTLY_INCREASING;
}
constexpr bool IsMonotonicDecreasing(Monotonicity m) {
	return m == Monotonicity::NON_INCREASING || m == Monotonicity::STRICTLY_DECREASING;
}
constexpr bool IsKnownMonotonic(Monotonicity m) {
	return m != Monotonicity::UNKNOWN;
}
constexpr bool IsStrict(Monotonicity m) {
	return m == Monotonicity::STRICTLY_INCREASING || m == Monotonicity::STRICTLY_DECREASING;
}

} // namespace duckdb
