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

	//! Default is non-strict (NON_DECREASING / NON_INCREASING) — the conservative claim.
	//! Pass `true` only when distinct inputs are guaranteed to map to distinct outputs.
	ArgProperties &Increasing(bool strict = false) {
		monotonicity = strict ? Monotonicity::STRICTLY_INCREASING : Monotonicity::NON_DECREASING;
		return *this;
	}
	ArgProperties &Decreasing(bool strict = false) {
		monotonicity = strict ? Monotonicity::STRICTLY_DECREASING : Monotonicity::NON_INCREASING;
		return *this;
	}
	ArgProperties &Constant() {
		monotonicity = Monotonicity::CONSTANT;
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
