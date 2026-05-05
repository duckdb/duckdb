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
	//! Distinct inputs map to distinct outputs (preserves cardinality).
	//! Strict monotonicity implies it; also true for non-monotonic mappings like reverse(s) or for
	//! invertible casts. Non-strict monotonicity does NOT imply it (truncation collapses values).
	bool injective = false;

	ArgProperties &StrictlyIncreasing() {
		monotonicity = Monotonicity::STRICTLY_INCREASING;
		injective = true;
		return *this;
	}
	ArgProperties &NonDecreasing() {
		monotonicity = Monotonicity::NON_DECREASING;
		return *this;
	}
	ArgProperties &StrictlyDecreasing() {
		monotonicity = Monotonicity::STRICTLY_DECREASING;
		injective = true;
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
	ArgProperties &Injective() {
		injective = true;
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
