//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/double_na_equal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include <cmath>

namespace duckdb {

// special double/float class to deal with dictionary encoding: NaNs are equal, -0.0 and 0.0 are not
struct double_na_equal {
	double_na_equal() : val(0) {
	}
	explicit double_na_equal(const double val_p) : val(val_p) {
	}
	// NOLINTNEXTLINE: allow implicit conversion to double
	operator double() const {
		return val;
	}

	bool operator==(const double &right) const {
		if (std::isnan(val) && std::isnan(right)) {
			return true;
		}
		// a dictionary entry stands for the bytes written, so -0.0 and 0.0 are different values
		return val == right && std::signbit(val) == std::signbit(right);
	}

	bool operator!=(const double &right) const {
		return !(*this == right);
	}

	double val;
};

struct float_na_equal {
	float_na_equal() : val(0) {
	}
	explicit float_na_equal(const float val_p) : val(val_p) {
	}
	// NOLINTNEXTLINE: allow implicit conversion to float
	operator float() const {
		return val;
	}

	bool operator==(const float &right) const {
		if (std::isnan(val) && std::isnan(right)) {
			return true;
		}
		// a dictionary entry stands for the bytes written, so -0.0 and 0.0 are different values
		return val == right && std::signbit(val) == std::signbit(right);
	}

	bool operator!=(const float &right) const {
		return !(*this == right);
	}

	float val;
};

} // namespace duckdb
