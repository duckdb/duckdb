//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/abs.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"

#include <cmath>

namespace duckdb {

enum ScalarOperator { ADD, SUBTRACT };

struct NanInfHandler {
	static inline double HandleAddSub(double left, double right, ScalarOperator op) {
		// if either is nan, return nan
		if (std::isnan(left) || std::isnan(right)) {
			return std::nan("");
		}
		// if both are inf, check the inf - inf case (or -inf + inf)
		if (std::isinf(left) && std::isinf(right)) {
			if (op == ScalarOperator::ADD) {
				return left + right;
			} else if (op == ScalarOperator::SUBTRACT) {
				return left - right;
			}
		}
		if (std::isinf(left)) {
			return left;
		}
		// return right otherwise, must be inf
		return right;
	}

	static inline float HandleAddSub(float left, float right, ScalarOperator op) {
		// if either is nan, return nan
		if (std::isnan(left) || std::isnan(right)) {
			return std::nan("");
		}
		// if both are inf, check the inf - inf case (or -inf + inf)
		if (std::isinf(left) && std::isinf(right)) {
			if (op == ScalarOperator::ADD) {
				return left + right;
			} else if (op == ScalarOperator::SUBTRACT) {
				return left - right;
			}
		}
		if (std::isinf(left)) {
			return left;
		}
		// return right otherwise, must be inf
		return right;
	}

	static inline double HandleMult(double left, double right) {
		// inf * 0 = nan in sqlite3 and pandas.
		if (std::isnan(left) || std::isnan(right) || left == 0 || right == 0) {
			return std::nan("");
		}
		if (std::isinf(left) && std::isinf(right)) {
			// TODO: check if one is -inf and the other +inf. return -inf if so
			return left * right;
		}
		if (std::isinf(left)) {
			return left;
		}
		// return right otherwise, must be inf
		return right;
	}

	static inline double HandleMult(float left, float right) {
		// inf * 0 = nan in sqlite3 and pandas.
		if (std::isnan(left) || std::isnan(right) || left == 0 || right == 0) {
			return std::nan("");
		}
		if (std::isinf(left) && std::isinf(right)) {
			// TODO: check if one is -inf and the other +inf. return -inf if so
			return left * right;
		}
		if (std::isinf(left)) {
			return left;
		}
		// return right otherwise, must be inf
		return right;
	}

	static inline double HandleDiv(double left, double right) {
		// np.nan / * = np.nan && * / np.nan = np.nan
		if (std::isnan(left) || std::isnan(right)) {
			return std::nan("");
		}
		// np.inf / np.inf = np.nan
		if (std::isinf(left) && std::isinf(right)) {
			return std::nan("");
		}
		// anything not np.inf or np.nan / np.inf = 0
		if (std::isinf(right)) {
			// TODO: check if one is -inf and the other +inf. return -inf if so
			return 0.0;
		}
		if (std::isinf(left)) {
			return left;
		}
		// return right otherwise, must be inf
		return left / right;
	}

	static inline double HandleDiv(float left, float right) {
		// np.nan / * = np.nan && * / np.nan = np.nan
		if (std::isnan(left) || std::isnan(right)) {
			return std::nan("");
		}
		// np.inf / np.inf = np.nan
		if (std::isinf(left) && std::isinf(right)) {
			return std::nan("");
		}
		// anything not np.inf or np.nan / np.inf = 0
		if (std::isinf(right)) {
			// TODO: check if one is -inf and the other +inf. return -inf if so
			return 0.0;
		}
		if (std::isinf(left)) {
			return left;
		}
		// return right otherwise, must be inf
		return left / right;
	}

	static inline double HandleMod(double left, double right) {
		// np.nan % * = np.nan && * % np.nan = np.nan
		// np.inf % * = np.nan as well
		if (std::isnan(left) || std::isnan(right) || std::isinf(left)) {
			return std::nan("");
		}
		// anything not np.inf or np.nan % np.inf = first term
		if (std::isinf(right)) {
			return left;
		}
		// should not get here.
		return std::fmod(left, right);
	}

	static inline double HandleMod(float left, float right) {
		// np.nan % * = np.nan && * % np.nan = np.nan
		// np.inf % * = np.nan as well
		if (std::isnan(left) || std::isnan(right) || std::isinf(left)) {
			return std::nan("");
		}
		// anything not np.inf or np.nan % np.inf = first term
		if (std::isinf(right)) {
			return left;
		}
		// should not get here.
		return std::fmod(left, right);
	}

	template <class TA>
	static inline TA HandleNegate(TA left) {
		return -left;
	}
};

template <>
double NanInfHandler::HandleNegate(double val) {
	if (std::isinf(val)) {
		return -val;
	}
	// val is Nan
	return val;
}

template <>
float NanInfHandler::HandleNegate(float val) {
	if (std::isinf(val)) {
		return -val;
	}
	// val is Nan
	return val;
}

} // namespace duckdb

// if (!(Value::FloatIsFinite(left) && Value::FloatIsFinite(right))) {
//	return NanInfHandler::HandleMod(left, right);
// }
