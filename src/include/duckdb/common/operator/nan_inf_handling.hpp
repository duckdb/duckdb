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
		if (op == ScalarOperator::ADD) {
			return left + right;
		} else {
			return left - right;
		}
	}

	static inline float HandleAddSub(float left, float right, ScalarOperator op) {
		if (op == ScalarOperator::ADD) {
			return left + right;
		} else {
			return left - right;
		}
	}

	static inline double HandleMult(double left, double right) {
		return left * right;
	}

	static inline double HandleMult(float left, float right) {
		return left * right;
	}

	static inline double HandleDiv(double left, double right) {
		return left / right;
	}

	static inline double HandleDiv(float left, float right) {
		return left / right;
	}

	static inline double HandleMod(double left, double right) {
		return std::fmod(left, right);
	}

	static inline double HandleMod(float left, float right) {
		return std::fmod(left, right);
	}

	template <class TA>
	static inline TA HandleNegate(TA left) {
		return -left;
	}
};

template <>
double NanInfHandler::HandleNegate(double val) {
	return -val;
}

template <>
float NanInfHandler::HandleNegate(float val) {
	return -val;
}

} // namespace duckdb
