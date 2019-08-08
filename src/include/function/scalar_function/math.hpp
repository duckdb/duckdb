//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/math.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/scalar_function.hpp"

namespace duckdb {

static bool single_numeric_argument(vector<SQLType> &arguments) {
	if (arguments.size() != 1) {
		return false;
	}
	switch (arguments[0].id) {
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::DECIMAL:
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::SQLNULL:
		return true;
	default:
		return false;
	}
}

static bool no_arguments(vector<SQLType> &arguments) {
	return arguments.size() == 0;
}

static SQLType same_return_type(vector<SQLType> &arguments) {
	assert(arguments.size() == 1);
	return arguments[0];
}

static SQLType double_return_type(vector<SQLType> &arguments) {
	return SQLTypeId::DOUBLE;
}

static SQLType tint_return_type(vector<SQLType> &arguments) {
	return SQLTypeId::TINYINT;
}

void abs_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result);

class AbsFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("abs", single_numeric_argument, same_return_type, abs_function);
	}
};

void cbrt_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result);
class CbRtFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("cbrt", single_numeric_argument, double_return_type, cbrt_function);
	}
};

void degrees_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                      Vector &result);

class DegreesFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("degrees", single_numeric_argument, double_return_type, degrees_function);
	}
};

void radians_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                      Vector &result);

class RadiansFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("radians", single_numeric_argument, double_return_type, radians_function);
	}
};

void exp_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result);

class ExpFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("exp", single_numeric_argument, double_return_type, exp_function);
	}
};

void ln_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                 Vector &result);

class LnFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("ln", single_numeric_argument, double_return_type, ln_function);
	}
};

void log10_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                    Vector &result);

class LogFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("log", single_numeric_argument, double_return_type, log10_function);
	}
};

class Log10Function {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("log10", single_numeric_argument, double_return_type, log10_function);
	}
};

void log2_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result);

class Log2Function {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("log2", single_numeric_argument, double_return_type, log2_function);
	}
};

void sqrt_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result);

class SqrtFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("sqrt", single_numeric_argument, same_return_type, sqrt_function);
	}
};

void ceil_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result);

void floor_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                    Vector &result);

class CeilFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("ceil", single_numeric_argument, same_return_type, ceil_function);
	}
};

class CeilingFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("ceiling", single_numeric_argument, same_return_type, ceil_function);
	}
};

class FloorFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("floor", single_numeric_argument, same_return_type, floor_function);
	}
};

void pi_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                 Vector &result);

class PiFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("pi", no_arguments, double_return_type, pi_function);
	}
};

void sign_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result);

class SignFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("sign", single_numeric_argument, tint_return_type, sign_function);
	}
};

// log

// pi

// random

// SQRT, POWER, CBRT

} // namespace duckdb
