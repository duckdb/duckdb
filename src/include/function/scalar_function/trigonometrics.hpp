//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/trigonometrics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/scalar_function.hpp"

namespace duckdb {

void sin_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result);

void cos_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result);

void tan_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result);

void asin_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result);

void acos_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result);

void atan_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result);

void cot_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result);

// special snowflake, two parameters
void atan2_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                    Vector &result);

bool trig_matches_arguments(vector<SQLType> &arguments);
bool atan2_matches_arguments(vector<SQLType> &arguments);
SQLType trig_get_return_type(vector<SQLType> &arguments);

class SinFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("sin", trig_matches_arguments, trig_get_return_type, sin_function);
	}
};

class CosFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("cos", trig_matches_arguments, trig_get_return_type, cos_function);
	}
};

class TanFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("tan", trig_matches_arguments, trig_get_return_type, tan_function);
	}
};

class ASinFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("asin", trig_matches_arguments, trig_get_return_type, asin_function);
	}
};

class ACosFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("acos", trig_matches_arguments, trig_get_return_type, acos_function);
	}
};

class ATanFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("atan", trig_matches_arguments, trig_get_return_type, atan_function);
	}
};

class CoTFunction {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("cot", trig_matches_arguments, trig_get_return_type, cot_function);
	}
};

class ATan2Function {
public:
	static ScalarFunction GetFunction() {
		return ScalarFunction("atan2", atan2_matches_arguments, trig_get_return_type, atan2_function);
	}
};

} // namespace duckdb
