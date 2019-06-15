#include "function/scalar_function/trigonometrics.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/types/constant_vector.hpp"
#include "common/types/static_vector.hpp"

using namespace std;

namespace duckdb {

void sin_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	assert(input_count == 1);
	inputs[0].Cast(TypeId::DOUBLE);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::Sin(inputs[0], result);
}

void cos_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	assert(input_count == 1);
	inputs[0].Cast(TypeId::DOUBLE);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::Cos(inputs[0], result);
}

void tan_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	assert(input_count == 1);
	inputs[0].Cast(TypeId::DOUBLE);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::Tan(inputs[0], result);
}

void asin_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	assert(input_count == 1);
	inputs[0].Cast(TypeId::DOUBLE);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::ASin(inputs[0], result);
}

void acos_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	assert(input_count == 1);
	inputs[0].Cast(TypeId::DOUBLE);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::ACos(inputs[0], result);
}

void atan_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	assert(input_count == 1);
	inputs[0].Cast(TypeId::DOUBLE);
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::ATan(inputs[0], result);
}

void cot_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                  Vector &result) {
	assert(input_count == 1);
	inputs[0].Cast(TypeId::DOUBLE);
	result.Initialize(TypeId::DOUBLE);
	ConstantVector one(Value((double)1.0));
	StaticVector<double> tan_res;
	VectorOperations::Tan(inputs[0], tan_res);
	VectorOperations::Divide(one, tan_res, result);
}

void atan2_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                    Vector &result) {
	assert(input_count == 2);

	inputs[0].Cast(TypeId::DOUBLE);
	inputs[1].Cast(TypeId::DOUBLE);

	result.Initialize(TypeId::DOUBLE);
	VectorOperations::ATan2(inputs[0], inputs[1], result);
}

bool trig_matches_arguments(vector<SQLType> &arguments) {
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

bool atan2_matches_arguments(vector<SQLType> &arguments) {
	if (arguments.size() != 2) {
		return false;
	}
	if ((!IsNumericType(arguments[0].id) && arguments[0].id != SQLTypeId::SQLNULL) ||
	    (!IsNumericType(arguments[1].id) && arguments[1].id != SQLTypeId::SQLNULL)) {
		return false;
	}
	return true;
}

SQLType trig_get_return_type(vector<SQLType> &arguments) {
	return SQLTypeId::DOUBLE;
}

} // namespace duckdb
