#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"

namespace duckdb {
using namespace std;

static LogicalType get_value_binop_result_type(const Value &left, const Value &right) {
	auto left_type = left.type();
	auto right_type = right.type();
	if (left.is_null || right.is_null) {
		// either value is NULL: result is NULL
		if (left_type.InternalType() > right_type.InternalType()) {
			return left_type;
		} else {
			return right_type;
		}
	}
	// figure out the result type
	LogicalType result_type = left_type;
	if (left_type != right_type) {
		// pick the type with the highest type order
		if (left_type.NumericTypeOrder() > right_type.NumericTypeOrder()) {
			result_type = left_type;
		} else {
			result_type = right_type;
		}
	}
	return result_type;
}

template <class OP> static Value templated_binary_operation(const Value &left, const Value &right) {
	auto result_type = get_value_binop_result_type(left, right);
	if (left.is_null || right.is_null) {
		return Value(result_type);
	}
	if (result_type.IsIntegral()) {
		// integer addition
		return Value::Numeric(result_type, OP::template Operation<hugeint_t, hugeint_t, hugeint_t>(
		                                       left.GetValue<hugeint_t>(), right.GetValue<hugeint_t>()));
	} else if (result_type.InternalType() == PhysicalType::FLOAT) {
		return Value::FLOAT(
		    OP::template Operation<float, float, float>(left.GetValue<float>(), right.GetValue<float>()));
	} else if (result_type.InternalType() == PhysicalType::DOUBLE) {
		return Value::DOUBLE(
		    OP::template Operation<double, double, double>(left.GetValue<double>(), right.GetValue<double>()));
	} else {
		throw NotImplementedException("Unimplemented type for value binary op");
	}
}

//===--------------------------------------------------------------------===//
// Numeric Operations
//===--------------------------------------------------------------------===//
Value ValueOperations::Add(const Value &left, const Value &right) {
	return templated_binary_operation<duckdb::AddOperator>(left, right);
}

Value ValueOperations::Subtract(const Value &left, const Value &right) {
	return templated_binary_operation<duckdb::SubtractOperator>(left, right);
}

Value ValueOperations::Multiply(const Value &left, const Value &right) {
	return templated_binary_operation<duckdb::MultiplyOperator>(left, right);
}

Value ValueOperations::Modulo(const Value &left, const Value &right) {
	if (right == 0) {
		return templated_binary_operation<duckdb::ModuloOperator>(left, Value(right.type()));
	} else {
		return templated_binary_operation<duckdb::ModuloOperator>(left, right);
	}
}

Value ValueOperations::Divide(const Value &left, const Value &right) {
	if (right == 0) {
		return templated_binary_operation<duckdb::DivideOperator>(left, Value(right.type()));
	} else {
		return templated_binary_operation<duckdb::DivideOperator>(left, right);
	}
}

// Value ValueOperations::Min(const Value &left, const Value &right) {
// 	return templated_binary_operation<duckdb::Min, true>(left, right);
// }

// Value ValueOperations::Max(const Value &left, const Value &right) {
// 	return templated_binary_operation<duckdb::Max, true>(left, right);
// }

} // namespace duckdb
