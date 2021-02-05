#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/subtract.hpp"

namespace duckdb {

template <class OP>
static Value binary_value_operation(const Value &left, const Value &right) {
	auto left_type = left.type();
	auto right_type = right.type();
	LogicalType result_type = left_type;
	if (left_type != right_type) {
		result_type = LogicalType::MaxLogicalType(left.type(), right.type());
		Value left_cast = left.CastAs(result_type);
		Value right_cast = right.CastAs(result_type);
		return binary_value_operation<OP>(left_cast, right_cast);
	}
	if (left.is_null || right.is_null) {
		return Value().CastAs(result_type);
	}
	if (TypeIsIntegral(result_type.InternalType())) {
		hugeint_t left_hugeint;
		hugeint_t right_hugeint;
		switch (result_type.InternalType()) {
		case PhysicalType::INT8:
			left_hugeint = Hugeint::Convert(left.value_.tinyint);
			right_hugeint = Hugeint::Convert(right.value_.tinyint);
			break;
		case PhysicalType::INT16:
			left_hugeint = Hugeint::Convert(left.value_.smallint);
			right_hugeint = Hugeint::Convert(right.value_.smallint);
			break;
		case PhysicalType::INT32:
			left_hugeint = Hugeint::Convert(left.value_.integer);
			right_hugeint = Hugeint::Convert(right.value_.integer);
			break;
		case PhysicalType::INT64:
			left_hugeint = Hugeint::Convert(left.value_.bigint);
			right_hugeint = Hugeint::Convert(right.value_.bigint);
			break;
		case PhysicalType::INT128:
			left_hugeint = left.value_.hugeint;
			right_hugeint = right.value_.hugeint;
			break;
		default:
			throw NotImplementedException("Unimplemented type for value binary op");
		}
		// integer addition
		return Value::Numeric(result_type,
		                      OP::template Operation<hugeint_t, hugeint_t, hugeint_t>(left_hugeint, right_hugeint));
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
	return binary_value_operation<duckdb::AddOperator>(left, right);
}

Value ValueOperations::Subtract(const Value &left, const Value &right) {
	return binary_value_operation<duckdb::SubtractOperator>(left, right);
}

Value ValueOperations::Multiply(const Value &left, const Value &right) {
	return binary_value_operation<duckdb::MultiplyOperator>(left, right);
}

Value ValueOperations::Modulo(const Value &left, const Value &right) {
	if (right == 0) {
		return Value(right.type());
	} else {
		return binary_value_operation<duckdb::ModuloOperator>(left, right);
	}
}

Value ValueOperations::Divide(const Value &left, const Value &right) {
	if (right == 0) {
		return Value(right.type());
	} else {
		return binary_value_operation<duckdb::DivideOperator>(left, right);
	}
}

} // namespace duckdb
