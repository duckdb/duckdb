#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"

namespace duckdb {
using namespace std;

template <class OP> static Value templated_binary_operation(const Value &left, const Value &right) {
	auto left_type = left.type();
	auto right_type = right.type();
	LogicalType result_type = left_type;
	if (left_type != right_type) {
		result_type = LogicalType::MaxLogicalType(left.type(), right.type());
		Value left_cast = left.CastAs(result_type);
		Value right_cast = right.CastAs(result_type);
		return templated_binary_operation<OP>(left_cast, right_cast);
	}
	if (TypeIsIntegral(result_type.InternalType())) {
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
