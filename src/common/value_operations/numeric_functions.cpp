#include "duckdb/common/operator/numeric_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"

using namespace duckdb;
using namespace std;

Value ValueOperations::Abs(const Value &op) {
	if (op.is_null) {
		return Value(op.type);
	}
	Value result;
	result.is_null = false;
	result.type = op.type;
	switch (op.type) {
	case TypeId::TINYINT:
		result.value_.tinyint = Abs::Operation(op.value_.tinyint);
		break;
	case TypeId::SMALLINT:
		result.value_.smallint = Abs::Operation(op.value_.smallint);
		break;
	case TypeId::INTEGER:
		result.value_.integer = Abs::Operation(op.value_.integer);
		break;
	case TypeId::BIGINT:
		result.value_.bigint = Abs::Operation(op.value_.bigint);
		break;
	case TypeId::FLOAT:
		result.value_.float_ = Abs::Operation(op.value_.float_);
		break;
	case TypeId::DOUBLE:
		result.value_.double_ = Abs::Operation(op.value_.double_);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
	return result;
}
