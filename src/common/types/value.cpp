
#include "common/types/value.hpp"
#include "common/exception.hpp"
#include "common/types/operators.hpp"

using namespace duckdb;
using namespace std;

Value::Value(const Value &other)
    : type(other.type), is_null(other.is_null), len(other.len) {
	if ((type == TypeId::VARCHAR || type == TypeId::VARBINARY ||
	     type == TypeId::ARRAY) &&
	    other.value_.data) {
		value_.data = new char[len + 1];
		memcpy(value_.data, other.value_.data, other.len + 1);
	} else {
		this->value_ = other.value_;
	}
}

Value Value::NumericValue(TypeId id, int64_t value) {
	switch(id) {
	case TypeId::TINYINT:
		return Value((int8_t) value);
	case TypeId::SMALLINT:
		return Value((int16_t) value);
	case TypeId::INTEGER:
		return Value((int32_t) value);
	case TypeId::BIGINT:
		return Value((int64_t) value);
	case TypeId::DECIMAL:
		return Value((double) value);
	default:
		throw Exception("TypeId is not numeric!");
	}
}

Value Value::CastAs(TypeId new_type) {
	// check if we can just make a copy
	if (new_type == this->type) {
		return *this;
	}
	// have to do a cast
	Value new_value;
	new_value.type = new_type;
	new_value.is_null = this->is_null;
	if (is_null) {
		return new_value;
	}

	throw NotImplementedException("Did not implement value cast yet!");
}

string Value::ToString() const {
	switch (type) {
	case TypeId::BOOLEAN:
		return value_.boolean ? "True" : "False";
	case TypeId::TINYINT:
		return to_string(value_.tinyint);
	case TypeId::SMALLINT:
		return to_string(value_.smallint);
	case TypeId::INTEGER:
		return to_string(value_.integer);
	case TypeId::BIGINT:
		return to_string(value_.bigint);
	case TypeId::DECIMAL:
		return to_string(value_.decimal);
	default:
		throw NotImplementedException("Unimplemented printing");
	}
}

template<class OP>
static void _templated_binary_operation(Value &left, Value &right, Value &result) {
	if (left.type != right.type || left.type != result.type) {
		throw NotImplementedException("Not matching type not implemented!");
	}
	switch(left.type) {
	case TypeId::BOOLEAN:
		result.value_.boolean = OP::Operation(left.value_.boolean, right.value_.boolean);
		break;
	case TypeId::TINYINT:
		result.value_.tinyint = OP::Operation(left.value_.tinyint, right.value_.tinyint);
		break;
	case TypeId::SMALLINT:
		result.value_.smallint = OP::Operation(left.value_.smallint, right.value_.smallint);
		break;
	case TypeId::INTEGER:
		result.value_.integer = OP::Operation(left.value_.integer, right.value_.integer);
		break;
	case TypeId::BIGINT:
		result.value_.bigint = OP::Operation(left.value_.bigint, right.value_.bigint);
		break;
	case TypeId::DECIMAL:
		result.value_.decimal = OP::Operation(left.value_.decimal, right.value_.decimal);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

void Value::Add(Value &left, Value &right, Value &result) {
	_templated_binary_operation<operators::Addition>(left, right, result);
}

void Value::Subtract(Value &left, Value &right, Value &result) {
	_templated_binary_operation<operators::Subtraction>(left, right, result);
}

void Value::Multiply(Value &left, Value &right, Value &result) {
	_templated_binary_operation<operators::Multiplication>(left, right, result);
}

void Value::Divide(Value &left, Value &right, Value &result) {
	_templated_binary_operation<operators::Division>(left, right, result);
}

void Value::Min(Value &left, Value &right, Value &result) {
	_templated_binary_operation<operators::Min>(left, right, result);
}

void Value::Max(Value &left, Value &right, Value &result) {
	_templated_binary_operation<operators::Max>(left, right, result);
}

