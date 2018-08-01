
#include "common/types/value.hpp"
#include "common/exception.hpp"
#include "common/types/operators.hpp"

using namespace duckdb;
using namespace std;

Value::Value(const Value &other)
    : type(other.type), is_null(other.is_null), str_value(other.str_value) {
	this->value_ = other.value_;
}

Value Value::NumericValue(TypeId id, int64_t value) {
	switch (id) {
	case TypeId::TINYINT:
		return Value((int8_t)value);
	case TypeId::SMALLINT:
		return Value((int16_t)value);
	case TypeId::INTEGER:
		return Value((int32_t)value);
	case TypeId::BIGINT:
		return Value((int64_t)value);
	case TypeId::DECIMAL:
		return Value((double)value);
	case TypeId::POINTER:
		return Value((uint64_t)value);
	default:
		throw Exception("TypeId is not numeric!");
	}
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
	case TypeId::POINTER:
		return to_string(value_.pointer);
	case TypeId::DATE:
		return Date::ToString(value_.date);
	case TypeId::VARCHAR:
		return str_value;
	default:
		throw NotImplementedException("Unimplemented type for printing");
	}
}

template <class DST, class OP>
DST Value::_cast(Value &v) {
	switch (v.type) {
	case TypeId::BOOLEAN:
		return OP::template Operation<int8_t, DST>(v.value_.boolean);
	case TypeId::TINYINT:
		return OP::template Operation<int8_t, DST>(v.value_.tinyint);
	case TypeId::SMALLINT:
		return OP::template Operation<int16_t, DST>(v.value_.smallint);
	case TypeId::INTEGER:
		return OP::template Operation<int32_t, DST>(v.value_.integer);
	case TypeId::BIGINT:
		return OP::template Operation<int64_t, DST>(v.value_.bigint);
	case TypeId::DECIMAL:
		return OP::template Operation<double, DST>(v.value_.decimal);
	case TypeId::POINTER:
		return OP::template Operation<uint64_t, DST>(v.value_.pointer);
	case TypeId::VARCHAR:
		return OP::template Operation<const char *, DST>(v.str_value.c_str());
	case TypeId::DATE:
		return operators::CastFromDate::Operation<date_t, DST>(v.value_.date);
	default:
		throw NotImplementedException("Unimplemented type for casting");
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

	switch (new_value.type) {
	case TypeId::BOOLEAN:
		new_value.value_.boolean = _cast<int8_t, operators::Cast>(*this);
		break;
	case TypeId::TINYINT:
		new_value.value_.tinyint = _cast<int8_t, operators::Cast>(*this);
		break;
	case TypeId::SMALLINT:
		new_value.value_.smallint = _cast<int16_t, operators::Cast>(*this);
		break;
	case TypeId::INTEGER:
		new_value.value_.integer = _cast<int32_t, operators::Cast>(*this);
		break;
	case TypeId::BIGINT:
		new_value.value_.bigint = _cast<int64_t, operators::Cast>(*this);
		break;
	case TypeId::DECIMAL:
		new_value.value_.decimal = _cast<double, operators::Cast>(*this);
		break;
	case TypeId::POINTER:
		new_value.value_.pointer = _cast<uint64_t, operators::Cast>(*this);
		break;
	case TypeId::DATE:
		new_value.value_.date = _cast<date_t, operators::CastToDate>(*this);
		break;
	default:
		throw NotImplementedException("Unimplemented type for casting");
	}
	return new_value;
}

template <class OP>
void Value::_templated_binary_operation(Value &left, Value &right,
                                        Value &result) {
	if (left.type != right.type || left.type != result.type) {
		throw NotImplementedException("Not matching type not implemented!");
	}
	switch (left.type) {
	case TypeId::BOOLEAN:
		result.value_.boolean =
		    OP::Operation(left.value_.boolean, right.value_.boolean);
		break;
	case TypeId::TINYINT:
		result.value_.tinyint =
		    OP::Operation(left.value_.tinyint, right.value_.tinyint);
		break;
	case TypeId::SMALLINT:
		result.value_.smallint =
		    OP::Operation(left.value_.smallint, right.value_.smallint);
		break;
	case TypeId::INTEGER:
		result.value_.integer =
		    OP::Operation(left.value_.integer, right.value_.integer);
		break;
	case TypeId::BIGINT:
		result.value_.bigint =
		    OP::Operation(left.value_.bigint, right.value_.bigint);
		break;
	case TypeId::DECIMAL:
		result.value_.decimal =
		    OP::Operation(left.value_.decimal, right.value_.decimal);
		break;
	case TypeId::POINTER:
		result.value_.pointer =
		    OP::Operation(left.value_.pointer, right.value_.pointer);
		break;
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

//===--------------------------------------------------------------------===//
// Numeric Operations
//===--------------------------------------------------------------------===//
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

void Value::Modulo(Value &left, Value &right, Value &result) {
	_templated_binary_operation<operators::Modulo>(left, right, result);
}

void Value::Min(Value &left, Value &right, Value &result) {
	_templated_binary_operation<operators::Min>(left, right, result);
}

void Value::Max(Value &left, Value &right, Value &result) {
	_templated_binary_operation<operators::Max>(left, right, result);
}

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
template <class OP>
bool Value::_templated_boolean_operation(Value &left, Value &right) {
	if (left.type != right.type) {
		throw NotImplementedException("Not matching type not implemented!");
	}
	switch (left.type) {
	case TypeId::BOOLEAN:
		return OP::Operation(left.value_.boolean, right.value_.boolean);
	case TypeId::TINYINT:
		return OP::Operation(left.value_.tinyint, right.value_.tinyint);
	case TypeId::SMALLINT:
		return OP::Operation(left.value_.smallint, right.value_.smallint);
	case TypeId::INTEGER:
		return OP::Operation(left.value_.integer, right.value_.integer);
	case TypeId::BIGINT:
		return OP::Operation(left.value_.bigint, right.value_.bigint);
	case TypeId::DECIMAL:
		return OP::Operation(left.value_.decimal, right.value_.decimal);
	case TypeId::POINTER:
		return OP::Operation(left.value_.pointer, right.value_.pointer);
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

bool Value::Equals(Value &left, Value &right) {
	return _templated_boolean_operation<operators::Equals>(left, right);
}

bool Value::NotEquals(Value &left, Value &right) {
	return _templated_boolean_operation<operators::NotEquals>(left, right);
}

bool Value::GreaterThan(Value &left, Value &right) {
	return _templated_boolean_operation<operators::GreaterThan>(left, right);
}

bool Value::GreaterThanEquals(Value &left, Value &right) {
	return _templated_boolean_operation<operators::GreaterThanEquals>(left,
	                                                                  right);
}

bool Value::LessThan(Value &left, Value &right) {
	return _templated_boolean_operation<operators::LessThan>(left, right);
}

bool Value::LessThanEquals(Value &left, Value &right) {
	return _templated_boolean_operation<operators::LessThanEquals>(left, right);
}
