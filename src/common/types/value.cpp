
#include "common/types/value.hpp"
#include "common/exception.hpp"
#include "common/operator/comparison_operators.hpp"
#include "common/operator/numeric_binary_operators.hpp"
#include "common/serializer.hpp"
#include "common/types/operators.hpp"

using namespace duckdb;
using namespace std;

Value::Value(const Value &other)
    : type(other.type), is_null(other.is_null), str_value(other.str_value) {
	this->value_ = other.value_;
}

Value Value::MinimumValue(TypeId type) {
	Value result;
	result.type = type;
	result.is_null = false;
	switch (type) {
	case TypeId::TINYINT:
		result.value_.tinyint = std::numeric_limits<int8_t>::min();
		break;
	case TypeId::SMALLINT:
		result.value_.smallint = std::numeric_limits<int16_t>::min();
		break;
	case TypeId::INTEGER:
		result.value_.integer = std::numeric_limits<int32_t>::min();
		break;
	case TypeId::BIGINT:
		result.value_.bigint = std::numeric_limits<int64_t>::min();
		break;
	case TypeId::DECIMAL:
		result.value_.decimal = std::numeric_limits<double>::min();
		break;
	case TypeId::DATE:
		result.value_.date = std::numeric_limits<date_t>::min();
		break;
	case TypeId::POINTER:
		result.value_.pointer = std::numeric_limits<uint64_t>::min();
		break;
	case TypeId::TIMESTAMP:
		result.value_.timestamp = std::numeric_limits<timestamp_t>::min();
		break;
	default:
		throw InvalidTypeException(type, "MinimumValue requires numeric type");
	}
	return result;
}

Value Value::MaximumValue(TypeId type) {
	Value result;
	result.type = type;
	result.is_null = false;
	switch (type) {
	case TypeId::TINYINT:
		result.value_.tinyint = std::numeric_limits<int8_t>::max();
		break;
	case TypeId::SMALLINT:
		result.value_.smallint = std::numeric_limits<int16_t>::max();
		break;
	case TypeId::INTEGER:
		result.value_.integer = std::numeric_limits<int32_t>::max();
		break;
	case TypeId::BIGINT:
		result.value_.bigint = std::numeric_limits<int64_t>::max();
		break;
	case TypeId::DECIMAL:
		result.value_.decimal = std::numeric_limits<double>::max();
		break;
	case TypeId::DATE:
		result.value_.date = std::numeric_limits<date_t>::max();
		break;
	case TypeId::POINTER:
		result.value_.pointer = std::numeric_limits<uint64_t>::max();
		break;
	case TypeId::TIMESTAMP:
		result.value_.timestamp = std::numeric_limits<timestamp_t>::min();
		break;
	default:
		throw InvalidTypeException(type, "MaximumValue requires numeric type");
	}
	return result;
}

Value Value::BOOLEAN(int8_t value) {
	Value result(TypeId::TINYINT);
	result.value_.boolean = value ? true : false;
	result.is_null = false;
	return result;
}

Value Value::TINYINT(int8_t value) {
	Value result(TypeId::TINYINT);
	result.value_.tinyint = value;
	result.is_null = false;
	return result;
}
Value Value::SMALLINT(int16_t value) {
	Value result(TypeId::SMALLINT);
	result.value_.smallint = value;
	result.is_null = false;
	return result;
}
Value Value::INTEGER(int32_t value) {
	Value result(TypeId::INTEGER);
	result.value_.integer = value;
	result.is_null = false;
	return result;
}
Value Value::BIGINT(int64_t value) {
	Value result(TypeId::BIGINT);
	result.value_.bigint = value;
	result.is_null = false;
	return result;
}
Value Value::POINTER(uint64_t value) {
	Value result(TypeId::POINTER);
	result.value_.pointer = value;
	result.is_null = false;
	return result;
}
Value Value::DATE(date_t value) {
	Value result(TypeId::DATE);
	result.value_.date = value;
	result.is_null = false;
	return result;
}

Value Value::Numeric(TypeId type, int64_t value) {
	assert(!TypeIsIntegral(type) || (value >= duckdb::MinimumValue(type) &&
	                                 value <= duckdb::MaximumValue(type)));
	Value val(type);
	val.is_null = false;
	switch (type) {
	case TypeId::TINYINT:
		return Value::TINYINT(value);
	case TypeId::SMALLINT:
		return Value::SMALLINT(value);
	case TypeId::INTEGER:
		return Value::INTEGER(value);
	case TypeId::BIGINT:
		return Value::BIGINT(value);
	case TypeId::DECIMAL:
		return Value((double)value);
	case TypeId::DATE:
		return Value::DATE(value);
	case TypeId::POINTER:
		return Value::POINTER(value);
	default:
		throw InvalidTypeException(type, "Numeric requires numeric type");
	}
	return val;
}

int64_t Value::GetNumericValue() {
	if (is_null) {
		throw ConversionException("Cannot convert NULL Value to numeric value");
	}
	switch (type) {
	case TypeId::TINYINT:
		return value_.tinyint;
	case TypeId::SMALLINT:
		return value_.smallint;
	case TypeId::INTEGER:
		return value_.integer;
	case TypeId::BIGINT:
		return value_.bigint;
	case TypeId::DECIMAL:
		return value_.decimal;
	case TypeId::DATE:
		return value_.date;
	case TypeId::POINTER:
		return value_.pointer;
	default:
		throw InvalidTypeException(type,
		                           "GetNumericValue requires numeric type");
	}
}

string Value::ToString() const {
	if (is_null) {
		return "NULL";
	}
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

template <class DST, class OP> DST Value::_cast(const Value &v) {
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

Value Value::CastAs(TypeId new_type) const {
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
	case TypeId::VARCHAR: {
		new_value.str_value = _cast<std::string, operators::Cast>(*this);
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for casting");
	}
	return new_value;
}

template <class OP>
void Value::_templated_binary_operation(const Value &left, const Value &right,
                                        Value &result, bool ignore_null) {
	if (left.is_null || right.is_null) {
		if (ignore_null) {
			if (!right.is_null) {
				result = right;
			} else {
				result = left;
			}
		} else {
			result.type = std::max(left.type, right.type);
			result.is_null = true;
		}
		return;
	}
	result.is_null = false;
	if (TypeIsIntegral(left.type) && TypeIsIntegral(right.type) &&
	    (left.type < TypeId::BIGINT || right.type < TypeId::BIGINT)) {
		// upcast integer types if necessary
		Value left_cast = left.CastAs(TypeId::BIGINT);
		Value right_cast = right.CastAs(TypeId::BIGINT);
		_templated_binary_operation<OP>(left_cast, right_cast, result,
		                                ignore_null);
		if (result.is_null) {
			result.type = std::max(left.type, right.type);
		} else {
			auto type = std::max(MinimalType(result.GetNumericValue()),
			                     std::max(left.type, right.type));
			result = result.CastAs(type);
		}
		return;
	}
	if (TypeIsIntegral(left.type) && right.type == TypeId::DECIMAL) {
		Value left_cast = left.CastAs(TypeId::DECIMAL);
		_templated_binary_operation<OP>(left_cast, right, result, ignore_null);
		return;
	}
	if (left.type == TypeId::DECIMAL && TypeIsIntegral(right.type)) {
		Value right_cast = right.CastAs(TypeId::DECIMAL);
		_templated_binary_operation<OP>(left, right_cast, result, ignore_null);
		return;
	}
	if (left.type != right.type) {
		throw TypeMismatchException(
		    left.type, right.type,
		    "Cannot perform binary operation on these two types");
	}
	result.type = left.type;
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
	case TypeId::DATE:
		result.value_.date = OP::Operation(left.value_.date, right.value_.date);
		break;
	case TypeId::TIMESTAMP:
		result.value_.timestamp =
		    OP::Operation(left.value_.timestamp, right.value_.timestamp);
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
void Value::Add(const Value &left, const Value &right, Value &result) {
	_templated_binary_operation<operators::Add>(left, right, result, false);
}

void Value::Subtract(const Value &left, const Value &right, Value &result) {
	_templated_binary_operation<operators::Subtract>(left, right, result,
	                                                 false);
}

void Value::Multiply(const Value &left, const Value &right, Value &result) {
	_templated_binary_operation<operators::Multiply>(left, right, result,
	                                                 false);
}

void Value::Divide(const Value &left, const Value &right, Value &result) {
	Value zero = Value::Numeric(right.type, 0);
	if (Value::Equals(right, zero)) {
		// special case: divide by zero
		result.type = std::max(left.type, right.type);
		result.is_null = true;
	} else {
		_templated_binary_operation<operators::Divide>(left, right, result,
		                                               false);
	}
}

void Value::Modulo(const Value &left, const Value &right, Value &result) {
	//_templated_binary_operation<operators::Modulo>(left, right, result,
	// false);
}

void Value::Min(const Value &left, const Value &right, Value &result) {
	_templated_binary_operation<operators::Min>(left, right, result, true);
}

void Value::Max(const Value &left, const Value &right, Value &result) {
	_templated_binary_operation<operators::Max>(left, right, result, true);
}

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
template <class OP>
bool Value::_templated_boolean_operation(const Value &left,
                                         const Value &right) {
	if (left.type != right.type) {
		if (TypeIsNumeric(left.type) && TypeIsNumeric(right.type)) {
			if (left.type < right.type) {
				Value left_cast = left.CastAs(right.type);
				return _templated_boolean_operation<OP>(left_cast, right);
			} else {
				Value right_cast = right.CastAs(left.type);
				return _templated_boolean_operation<OP>(left, right_cast);
			}
		}
		throw TypeMismatchException(
		    left.type, right.type,
		    "Cannot perform boolean operation on these two types");
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
	case TypeId::DATE:
		return OP::Operation(left.value_.date, right.value_.date);
	case TypeId::TIMESTAMP:
		return OP::Operation(left.value_.timestamp, right.value_.timestamp);
	case TypeId::VARCHAR:
		return OP::Operation(left.str_value, right.str_value);
	default:
		throw NotImplementedException("Unimplemented type");
	}
}

bool Value::Equals(const Value &left, const Value &right) {
	if (left.is_null && right.is_null) {
		return true;
	}
	return _templated_boolean_operation<operators::Equals>(left, right);
}

bool Value::NotEquals(const Value &left, const Value &right) {
	return !Value::Equals(left, right);
}

bool Value::GreaterThan(const Value &left, const Value &right) {
	if (left.is_null && right.is_null) {
		return false;
	} else if (right.is_null) {
		return true;
	} else if (left.is_null) {
		return false;
	}
	return _templated_boolean_operation<operators::GreaterThan>(left, right);
}

bool Value::GreaterThanEquals(const Value &left, const Value &right) {
	if (left.is_null && right.is_null) {
		return true;
	} else if (right.is_null) {
		return true;
	} else if (left.is_null) {
		return false;
	}
	return _templated_boolean_operation<operators::GreaterThanEquals>(left,
	                                                                  right);
}

bool Value::LessThan(const Value &left, const Value &right) {
	return Value::GreaterThan(right, left);
}

bool Value::LessThanEquals(const Value &left, const Value &right) {
	return Value::GreaterThanEquals(right, left);
}

void Value::Serialize(Serializer &serializer) {
	serializer.Write<TypeId>(type);
	serializer.Write<bool>(is_null);
	if (!is_null) {
		switch (type) {
		case TypeId::BOOLEAN:
			serializer.Write<int8_t>(value_.boolean);
			break;
		case TypeId::TINYINT:
			serializer.Write<int8_t>(value_.tinyint);
			break;
		case TypeId::SMALLINT:
			serializer.Write<int16_t>(value_.smallint);
			break;
		case TypeId::INTEGER:
			serializer.Write<int32_t>(value_.integer);
			break;
		case TypeId::BIGINT:
			serializer.Write<int64_t>(value_.bigint);
			break;
		case TypeId::DECIMAL:
			serializer.Write<double>(value_.decimal);
			break;
		case TypeId::POINTER:
			serializer.Write<uint64_t>(value_.pointer);
			break;
		case TypeId::DATE:
			serializer.Write<date_t>(value_.date);
			break;
		case TypeId::TIMESTAMP:
			serializer.Write<timestamp_t>(value_.timestamp);
			break;
		case TypeId::VARCHAR:
			serializer.WriteString(str_value);
			break;
		default:
			throw NotImplementedException(
			    "Value type not implemented for serialization!");
		}
	}
}

Value Value::Deserialize(Deserializer &source) {
	auto type = source.Read<TypeId>();
	auto is_null = source.Read<bool>();
	Value new_value = Value(type);
	if (is_null) {
		return new_value;
	}
	new_value.is_null = false;
	switch (type) {
	case TypeId::BOOLEAN:
		new_value.value_.boolean = source.Read<int8_t>();
		break;
	case TypeId::TINYINT:
		new_value.value_.tinyint = source.Read<int8_t>();
		break;
	case TypeId::SMALLINT:
		new_value.value_.smallint = source.Read<int16_t>();
		break;
	case TypeId::INTEGER:
		new_value.value_.integer = source.Read<int32_t>();
		break;
	case TypeId::BIGINT:
		new_value.value_.bigint = source.Read<int64_t>();
		break;
	case TypeId::DECIMAL:
		new_value.value_.decimal = source.Read<double>();
		break;
	case TypeId::POINTER:
		new_value.value_.pointer = source.Read<uint64_t>();
		break;
	case TypeId::DATE:
		new_value.value_.date = source.Read<date_t>();
		break;
	case TypeId::TIMESTAMP:
		new_value.value_.timestamp = source.Read<timestamp_t>();
		break;
	case TypeId::VARCHAR:
		new_value.str_value = source.Read<string>();
		break;
	default:
		throw NotImplementedException(
		    "Value type not implemented for deserialization");
	}
	return new_value;
}
