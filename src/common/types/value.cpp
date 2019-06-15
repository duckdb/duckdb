#include "common/types/value.hpp"

#include "common/exception.hpp"
#include "common/limits.hpp"
#include "common/operator/aggregate_operators.hpp"
#include "common/operator/cast_operators.hpp"
#include "common/operator/comparison_operators.hpp"
#include "common/operator/numeric_binary_operators.hpp"
#include "common/printer.hpp"
#include "common/serializer.hpp"
#include "common/types/date.hpp"
#include "common/types/time.hpp"
#include "common/types/timestamp.hpp"
#include "common/types/vector.hpp"
#include "common/value_operations/value_operations.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/string_util.hpp"

using namespace duckdb;
using namespace std;

Value::Value(const Value &other) : type(other.type), is_null(other.is_null), str_value(other.str_value) {
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
	case TypeId::FLOAT:
		result.value_.float_ = std::numeric_limits<float>::min();
		break;
	case TypeId::DOUBLE:
		result.value_.double_ = std::numeric_limits<double>::min();
		break;
	case TypeId::POINTER:
		result.value_.pointer = std::numeric_limits<uint64_t>::min();
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
	case TypeId::FLOAT:
		result.value_.float_ = std::numeric_limits<float>::max();
		break;
	case TypeId::DOUBLE:
		result.value_.double_ = std::numeric_limits<double>::max();
		break;
	case TypeId::POINTER:
		result.value_.pointer = std::numeric_limits<uintptr_t>::max();
		break;
	default:
		throw InvalidTypeException(type, "MaximumValue requires numeric type");
	}
	return result;
}

Value Value::BOOLEAN(int8_t value) {
	Value result(TypeId::BOOLEAN);
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

Value Value::HASH(uint64_t value) {
	Value result(TypeId::HASH);
	result.value_.hash = value;
	result.is_null = false;
	return result;
}

Value Value::POINTER(uintptr_t value) {
	Value result(TypeId::POINTER);
	result.value_.pointer = value;
	result.is_null = false;
	return result;
}

Value Value::DATE(int32_t year, int32_t month, int32_t day) {
	return Value::INTEGER(Date::FromDate(year, month, day));
}

Value Value::TIMESTAMP(timestamp_t timestamp) {
	return Value::BIGINT(timestamp);
}

Value Value::TIMESTAMP(date_t date, dtime_t time) {
	return Value::BIGINT(Timestamp::FromDatetime(date, time));
}

Value Value::TIMESTAMP(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec, int32_t msec) {
	return Value::TIMESTAMP(Date::FromDate(year, month, day), Time::FromTime(hour, min, sec, msec));
}

template <> Value Value::CreateValue(int8_t value) {
	return Value::TINYINT(value);
}

template <> Value Value::CreateValue(int16_t value) {
	return Value::SMALLINT(value);
}

template <> Value Value::CreateValue(int32_t value) {
	return Value::INTEGER(value);
}

template <> Value Value::CreateValue(int64_t value) {
	return Value::BIGINT(value);
}

template <> Value Value::CreateValue(const char *value) {
	return Value(string(value));
}

template <> Value Value::CreateValue(string value) {
	return Value(value);
}

Value Value::Numeric(TypeId type, int64_t value) {
	assert(!TypeIsIntegral(type) ||
	       (value >= duckdb::MinimumValue(type) && (uint64_t)value <= duckdb::MaximumValue(type)));
	Value val(type);
	val.is_null = false;
	switch (type) {
	case TypeId::TINYINT:
		assert(value <= std::numeric_limits<int8_t>::max());
		return Value::TINYINT((int8_t)value);
	case TypeId::SMALLINT:
		assert(value <= std::numeric_limits<int16_t>::max());
		return Value::SMALLINT((int16_t)value);
	case TypeId::INTEGER:
		assert(value <= std::numeric_limits<int32_t>::max());
		return Value::INTEGER((int32_t)value);
	case TypeId::BIGINT:
		return Value::BIGINT(value);
	case TypeId::FLOAT:
		return Value((float)value);
	case TypeId::DOUBLE:
		return Value((double)value);
	case TypeId::HASH:
		return Value::HASH(value);
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
	case TypeId::FLOAT:
		return value_.float_;
	case TypeId::DOUBLE:
		return value_.double_;
	case TypeId::POINTER:
		return value_.pointer;
	default:
		throw InvalidTypeException(type, "GetNumericValue requires numeric type");
	}
}

string Value::ToString(SQLType sql_type) const {
	switch (sql_type.id) {
	case SQLTypeId::BOOLEAN:
		return value_.boolean ? "True" : "False";
	case SQLTypeId::TINYINT:
		return to_string(value_.tinyint);
	case SQLTypeId::SMALLINT:
		return to_string(value_.smallint);
	case SQLTypeId::INTEGER:
		return to_string(value_.integer);
	case SQLTypeId::BIGINT:
		return to_string(value_.bigint);
	case SQLTypeId::FLOAT:
		return to_string(value_.float_);
	case SQLTypeId::DOUBLE:
		return to_string(value_.double_);
	case SQLTypeId::DATE:
		return Date::ToString(value_.integer);
	case SQLTypeId::TIMESTAMP:
		return Timestamp::ToString(value_.bigint);
	case SQLTypeId::VARCHAR:
		return str_value;
	default:
		throw NotImplementedException("Unimplemented type for printing");
	}
}

string Value::ToString() const {
	if (is_null) {
		return "NULL";
	}
	return ToString(SQLTypeFromInternalType(type));
}

//===--------------------------------------------------------------------===//
// Numeric Operators
//===--------------------------------------------------------------------===//
Value Value::operator+(const Value &rhs) const {
	return ValueOperations::Add(*this, rhs);
}

Value Value::operator-(const Value &rhs) const {
	return ValueOperations::Subtract(*this, rhs);
}

Value Value::operator*(const Value &rhs) const {
	return ValueOperations::Multiply(*this, rhs);
}

Value Value::operator/(const Value &rhs) const {
	return ValueOperations::Divide(*this, rhs);
}

Value Value::operator%(const Value &rhs) const {
	throw NotImplementedException("value modulo");
	// return ValueOperations::Modulo(*this, rhs);
}

//===--------------------------------------------------------------------===//
// Comparison Operators
//===--------------------------------------------------------------------===//
bool Value::operator==(const Value &rhs) const {
	return ValueOperations::Equals(*this, rhs);
}

bool Value::operator!=(const Value &rhs) const {
	return ValueOperations::NotEquals(*this, rhs);
}

bool Value::operator<(const Value &rhs) const {
	return ValueOperations::LessThan(*this, rhs);
}

bool Value::operator>(const Value &rhs) const {
	return ValueOperations::GreaterThan(*this, rhs);
}

bool Value::operator<=(const Value &rhs) const {
	return ValueOperations::LessThanEquals(*this, rhs);
}

bool Value::operator>=(const Value &rhs) const {
	return ValueOperations::GreaterThanEquals(*this, rhs);
}

bool Value::operator==(const int64_t &rhs) const {
	return *this == Value::Numeric(type, rhs);
}

bool Value::operator!=(const int64_t &rhs) const {
	return *this != Value::Numeric(type, rhs);
}

bool Value::operator<(const int64_t &rhs) const {
	return *this < Value::Numeric(type, rhs);
}

bool Value::operator>(const int64_t &rhs) const {
	return *this > Value::Numeric(type, rhs);
}

bool Value::operator<=(const int64_t &rhs) const {
	return *this <= Value::Numeric(type, rhs);
}

bool Value::operator>=(const int64_t &rhs) const {
	return *this >= Value::Numeric(type, rhs);
}

Value Value::CastAs(SQLType source_type, SQLType target_type) {
	if (source_type == target_type) {
		return Copy();
	}
	Vector input, result;
	input.Reference(*this);
	result.Initialize(GetInternalType(target_type));
	VectorOperations::Cast(input, result, source_type, target_type);
	return result.GetValue(0);
}

Value Value::CastAs(TypeId target_type) const {
	if (target_type == type) {
		return Copy(); // in case of types that have no SQLType equivalent such as POINTER
	}
	return Copy().CastAs(SQLTypeFromInternalType(type), SQLTypeFromInternalType(target_type));
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
		case TypeId::FLOAT:
			serializer.Write<double>(value_.float_);
			break;
		case TypeId::DOUBLE:
			serializer.Write<double>(value_.double_);
			break;
		case TypeId::POINTER:
			serializer.Write<uintptr_t>(value_.pointer);
			break;
		case TypeId::VARCHAR:
			serializer.WriteString(str_value);
			break;
		default:
			throw NotImplementedException("Value type not implemented for serialization!");
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
	case TypeId::FLOAT:
		new_value.value_.float_ = source.Read<float>();
		break;
	case TypeId::DOUBLE:
		new_value.value_.double_ = source.Read<double>();
		break;
	case TypeId::POINTER:
		new_value.value_.pointer = source.Read<uint64_t>();
		break;
	case TypeId::VARCHAR:
		new_value.str_value = source.Read<string>();
		break;
	default:
		throw NotImplementedException("Value type not implemented for deserialization");
	}
	return new_value;
}

// adapted from MonetDB's str.c
bool Value::IsUTF8String(const char *s) {
	int c;

	if (s == NULL) {
		return true;
	}
	if (*s == '\200' && s[1] == '\0') {
		return true; /* str_nil */
	}
	while ((c = *s++) != '\0') {
		if ((c & 0x80) == 0)
			continue;
		if ((*s++ & 0xC0) != 0x80)
			return false;
		if ((c & 0xE0) == 0xC0)
			continue;
		if ((*s++ & 0xC0) != 0x80)
			return false;
		if ((c & 0xF0) == 0xE0)
			continue;
		if ((*s++ & 0xC0) != 0x80)
			return false;
		if ((c & 0xF8) == 0xF0)
			continue;
		return false;
	}
	return true;
}

void Value::Print() {
	Printer::Print(ToString());
}

bool Value::ValuesAreEqual(Value result_value, Value value) {
	if (result_value.is_null && value.is_null) {
		// NULL = NULL in checking code
		return true;
	}
	switch (value.type) {
	case TypeId::FLOAT: {
		float ldecimal = value.value_.float_;
		float rdecimal = result_value.value_.float_;
		return ApproxEqual(ldecimal, rdecimal);
	}
	case TypeId::DOUBLE: {
		double ldecimal = value.value_.double_;
		double rdecimal = result_value.value_.double_;
		return ApproxEqual(ldecimal, rdecimal);
	}
	case TypeId::VARCHAR: {
		// some results might contain padding spaces, e.g. when rendering
		// VARCHAR(10) and the string only has 6 characters, they will be padded
		// with spaces to 10 in the rendering. We don't do that here yet as we
		// are looking at internal structures. So just ignore any extra spaces
		// on the right
		string left = result_value.str_value;
		string right = value.str_value;
		StringUtil::RTrim(left);
		StringUtil::RTrim(right);
		return left == right;
	}
	default:
		return value == result_value;
	}
}
