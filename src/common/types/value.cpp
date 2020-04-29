#include "duckdb/common/types/value.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

#include "utf8proc_wrapper.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

Value::Value(string_t val) : Value(string(val.GetData(), val.GetSize())) {
}

Value::Value(string val) : type(TypeId::VARCHAR), is_null(false) {
	auto utf_type = Utf8Proc::Analyze(val);
	switch (utf_type) {
	case UnicodeType::INVALID:
		throw Exception("String value is not valid UTF8");
	case UnicodeType::ASCII:
		str_value = val;
		break;
	case UnicodeType::UNICODE:
		str_value = Utf8Proc::Normalize(val);
		break;
	}
}

Value Value::MinimumValue(TypeId type) {
	Value result;
	result.type = type;
	result.is_null = false;
	switch (type) {
	case TypeId::BOOL:
		result.value_.boolean = false;
		break;
	case TypeId::INT8:
		result.value_.tinyint = std::numeric_limits<int8_t>::min();
		break;
	case TypeId::INT16:
		result.value_.smallint = std::numeric_limits<int16_t>::min();
		break;
	case TypeId::INT32:
		result.value_.integer = std::numeric_limits<int32_t>::min();
		break;
	case TypeId::INT64:
		result.value_.bigint = std::numeric_limits<int64_t>::min();
		break;
	case TypeId::FLOAT:
		result.value_.float_ = std::numeric_limits<float>::min();
		break;
	case TypeId::DOUBLE:
		result.value_.double_ = std::numeric_limits<double>::min();
		break;
	case TypeId::POINTER:
		result.value_.pointer = std::numeric_limits<uintptr_t>::min();
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
	case TypeId::BOOL:
		result.value_.boolean = true;
		break;
	case TypeId::INT8:
		result.value_.tinyint = std::numeric_limits<int8_t>::max();
		break;
	case TypeId::INT16:
		result.value_.smallint = std::numeric_limits<int16_t>::max();
		break;
	case TypeId::INT32:
		result.value_.integer = std::numeric_limits<int32_t>::max();
		break;
	case TypeId::INT64:
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
	Value result(TypeId::BOOL);
	result.value_.boolean = value ? true : false;
	result.is_null = false;
	return result;
}

Value Value::TINYINT(int8_t value) {
	Value result(TypeId::INT8);
	result.value_.tinyint = value;
	result.is_null = false;
	return result;
}

Value Value::SMALLINT(int16_t value) {
	Value result(TypeId::INT16);
	result.value_.smallint = value;
	result.is_null = false;
	return result;
}

Value Value::INTEGER(int32_t value) {
	Value result(TypeId::INT32);
	result.value_.integer = value;
	result.is_null = false;
	return result;
}

Value Value::BIGINT(int64_t value) {
	Value result(TypeId::INT64);
	result.value_.bigint = value;
	result.is_null = false;
	return result;
}

Value Value::BLOB(string value) {
	Value result(TypeId::VARCHAR);
	result.str_value = value;
	result.is_null = false;
	result.sql_type = SQLType::VARBINARY;
	return result;
}

bool Value::FloatIsValid(float value) {
	return !(std::isnan(value) || std::isinf(value));
}

bool Value::DoubleIsValid(double value) {
	return !(std::isnan(value) || std::isinf(value));
}

Value Value::FLOAT(float value) {
	if (!Value::FloatIsValid(value)) {
		throw OutOfRangeException("Invalid float value %f", value);
	}
	Value result(TypeId::FLOAT);
	result.value_.float_ = value;
	result.is_null = false;
	return result;
}

Value Value::DOUBLE(double value) {
	if (!Value::DoubleIsValid(value)) {
		throw OutOfRangeException("Invalid double value %f", value);
	}
	Value result(TypeId::DOUBLE);
	result.value_.double_ = value;
	result.is_null = false;
	return result;
}

Value Value::HASH(hash_t value) {
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

Value Value::DATE(date_t date) {
	auto val = Value::INTEGER(date);
	val.sql_type = SQLType::DATE;
	return val;
}

Value Value::DATE(int32_t year, int32_t month, int32_t day) {
	auto val = Value::INTEGER(Date::FromDate(year, month, day));
	val.sql_type = SQLType::DATE;
	return val;
}

Value Value::TIME(int32_t hour, int32_t min, int32_t sec, int32_t msec) {
	auto val = Value::INTEGER(Time::FromTime(hour, min, sec, msec));
	val.sql_type = SQLType::TIME;
	return val;
}

Value Value::TIMESTAMP(timestamp_t timestamp) {
	auto val = Value::BIGINT(timestamp);
	val.sql_type = SQLType::TIMESTAMP;
	return val;
}

Value Value::TIMESTAMP(date_t date, dtime_t time) {
	auto val = Value::BIGINT(Timestamp::FromDatetime(date, time));
	val.sql_type = SQLType::TIMESTAMP;
	return val;
}

Value Value::TIMESTAMP(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec, int32_t msec) {
	auto val = Value::TIMESTAMP(Date::FromDate(year, month, day), Time::FromTime(hour, min, sec, msec));
	val.sql_type = SQLType::TIMESTAMP;
	return val;
}

Value Value::STRUCT(child_list_t<Value> values) {
	Value result(TypeId::STRUCT);
	result.struct_value = move(values);
	result.is_null = false;
	return result;
}

Value Value::LIST(vector<Value> values) {
	Value result(TypeId::LIST);
	result.list_value = move(values);
	result.is_null = false;
	return result;
}

//===--------------------------------------------------------------------===//
// CreateValue
//===--------------------------------------------------------------------===//
template <> Value Value::CreateValue(bool value) {
	return Value::BOOLEAN(value);
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

template <> Value Value::CreateValue(string_t value) {
	return Value(value);
}

template <> Value Value::CreateValue(float value) {
	return Value::FLOAT(value);
}

template <> Value Value::CreateValue(double value) {
	return Value::DOUBLE(value);
}

template <> Value Value::CreateValue(Value value) {
	return value;
}
//===--------------------------------------------------------------------===//
// GetValue
//===--------------------------------------------------------------------===//
template <class T> T Value::GetValueInternal() {
	if (is_null) {
		return NullValue<T>();
	}
	switch (type) {
	case TypeId::BOOL:
		return Cast::Operation<bool, T>(value_.boolean);
	case TypeId::INT8:
		return Cast::Operation<int8_t, T>(value_.tinyint);
	case TypeId::INT16:
		return Cast::Operation<int16_t, T>(value_.smallint);
	case TypeId::INT32:
		return Cast::Operation<int32_t, T>(value_.integer);
	case TypeId::INT64:
		return Cast::Operation<int64_t, T>(value_.bigint);
	case TypeId::FLOAT:
		return Cast::Operation<float, T>(value_.float_);
	case TypeId::DOUBLE:
		return Cast::Operation<double, T>(value_.double_);
	case TypeId::VARCHAR:
		return Cast::Operation<string_t, T>(str_value.c_str());
	default:
		throw NotImplementedException("Unimplemented type for GetValue()");
	}
}

template <> bool Value::GetValue() {
	return GetValueInternal<bool>();
}
template <> int8_t Value::GetValue() {
	return GetValueInternal<int8_t>();
}
template <> int16_t Value::GetValue() {
	return GetValueInternal<int16_t>();
}
template <> int32_t Value::GetValue() {
	return GetValueInternal<int32_t>();
}
template <> int64_t Value::GetValue() {
	return GetValueInternal<int64_t>();
}
template <> string Value::GetValue() {
	return GetValueInternal<string>();
}
template <> float Value::GetValue() {
	return GetValueInternal<float>();
}
template <> double Value::GetValue() {
	return GetValueInternal<double>();
}

Value Value::Numeric(TypeId type, int64_t value) {
	assert(!TypeIsIntegral(type) ||
	       (value >= duckdb::MinimumValue(type) && (value < 0 || (uint64_t)value <= duckdb::MaximumValue(type))));
	Value val(type);
	val.is_null = false;
	switch (type) {
	case TypeId::INT8:
		assert(value <= std::numeric_limits<int8_t>::max());
		return Value::TINYINT((int8_t)value);
	case TypeId::INT16:
		assert(value <= std::numeric_limits<int16_t>::max());
		return Value::SMALLINT((int16_t)value);
	case TypeId::INT32:
		assert(value <= std::numeric_limits<int32_t>::max());
		return Value::INTEGER((int32_t)value);
	case TypeId::INT64:
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

string Value::ToString(SQLType sql_type) const {
	if (is_null) {
		return "NULL";
	}
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
	case SQLTypeId::TIME:
		return Time::ToString(value_.integer);
	case SQLTypeId::TIMESTAMP:
		return Timestamp::ToString(value_.bigint);
	case SQLTypeId::VARCHAR:
		return str_value;
	case SQLTypeId::STRUCT: {
		string ret = "<";
		for (size_t i = 0; i < struct_value.size(); i++) {
			auto &child = struct_value[i];
			ret += child.first + ": " + child.second.ToString();
			if (i < struct_value.size() - 1) {
				ret += ", ";
			}
		}
		ret += ">";
		return ret;
	}
	case SQLTypeId::LIST: {
		string ret = "[";
		for (size_t i = 0; i < list_value.size(); i++) {
			auto &child = list_value[i];
			ret += child.ToString();
			if (i < list_value.size() - 1) {
				ret += ", ";
			}
		}
		ret += "]";
		return ret;
	}
	default:
		throw NotImplementedException("Unimplemented type for printing");
	}
}

string Value::ToString() const {
	switch (type) {
	case TypeId::POINTER:
		return to_string(value_.pointer);
	case TypeId::HASH:
		return to_string(value_.hash);
	default:
		return ToString(SQLTypeFromInternalType(type));
	}
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

Value Value::CastAs(SQLType source_type, SQLType target_type, bool strict) {
	if (source_type == target_type) {
		return Copy();
	}
	Vector input, result;
	input.Reference(*this);
	result.Initialize(GetInternalType(target_type));
	VectorOperations::Cast(input, result, source_type, target_type, 1, strict);
	return result.GetValue(0);
}

Value Value::CastAs(TypeId target_type, bool strict) const {
	if (target_type == type) {
		return Copy(); // in case of types that have no SQLType equivalent such as POINTER
	}
	return Copy().CastAs(SQLTypeFromInternalType(type), SQLTypeFromInternalType(target_type), strict);
}

bool Value::TryCastAs(SQLType source_type, SQLType target_type, bool strict) {
	Value new_value;
	try {
		new_value = CastAs(source_type, target_type, strict);
	} catch (Exception &) {
		return false;
	}
	type = new_value.type;
	is_null = new_value.is_null;
	value_ = new_value.value_;
	str_value = new_value.str_value;
	struct_value = new_value.struct_value;
	list_value = new_value.list_value;
	return true;
}

void Value::Serialize(Serializer &serializer) {
	serializer.Write<TypeId>(type);
	serializer.Write<bool>(is_null);
	if (!is_null) {
		switch (type) {
		case TypeId::BOOL:
			serializer.Write<int8_t>(value_.boolean);
			break;
		case TypeId::INT8:
			serializer.Write<int8_t>(value_.tinyint);
			break;
		case TypeId::INT16:
			serializer.Write<int16_t>(value_.smallint);
			break;
		case TypeId::INT32:
			serializer.Write<int32_t>(value_.integer);
			break;
		case TypeId::INT64:
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
	case TypeId::BOOL:
		new_value.value_.boolean = source.Read<int8_t>();
		break;
	case TypeId::INT8:
		new_value.value_.tinyint = source.Read<int8_t>();
		break;
	case TypeId::INT16:
		new_value.value_.smallint = source.Read<int16_t>();
		break;
	case TypeId::INT32:
		new_value.value_.integer = source.Read<int32_t>();
		break;
	case TypeId::INT64:
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
		auto other = result_value.CastAs(TypeId::FLOAT);
		float ldecimal = value.value_.float_;
		float rdecimal = other.value_.float_;
		return ApproxEqual(ldecimal, rdecimal);
	}
	case TypeId::DOUBLE: {
		auto other = result_value.CastAs(TypeId::DOUBLE);
		double ldecimal = value.value_.double_;
		double rdecimal = other.value_.double_;
		return ApproxEqual(ldecimal, rdecimal);
	}
	case TypeId::VARCHAR: {
		auto other = result_value.CastAs(TypeId::VARCHAR);
		// some results might contain padding spaces, e.g. when rendering
		// VARCHAR(10) and the string only has 6 characters, they will be padded
		// with spaces to 10 in the rendering. We don't do that here yet as we
		// are looking at internal structures. So just ignore any extra spaces
		// on the right
		string left = other.str_value;
		string right = value.str_value;
		StringUtil::RTrim(left);
		StringUtil::RTrim(right);
		return left == right;
	}
	default:
		return value == result_value;
	}
}
