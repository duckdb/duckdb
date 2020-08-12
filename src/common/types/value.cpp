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
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {
using namespace std;

Value::Value(string_t val) : Value(string(val.GetData(), val.GetSize())) {
}

Value::Value(string val) : type(PhysicalType::VARCHAR), is_null(false) {
	sql_type = LogicalType::VARCHAR;
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

Value Value::MinimumValue(PhysicalType type) {
	Value result;
	result.type = type;
	result.is_null = false;
	switch (type) {
	case PhysicalType::BOOL:
		result.value_.boolean = false;
		break;
	case PhysicalType::INT8:
		result.value_.tinyint = NumericLimits<int8_t>::Minimum();
		break;
	case PhysicalType::INT16:
		result.value_.smallint = NumericLimits<int16_t>::Minimum();
		break;
	case PhysicalType::INT32:
		result.value_.integer = NumericLimits<int32_t>::Minimum();
		break;
	case PhysicalType::INT64:
		result.value_.bigint = NumericLimits<int64_t>::Minimum();
		break;
	case PhysicalType::INT128:
		result.value_.hugeint.lower = 0;
		result.value_.hugeint.upper = NumericLimits<int64_t>::Minimum();
		break;
	case PhysicalType::FLOAT:
		result.value_.float_ = NumericLimits<float>::Minimum();
		break;
	case PhysicalType::DOUBLE:
		result.value_.double_ = NumericLimits<double>::Minimum();
		break;
	default:
		throw InvalidTypeException(type, "MinimumValue requires numeric type");
	}
	return result;
}

Value Value::MaximumValue(PhysicalType type) {
	Value result;
	result.type = type;
	result.is_null = false;
	switch (type) {
	case PhysicalType::BOOL:
		result.value_.boolean = true;
		break;
	case PhysicalType::INT8:
		result.value_.tinyint = NumericLimits<int8_t>::Maximum();
		break;
	case PhysicalType::INT16:
		result.value_.smallint = NumericLimits<int16_t>::Maximum();
		break;
	case PhysicalType::INT32:
		result.value_.integer = NumericLimits<int32_t>::Maximum();
		break;
	case PhysicalType::INT64:
		result.value_.bigint = NumericLimits<int64_t>::Maximum();
		break;
	case PhysicalType::INT128:
		result.value_.hugeint = NumericLimits<hugeint_t>::Maximum();
		break;
	case PhysicalType::FLOAT:
		result.value_.float_ = NumericLimits<float>::Maximum();
		break;
	case PhysicalType::DOUBLE:
		result.value_.double_ = NumericLimits<double>::Maximum();
		break;
	default:
		throw InvalidTypeException(type, "MaximumValue requires numeric type");
	}
	return result;
}

Value Value::BOOLEAN(int8_t value) {
	Value result(PhysicalType::BOOL);
	result.value_.boolean = value ? true : false;
	result.is_null = false;
	return result;
}

Value Value::TINYINT(int8_t value) {
	Value result(PhysicalType::INT8);
	result.value_.tinyint = value;
	result.is_null = false;
	return result;
}

Value Value::SMALLINT(int16_t value) {
	Value result(PhysicalType::INT16);
	result.value_.smallint = value;
	result.is_null = false;
	return result;
}

Value Value::INTEGER(int32_t value) {
	Value result(PhysicalType::INT32);
	result.value_.integer = value;
	result.is_null = false;
	return result;
}

Value Value::BIGINT(int64_t value) {
	Value result(PhysicalType::INT64);
	result.value_.bigint = value;
	result.is_null = false;
	return result;
}

Value Value::HUGEINT(hugeint_t value) {
	Value result(PhysicalType::INT128);
	result.value_.hugeint = value;
	result.is_null = false;
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
	Value result(PhysicalType::FLOAT);
	result.value_.float_ = value;
	result.is_null = false;
	return result;
}

Value Value::DOUBLE(double value) {
	if (!Value::DoubleIsValid(value)) {
		throw OutOfRangeException("Invalid double value %f", value);
	}
	Value result(PhysicalType::DOUBLE);
	result.value_.double_ = value;
	result.is_null = false;
	return result;
}

Value Value::HASH(hash_t value) {
	Value result(PhysicalType::HASH);
	result.value_.hash = value;
	result.is_null = false;
	return result;
}

Value Value::POINTER(uintptr_t value) {
	Value result(PhysicalType::POINTER);
	result.value_.pointer = value;
	result.is_null = false;
	return result;
}

Value Value::DATE(date_t date) {
	auto val = Value::INTEGER(date);
	val.sql_type = LogicalType::DATE;
	return val;
}

Value Value::DATE(int32_t year, int32_t month, int32_t day) {
	auto val = Value::INTEGER(Date::FromDate(year, month, day));
	val.sql_type = LogicalType::DATE;
	return val;
}

Value Value::TIME(int32_t hour, int32_t min, int32_t sec, int32_t msec) {
	auto val = Value::INTEGER(Time::FromTime(hour, min, sec, msec));
	val.sql_type = LogicalType::TIME;
	return val;
}

Value Value::TIMESTAMP(timestamp_t timestamp) {
	auto val = Value::BIGINT(timestamp);
	val.sql_type = LogicalType::TIMESTAMP;
	return val;
}

Value Value::TIMESTAMP(date_t date, dtime_t time) {
	auto val = Value::BIGINT(Timestamp::FromDatetime(date, time));
	val.sql_type = LogicalType::TIMESTAMP;
	return val;
}

Value Value::TIMESTAMP(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec, int32_t msec) {
	auto val = Value::TIMESTAMP(Date::FromDate(year, month, day), Time::FromTime(hour, min, sec, msec));
	val.sql_type = LogicalType::TIMESTAMP;
	return val;
}

Value Value::STRUCT(child_list_t<Value> values) {
	Value result(PhysicalType::STRUCT);
	result.struct_value = move(values);
	result.is_null = false;
	return result;
}

Value Value::LIST(vector<Value> values) {
	Value result(PhysicalType::LIST);
	result.list_value = move(values);
	result.is_null = false;
	return result;
}

Value Value::BLOB(string data, bool must_cast) {
	Value result(PhysicalType::VARCHAR);
	result.sql_type = LogicalType::BLOB;
	result.is_null = false;
	// hex string identifier: "\\x", must be double '\'
	// single '\x' is a special char for hex chars in C++,
	// e.g., '\xAA' will be transformed into the char "ª" (1010 1010),
	// and Postgres uses double "\\x" for hex -> SELECT E'\\xDEADBEEF';
	if (must_cast && data.size() >= 2 && data.substr(0, 2) == "\\x") {
		size_t hex_size = (data.size() - 2) / 2;
		unique_ptr<char[]> hex_data(new char[hex_size + 1]);
		string_t hex_str(hex_data.get(), hex_size);
		CastFromBlob::FromHexToBytes(string_t(data), hex_str);
		result.str_value = string(hex_str.GetData());
	} else {
		// raw string
		result.str_value = data;
	}
	return result;
}

Value Value::INTERVAL(int32_t months, int32_t days, int64_t msecs) {
	Value result(PhysicalType::INTERVAL);
	result.is_null = false;
	result.value_.interval.months = months;
	result.value_.interval.days = days;
	result.value_.interval.msecs = msecs;
	return result;
}

Value Value::INTERVAL(interval_t interval) {
	return Value::INTERVAL(interval.months, interval.days, interval.msecs);
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
	return Value::BLOB(value);
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
	case PhysicalType::BOOL:
		return Cast::Operation<bool, T>(value_.boolean);
	case PhysicalType::INT8:
		return Cast::Operation<int8_t, T>(value_.tinyint);
	case PhysicalType::INT16:
		return Cast::Operation<int16_t, T>(value_.smallint);
	case PhysicalType::INT32:
		return Cast::Operation<int32_t, T>(value_.integer);
	case PhysicalType::INT64:
		return Cast::Operation<int64_t, T>(value_.bigint);
	case PhysicalType::INT128:
		return Cast::Operation<hugeint_t, T>(value_.hugeint);
	case PhysicalType::FLOAT:
		return Cast::Operation<float, T>(value_.float_);
	case PhysicalType::DOUBLE:
		return Cast::Operation<double, T>(value_.double_);
	case PhysicalType::VARCHAR:
		return Cast::Operation<string_t, T>(str_value.c_str());
	default:
		throw NotImplementedException("Unimplemented type for GetValue()");
	}
}

template <> bool Value::GetValue() {
	return GetValueInternal<int8_t>();
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

Value Value::Numeric(PhysicalType type, int64_t value) {
	Value val(type);
	val.is_null = false;
	switch (type) {
	case PhysicalType::INT8:
		assert(value <= NumericLimits<int8_t>::Maximum());
		return Value::TINYINT((int8_t)value);
	case PhysicalType::INT16:
		assert(value <= NumericLimits<int16_t>::Maximum());
		return Value::SMALLINT((int16_t)value);
	case PhysicalType::INT32:
		assert(value <= NumericLimits<int32_t>::Maximum());
		return Value::INTEGER((int32_t)value);
	case PhysicalType::INT64:
		return Value::BIGINT(value);
	case PhysicalType::INT128:
		return Value::HUGEINT(value);
	case PhysicalType::FLOAT:
		return Value((float)value);
	case PhysicalType::DOUBLE:
		return Value((double)value);
	case PhysicalType::HASH:
		return Value::HASH(value);
	case PhysicalType::POINTER:
		return Value::POINTER(value);
	default:
		throw InvalidTypeException(type, "Numeric requires numeric type");
	}
	return val;
}

string Value::ToString(LogicalType sql_type) const {
	if (is_null) {
		return "NULL";
	}
	switch (sql_type.id()) {
	case LogicalTypeId::BOOLEAN:
		return value_.boolean ? "True" : "False";
	case LogicalTypeId::TINYINT:
		return to_string(value_.tinyint);
	case LogicalTypeId::SMALLINT:
		return to_string(value_.smallint);
	case LogicalTypeId::INTEGER:
		return to_string(value_.integer);
	case LogicalTypeId::BIGINT:
		return to_string(value_.bigint);
	case LogicalTypeId::HUGEINT:
		return Hugeint::ToString(value_.hugeint);
	case LogicalTypeId::FLOAT:
		return to_string(value_.float_);
	case LogicalTypeId::DOUBLE:
		return to_string(value_.double_);
	case LogicalTypeId::DATE:
		return Date::ToString(value_.integer);
	case LogicalTypeId::TIME:
		return Time::ToString(value_.integer);
	case LogicalTypeId::TIMESTAMP:
		return Timestamp::ToString(value_.bigint);
	case LogicalTypeId::INTERVAL:
		return Interval::ToString(value_.interval);
	case LogicalTypeId::VARCHAR:
		return str_value;
	case LogicalTypeId::BLOB: {
		unique_ptr<char[]> hex_data(new char[str_value.size() * 2 + 2 + 1]);
		string_t hex_str(hex_data.get(), str_value.size() * 2 + 2);
		CastFromBlob::ToHexString(string_t(str_value), hex_str);
		string result(hex_str.GetData());
		return result;
	}
	case LogicalTypeId::STRUCT: {
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
	case LogicalTypeId::LIST: {
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
		throw NotImplementedException("Unimplemented type for printing: %s", sql_type.ToString().c_str());
	}
}

string Value::ToString() const {
	switch (type) {
	case PhysicalType::POINTER:
		return to_string(value_.pointer);
	case PhysicalType::HASH:
		return to_string(value_.hash);
	default:
		return ToString(LogicalTypeFromInternalType(type));
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

Value Value::CastAs(LogicalType source_type, LogicalType target_type, bool strict) {
	if (source_type == target_type) {
		return Copy();
	}
	Vector input, result;
	input.Reference(*this);
	result.Initialize(GetInternalType(target_type));
	VectorOperations::Cast(input, result, source_type, target_type, 1, strict);
	return result.GetValue(0);
}

Value Value::CastAs(PhysicalType target_type, bool strict) const {
	if (target_type == type) {
		return Copy(); // in case of types that have no LogicalType equivalent such as POINTER
	}
	return Copy().CastAs(LogicalTypeFromInternalType(type), LogicalTypeFromInternalType(target_type), strict);
}

bool Value::TryCastAs(LogicalType source_type, LogicalType target_type, bool strict) {
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
	serializer.Write<PhysicalType>(type);
	serializer.Write<bool>(is_null);
	if (!is_null) {
		switch (type) {
		case PhysicalType::BOOL:
			serializer.Write<int8_t>(value_.boolean);
			break;
		case PhysicalType::INT8:
			serializer.Write<int8_t>(value_.tinyint);
			break;
		case PhysicalType::INT16:
			serializer.Write<int16_t>(value_.smallint);
			break;
		case PhysicalType::INT32:
			serializer.Write<int32_t>(value_.integer);
			break;
		case PhysicalType::INT64:
			serializer.Write<int64_t>(value_.bigint);
			break;
		case PhysicalType::INT128:
			serializer.Write<hugeint_t>(value_.hugeint);
			break;
		case PhysicalType::FLOAT:
			serializer.Write<double>(value_.float_);
			break;
		case PhysicalType::DOUBLE:
			serializer.Write<double>(value_.double_);
			break;
		case PhysicalType::POINTER:
			serializer.Write<uintptr_t>(value_.pointer);
			break;
		case PhysicalType::INTERVAL:
			serializer.Write<interval_t>(value_.interval);
			break;
		case PhysicalType::VARCHAR:
			serializer.WriteString(str_value);
			break;
		default:
			throw NotImplementedException("Value type not implemented for serialization!");
		}
	}
}

Value Value::Deserialize(Deserializer &source) {
	auto type = source.Read<PhysicalType>();
	auto is_null = source.Read<bool>();
	Value new_value = Value(type);
	if (is_null) {
		return new_value;
	}
	new_value.is_null = false;
	switch (type) {
	case PhysicalType::BOOL:
		new_value.value_.boolean = source.Read<int8_t>();
		break;
	case PhysicalType::INT8:
		new_value.value_.tinyint = source.Read<int8_t>();
		break;
	case PhysicalType::INT16:
		new_value.value_.smallint = source.Read<int16_t>();
		break;
	case PhysicalType::INT32:
		new_value.value_.integer = source.Read<int32_t>();
		break;
	case PhysicalType::INT64:
		new_value.value_.bigint = source.Read<int64_t>();
		break;
	case PhysicalType::INT128:
		new_value.value_.hugeint = source.Read<hugeint_t>();
		break;
	case PhysicalType::FLOAT:
		new_value.value_.float_ = source.Read<float>();
		break;
	case PhysicalType::DOUBLE:
		new_value.value_.double_ = source.Read<double>();
		break;
	case PhysicalType::POINTER:
		new_value.value_.pointer = source.Read<uint64_t>();
		break;
	case PhysicalType::INTERVAL:
		new_value.value_.interval = source.Read<interval_t>();
		break;
	case PhysicalType::VARCHAR:
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
	case PhysicalType::FLOAT: {
		auto other = result_value.CastAs(PhysicalType::FLOAT);
		float ldecimal = value.value_.float_;
		float rdecimal = other.value_.float_;
		return ApproxEqual(ldecimal, rdecimal);
	}
	case PhysicalType::DOUBLE: {
		auto other = result_value.CastAs(PhysicalType::DOUBLE);
		double ldecimal = value.value_.double_;
		double rdecimal = other.value_.double_;
		return ApproxEqual(ldecimal, rdecimal);
	}
	case PhysicalType::VARCHAR: {
		auto other = result_value.CastAs(PhysicalType::VARCHAR);
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

} // namespace duckdb
