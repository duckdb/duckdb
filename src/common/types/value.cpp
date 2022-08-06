#include "duckdb/common/types/value.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

#include "utf8proc_wrapper.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/hash.hpp"

#include <utility>
#include <cmath>

namespace duckdb {

Value::Value(LogicalType type) : type_(move(type)), is_null(true) {
}

Value::Value(int32_t val) : type_(LogicalType::INTEGER), is_null(false) {
	value_.integer = val;
}

Value::Value(int64_t val) : type_(LogicalType::BIGINT), is_null(false) {
	value_.bigint = val;
}

Value::Value(float val) : type_(LogicalType::FLOAT), is_null(false) {
	value_.float_ = val;
}

Value::Value(double val) : type_(LogicalType::DOUBLE), is_null(false) {
	value_.double_ = val;
}

Value::Value(const char *val) : Value(val ? string(val) : string()) {
}

Value::Value(std::nullptr_t val) : Value(LogicalType::VARCHAR) {
}

Value::Value(string_t val) : Value(string(val.GetDataUnsafe(), val.GetSize())) {
}

Value::Value(string val) : type_(LogicalType::VARCHAR), is_null(false), str_value(move(val)) {
	if (!Value::StringIsValid(str_value.c_str(), str_value.size())) {
		throw Exception("String value is not valid UTF8");
	}
}

Value::~Value() {
}

Value::Value(const Value &other)
    : type_(other.type_), is_null(other.is_null), value_(other.value_), str_value(other.str_value),
      struct_value(other.struct_value), list_value(other.list_value) {
}

Value::Value(Value &&other) noexcept
    : type_(move(other.type_)), is_null(other.is_null), value_(other.value_), str_value(move(other.str_value)),
      struct_value(move(other.struct_value)), list_value(move(other.list_value)) {
}

Value &Value::operator=(const Value &other) {
	type_ = other.type_;
	is_null = other.is_null;
	value_ = other.value_;
	str_value = other.str_value;
	struct_value = other.struct_value;
	list_value = other.list_value;
	return *this;
}

Value &Value::operator=(Value &&other) noexcept {
	type_ = move(other.type_);
	is_null = other.is_null;
	value_ = other.value_;
	str_value = move(other.str_value);
	struct_value = move(other.struct_value);
	list_value = move(other.list_value);
	return *this;
}

Value Value::MinimumValue(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(false);
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(NumericLimits<int8_t>::Minimum());
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(NumericLimits<int16_t>::Minimum());
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::SQLNULL:
		return Value::INTEGER(NumericLimits<int32_t>::Minimum());
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(NumericLimits<int64_t>::Minimum());
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(NumericLimits<hugeint_t>::Minimum());
	case LogicalTypeId::UUID:
		return Value::UUID(NumericLimits<hugeint_t>::Minimum());
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(NumericLimits<uint8_t>::Minimum());
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(NumericLimits<uint16_t>::Minimum());
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(NumericLimits<uint32_t>::Minimum());
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(NumericLimits<uint64_t>::Minimum());
	case LogicalTypeId::DATE:
		return Value::DATE(Date::FromDate(Date::DATE_MIN_YEAR, Date::DATE_MIN_MONTH, Date::DATE_MIN_DAY));
	case LogicalTypeId::TIME:
		return Value::TIME(dtime_t(0));
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(Date::FromDate(Timestamp::MIN_YEAR, Timestamp::MIN_MONTH, Timestamp::MIN_DAY),
		                        dtime_t(0));
	case LogicalTypeId::TIMESTAMP_SEC:
		return MinimumValue(LogicalType::TIMESTAMP).CastAs(LogicalType::TIMESTAMP_S);
	case LogicalTypeId::TIMESTAMP_MS:
		return MinimumValue(LogicalType::TIMESTAMP).CastAs(LogicalType::TIMESTAMP_MS);
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(timestamp_t(NumericLimits<int64_t>::Minimum()));
	case LogicalTypeId::TIME_TZ:
		return Value::TIMETZ(dtime_t(0));
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(Timestamp::FromDatetime(
		    Date::FromDate(Timestamp::MIN_YEAR, Timestamp::MIN_MONTH, Timestamp::MIN_DAY), dtime_t(0)));
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(NumericLimits<float>::Minimum());
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(NumericLimits<double>::Minimum());
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(int16_t(-NumericHelper::POWERS_OF_TEN[width] + 1), width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(int32_t(-NumericHelper::POWERS_OF_TEN[width] + 1), width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(int64_t(-NumericHelper::POWERS_OF_TEN[width] + 1), width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(-Hugeint::POWERS_OF_TEN[width] + 1, width, scale);
		default:
			throw InternalException("Unknown decimal type");
		}
	}
	case LogicalTypeId::ENUM:
		return Value::ENUM(0, type);
	default:
		throw InvalidTypeException(type, "MinimumValue requires numeric type");
	}
}

Value Value::MaximumValue(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(true);
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(NumericLimits<int8_t>::Maximum());
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(NumericLimits<int16_t>::Maximum());
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::SQLNULL:
		return Value::INTEGER(NumericLimits<int32_t>::Maximum());
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(NumericLimits<int64_t>::Maximum());
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(NumericLimits<hugeint_t>::Maximum());
	case LogicalTypeId::UUID:
		return Value::UUID(NumericLimits<hugeint_t>::Maximum());
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(NumericLimits<uint8_t>::Maximum());
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(NumericLimits<uint16_t>::Maximum());
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(NumericLimits<uint32_t>::Maximum());
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(NumericLimits<uint64_t>::Maximum());
	case LogicalTypeId::DATE:
		return Value::DATE(Date::FromDate(Date::DATE_MAX_YEAR, Date::DATE_MAX_MONTH, Date::DATE_MAX_DAY));
	case LogicalTypeId::TIME:
		return Value::TIME(dtime_t(Interval::SECS_PER_DAY * Interval::MICROS_PER_SEC - 1));
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(timestamp_t(NumericLimits<int64_t>::Maximum() - 1));
	case LogicalTypeId::TIMESTAMP_MS:
		return MaximumValue(LogicalType::TIMESTAMP).CastAs(LogicalType::TIMESTAMP_MS);
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(timestamp_t(NumericLimits<int64_t>::Maximum() - 1));
	case LogicalTypeId::TIMESTAMP_SEC:
		return MaximumValue(LogicalType::TIMESTAMP).CastAs(LogicalType::TIMESTAMP_S);
	case LogicalTypeId::TIME_TZ:
		return Value::TIMETZ(dtime_t(Interval::SECS_PER_DAY * Interval::MICROS_PER_SEC - 1));
	case LogicalTypeId::TIMESTAMP_TZ:
		return MaximumValue(LogicalType::TIMESTAMP);
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(NumericLimits<float>::Maximum());
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(NumericLimits<double>::Maximum());
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(int16_t(NumericHelper::POWERS_OF_TEN[width] - 1), width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(int32_t(NumericHelper::POWERS_OF_TEN[width] - 1), width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(int64_t(NumericHelper::POWERS_OF_TEN[width] - 1), width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(Hugeint::POWERS_OF_TEN[width] - 1, width, scale);
		default:
			throw InternalException("Unknown decimal type");
		}
	}
	case LogicalTypeId::ENUM:
		return Value::ENUM(EnumType::GetSize(type) - 1, type);
	default:
		throw InvalidTypeException(type, "MaximumValue requires numeric type");
	}
}

Value Value::BOOLEAN(int8_t value) {
	Value result(LogicalType::BOOLEAN);
	result.value_.boolean = value ? true : false;
	result.is_null = false;
	return result;
}

Value Value::TINYINT(int8_t value) {
	Value result(LogicalType::TINYINT);
	result.value_.tinyint = value;
	result.is_null = false;
	return result;
}

Value Value::SMALLINT(int16_t value) {
	Value result(LogicalType::SMALLINT);
	result.value_.smallint = value;
	result.is_null = false;
	return result;
}

Value Value::INTEGER(int32_t value) {
	Value result(LogicalType::INTEGER);
	result.value_.integer = value;
	result.is_null = false;
	return result;
}

Value Value::BIGINT(int64_t value) {
	Value result(LogicalType::BIGINT);
	result.value_.bigint = value;
	result.is_null = false;
	return result;
}

Value Value::HUGEINT(hugeint_t value) {
	Value result(LogicalType::HUGEINT);
	result.value_.hugeint = value;
	result.is_null = false;
	return result;
}

Value Value::UUID(hugeint_t value) {
	Value result(LogicalType::UUID);
	result.value_.hugeint = value;
	result.is_null = false;
	return result;
}

Value Value::UUID(const string &value) {
	Value result(LogicalType::UUID);
	result.value_.hugeint = UUID::FromString(value);
	result.is_null = false;
	return result;
}

Value Value::UTINYINT(uint8_t value) {
	Value result(LogicalType::UTINYINT);
	result.value_.utinyint = value;
	result.is_null = false;
	return result;
}

Value Value::USMALLINT(uint16_t value) {
	Value result(LogicalType::USMALLINT);
	result.value_.usmallint = value;
	result.is_null = false;
	return result;
}

Value Value::UINTEGER(uint32_t value) {
	Value result(LogicalType::UINTEGER);
	result.value_.uinteger = value;
	result.is_null = false;
	return result;
}

Value Value::UBIGINT(uint64_t value) {
	Value result(LogicalType::UBIGINT);
	result.value_.ubigint = value;
	result.is_null = false;
	return result;
}

bool Value::FloatIsFinite(float value) {
	return !(std::isnan(value) || std::isinf(value));
}

bool Value::DoubleIsFinite(double value) {
	return !(std::isnan(value) || std::isinf(value));
}

template <>
bool Value::IsNan(float input) {
	return std::isnan(input);
}

template <>
bool Value::IsNan(double input) {
	return std::isnan(input);
}

template <>
bool Value::IsFinite(float input) {
	return Value::FloatIsFinite(input);
}

template <>
bool Value::IsFinite(double input) {
	return Value::DoubleIsFinite(input);
}

template <>
bool Value::IsFinite(date_t input) {
	return Date::IsFinite(input);
}

template <>
bool Value::IsFinite(timestamp_t input) {
	return Timestamp::IsFinite(input);
}

bool Value::StringIsValid(const char *str, idx_t length) {
	auto utf_type = Utf8Proc::Analyze(str, length);
	return utf_type != UnicodeType::INVALID;
}

Value Value::DECIMAL(int16_t value, uint8_t width, uint8_t scale) {
	D_ASSERT(width <= Decimal::MAX_WIDTH_INT16);
	Value result(LogicalType::DECIMAL(width, scale));
	result.value_.smallint = value;
	result.is_null = false;
	return result;
}

Value Value::DECIMAL(int32_t value, uint8_t width, uint8_t scale) {
	D_ASSERT(width >= Decimal::MAX_WIDTH_INT16 && width <= Decimal::MAX_WIDTH_INT32);
	Value result(LogicalType::DECIMAL(width, scale));
	result.value_.integer = value;
	result.is_null = false;
	return result;
}

Value Value::DECIMAL(int64_t value, uint8_t width, uint8_t scale) {
	auto decimal_type = LogicalType::DECIMAL(width, scale);
	Value result(decimal_type);
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		result.value_.smallint = value;
		break;
	case PhysicalType::INT32:
		result.value_.integer = value;
		break;
	case PhysicalType::INT64:
		result.value_.bigint = value;
		break;
	default:
		result.value_.hugeint = value;
		break;
	}
	result.type_.Verify();
	result.is_null = false;
	return result;
}

Value Value::DECIMAL(hugeint_t value, uint8_t width, uint8_t scale) {
	D_ASSERT(width >= Decimal::MAX_WIDTH_INT64 && width <= Decimal::MAX_WIDTH_INT128);
	Value result(LogicalType::DECIMAL(width, scale));
	result.value_.hugeint = value;
	result.is_null = false;
	return result;
}

Value Value::FLOAT(float value) {
	Value result(LogicalType::FLOAT);
	result.value_.float_ = value;
	result.is_null = false;
	return result;
}

Value Value::DOUBLE(double value) {
	Value result(LogicalType::DOUBLE);
	result.value_.double_ = value;
	result.is_null = false;
	return result;
}

Value Value::HASH(hash_t value) {
	Value result(LogicalType::HASH);
	result.value_.hash = value;
	result.is_null = false;
	return result;
}

Value Value::POINTER(uintptr_t value) {
	Value result(LogicalType::POINTER);
	result.value_.pointer = value;
	result.is_null = false;
	return result;
}

Value Value::DATE(date_t value) {
	Value result(LogicalType::DATE);
	result.value_.date = value;
	result.is_null = false;
	return result;
}

Value Value::DATE(int32_t year, int32_t month, int32_t day) {
	return Value::DATE(Date::FromDate(year, month, day));
}

Value Value::TIME(dtime_t value) {
	Value result(LogicalType::TIME);
	result.value_.time = value;
	result.is_null = false;
	return result;
}

Value Value::TIMETZ(dtime_t value) {
	Value result(LogicalType::TIME_TZ);
	result.value_.time = value;
	result.is_null = false;
	return result;
}

Value Value::TIME(int32_t hour, int32_t min, int32_t sec, int32_t micros) {
	return Value::TIME(Time::FromTime(hour, min, sec, micros));
}

Value Value::TIMESTAMP(timestamp_t value) {
	Value result(LogicalType::TIMESTAMP);
	result.value_.timestamp = value;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMPTZ(timestamp_t value) {
	Value result(LogicalType::TIMESTAMP_TZ);
	result.value_.timestamp = value;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMPNS(timestamp_t timestamp) {
	Value result(LogicalType::TIMESTAMP_NS);
	result.value_.timestamp = timestamp;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMPMS(timestamp_t timestamp) {
	Value result(LogicalType::TIMESTAMP_MS);
	result.value_.timestamp = timestamp;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMPSEC(timestamp_t timestamp) {
	Value result(LogicalType::TIMESTAMP_S);
	result.value_.timestamp = timestamp;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMP(date_t date, dtime_t time) {
	return Value::TIMESTAMP(Timestamp::FromDatetime(date, time));
}

Value Value::TIMESTAMP(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec,
                       int32_t micros) {
	auto val = Value::TIMESTAMP(Date::FromDate(year, month, day), Time::FromTime(hour, min, sec, micros));
	val.type_ = LogicalType::TIMESTAMP;
	return val;
}

Value Value::STRUCT(child_list_t<Value> values) {
	Value result;
	child_list_t<LogicalType> child_types;
	for (auto &child : values) {
		child_types.push_back(make_pair(move(child.first), child.second.type()));
		result.struct_value.push_back(move(child.second));
	}
	result.type_ = LogicalType::STRUCT(move(child_types));

	result.is_null = false;
	return result;
}

Value Value::MAP(Value key, Value value) {
	Value result;
	child_list_t<LogicalType> child_types;
	child_types.push_back({"key", key.type()});
	child_types.push_back({"value", value.type()});

	result.type_ = LogicalType::MAP(move(child_types));

	result.struct_value.push_back(move(key));
	result.struct_value.push_back(move(value));
	result.is_null = false;
	return result;
}

Value Value::LIST(vector<Value> values) {
	if (values.empty()) {
		throw InternalException("Value::LIST requires a non-empty list of values. Use Value::EMPTYLIST instead.");
	}
#ifdef DEBUG
	for (idx_t i = 1; i < values.size(); i++) {
		D_ASSERT(values[i].type() == values[0].type());
	}
#endif
	Value result;
	result.type_ = LogicalType::LIST(values[0].type());
	result.list_value = move(values);
	result.is_null = false;
	return result;
}

Value Value::LIST(LogicalType child_type, vector<Value> values) {
	if (values.empty()) {
		return Value::EMPTYLIST(move(child_type));
	}
	for (auto &val : values) {
		val = val.CastAs(child_type);
	}
	return Value::LIST(move(values));
}

Value Value::EMPTYLIST(LogicalType child_type) {
	Value result;
	result.type_ = LogicalType::LIST(move(child_type));
	result.is_null = false;
	return result;
}

Value Value::BLOB(const_data_ptr_t data, idx_t len) {
	Value result(LogicalType::BLOB);
	result.is_null = false;
	result.str_value = string((const char *)data, len);
	return result;
}

Value Value::BLOB(const string &data) {
	Value result(LogicalType::BLOB);
	result.is_null = false;
	result.str_value = Blob::ToBlob(string_t(data));
	return result;
}

Value Value::JSON(const char *val) {
	auto result = Value(val);
	result.type_ = LogicalTypeId::JSON;
	return result;
}

Value Value::JSON(string_t val) {
	auto result = Value(val);
	result.type_ = LogicalTypeId::JSON;
	return result;
}

Value Value::JSON(string val) {
	auto result = Value(move(val));
	result.type_ = LogicalTypeId::JSON;
	return result;
}

Value Value::ENUM(uint64_t value, const LogicalType &original_type) {
	D_ASSERT(original_type.id() == LogicalTypeId::ENUM);
	Value result(original_type);
	switch (original_type.InternalType()) {
	case PhysicalType::UINT8:
		result.value_.utinyint = value;
		break;
	case PhysicalType::UINT16:
		result.value_.usmallint = value;
		break;
	case PhysicalType::UINT32:
		result.value_.uinteger = value;
		break;
	case PhysicalType::UINT64: //  DEDUP_POINTER_ENUM
		result.value_.ubigint = value;
		break;
	default:
		throw InternalException("Incorrect Physical Type for ENUM");
	}
	result.is_null = false;
	return result;
}

Value Value::INTERVAL(int32_t months, int32_t days, int64_t micros) {
	Value result(LogicalType::INTERVAL);
	result.is_null = false;
	result.value_.interval.months = months;
	result.value_.interval.days = days;
	result.value_.interval.micros = micros;
	return result;
}

Value Value::INTERVAL(interval_t interval) {
	return Value::INTERVAL(interval.months, interval.days, interval.micros);
}

//===--------------------------------------------------------------------===//
// CreateValue
//===--------------------------------------------------------------------===//
template <>
Value Value::CreateValue(bool value) {
	return Value::BOOLEAN(value);
}

template <>
Value Value::CreateValue(int8_t value) {
	return Value::TINYINT(value);
}

template <>
Value Value::CreateValue(int16_t value) {
	return Value::SMALLINT(value);
}

template <>
Value Value::CreateValue(int32_t value) {
	return Value::INTEGER(value);
}

template <>
Value Value::CreateValue(int64_t value) {
	return Value::BIGINT(value);
}

template <>
Value Value::CreateValue(uint8_t value) {
	return Value::UTINYINT(value);
}

template <>
Value Value::CreateValue(uint16_t value) {
	return Value::USMALLINT(value);
}

template <>
Value Value::CreateValue(uint32_t value) {
	return Value::UINTEGER(value);
}

template <>
Value Value::CreateValue(uint64_t value) {
	return Value::UBIGINT(value);
}

template <>
Value Value::CreateValue(hugeint_t value) {
	return Value::HUGEINT(value);
}

template <>
Value Value::CreateValue(date_t value) {
	return Value::DATE(value);
}

template <>
Value Value::CreateValue(dtime_t value) {
	return Value::TIME(value);
}

template <>
Value Value::CreateValue(timestamp_t value) {
	return Value::TIMESTAMP(value);
}

template <>
Value Value::CreateValue(const char *value) {
	return Value(string(value));
}

template <>
Value Value::CreateValue(string value) { // NOLINT: required for templating
	return Value::BLOB(value);
}

template <>
Value Value::CreateValue(string_t value) {
	return Value(value);
}

template <>
Value Value::CreateValue(float value) {
	return Value::FLOAT(value);
}

template <>
Value Value::CreateValue(double value) {
	return Value::DOUBLE(value);
}

template <>
Value Value::CreateValue(interval_t value) {
	return Value::INTERVAL(value);
}

template <>
Value Value::CreateValue(Value value) {
	return value;
}

//===--------------------------------------------------------------------===//
// GetValue
//===--------------------------------------------------------------------===//
template <class T>
T Value::GetValueInternal() const {
	if (IsNull()) {
		throw InternalException("Calling GetValueInternal on a value that is NULL");
	}
	switch (type_.id()) {
	case LogicalTypeId::BOOLEAN:
		return Cast::Operation<bool, T>(value_.boolean);
	case LogicalTypeId::TINYINT:
		return Cast::Operation<int8_t, T>(value_.tinyint);
	case LogicalTypeId::SMALLINT:
		return Cast::Operation<int16_t, T>(value_.smallint);
	case LogicalTypeId::INTEGER:
		return Cast::Operation<int32_t, T>(value_.integer);
	case LogicalTypeId::BIGINT:
		return Cast::Operation<int64_t, T>(value_.bigint);
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UUID:
		return Cast::Operation<hugeint_t, T>(value_.hugeint);
	case LogicalTypeId::DATE:
		return Cast::Operation<date_t, T>(value_.date);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return Cast::Operation<dtime_t, T>(value_.time);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return Cast::Operation<timestamp_t, T>(value_.timestamp);
	case LogicalTypeId::UTINYINT:
		return Cast::Operation<uint8_t, T>(value_.utinyint);
	case LogicalTypeId::USMALLINT:
		return Cast::Operation<uint16_t, T>(value_.usmallint);
	case LogicalTypeId::UINTEGER:
		return Cast::Operation<uint32_t, T>(value_.uinteger);
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::UBIGINT:
		return Cast::Operation<uint64_t, T>(value_.ubigint);
	case LogicalTypeId::FLOAT:
		return Cast::Operation<float, T>(value_.float_);
	case LogicalTypeId::DOUBLE:
		return Cast::Operation<double, T>(value_.double_);
	case LogicalTypeId::VARCHAR:
		return Cast::Operation<string_t, T>(str_value.c_str());
	case LogicalTypeId::INTERVAL:
		return Cast::Operation<interval_t, T>(value_.interval);
	case LogicalTypeId::DECIMAL:
		return CastAs(LogicalType::DOUBLE).GetValueInternal<T>();
	case LogicalTypeId::ENUM: {
		switch (type_.InternalType()) {
		case PhysicalType::UINT8:
			return Cast::Operation<uint8_t, T>(value_.utinyint);
		case PhysicalType::UINT16:
			return Cast::Operation<uint16_t, T>(value_.usmallint);
		case PhysicalType::UINT32:
			return Cast::Operation<uint32_t, T>(value_.uinteger);
		default:
			throw InternalException("Invalid Internal Type for ENUMs");
		}
	}
	default:
		throw NotImplementedException("Unimplemented type \"%s\" for GetValue()", type_.ToString());
	}
}

template <>
bool Value::GetValue() const {
	return GetValueInternal<int8_t>();
}
template <>
int8_t Value::GetValue() const {
	return GetValueInternal<int8_t>();
}
template <>
int16_t Value::GetValue() const {
	return GetValueInternal<int16_t>();
}
template <>
int32_t Value::GetValue() const {
	if (type_.id() == LogicalTypeId::DATE) {
		return value_.integer;
	}
	return GetValueInternal<int32_t>();
}
template <>
int64_t Value::GetValue() const {
	if (IsNull()) {
		throw InternalException("Calling GetValue on a value that is NULL");
	}
	switch (type_.id()) {
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
		return value_.bigint;
	default:
		return GetValueInternal<int64_t>();
	}
}
template <>
hugeint_t Value::GetValue() const {
	return GetValueInternal<hugeint_t>();
}
template <>
uint8_t Value::GetValue() const {
	return GetValueInternal<uint8_t>();
}
template <>
uint16_t Value::GetValue() const {
	return GetValueInternal<uint16_t>();
}
template <>
uint32_t Value::GetValue() const {
	return GetValueInternal<uint32_t>();
}
template <>
uint64_t Value::GetValue() const {
	return GetValueInternal<uint64_t>();
}
template <>
string Value::GetValue() const {
	return ToString();
}
template <>
float Value::GetValue() const {
	return GetValueInternal<float>();
}
template <>
double Value::GetValue() const {
	return GetValueInternal<double>();
}
template <>
date_t Value::GetValue() const {
	return GetValueInternal<date_t>();
}
template <>
dtime_t Value::GetValue() const {
	return GetValueInternal<dtime_t>();
}
template <>
timestamp_t Value::GetValue() const {
	return GetValueInternal<timestamp_t>();
}

template <>
DUCKDB_API interval_t Value::GetValue() const {
	return GetValueInternal<interval_t>();
}

template <>
DUCKDB_API Value Value::GetValue() const {
	return Value(*this);
}

uintptr_t Value::GetPointer() const {
	D_ASSERT(type() == LogicalType::POINTER);
	return value_.pointer;
}

Value Value::Numeric(const LogicalType &type, int64_t value) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		D_ASSERT(value == 0 || value == 1);
		return Value::BOOLEAN(value ? 1 : 0);
	case LogicalTypeId::TINYINT:
		D_ASSERT(value >= NumericLimits<int8_t>::Minimum() && value <= NumericLimits<int8_t>::Maximum());
		return Value::TINYINT((int8_t)value);
	case LogicalTypeId::SMALLINT:
		D_ASSERT(value >= NumericLimits<int16_t>::Minimum() && value <= NumericLimits<int16_t>::Maximum());
		return Value::SMALLINT((int16_t)value);
	case LogicalTypeId::INTEGER:
		D_ASSERT(value >= NumericLimits<int32_t>::Minimum() && value <= NumericLimits<int32_t>::Maximum());
		return Value::INTEGER((int32_t)value);
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(value);
	case LogicalTypeId::UTINYINT:
		D_ASSERT(value >= NumericLimits<uint8_t>::Minimum() && value <= NumericLimits<uint8_t>::Maximum());
		return Value::UTINYINT((uint8_t)value);
	case LogicalTypeId::USMALLINT:
		D_ASSERT(value >= NumericLimits<uint16_t>::Minimum() && value <= NumericLimits<uint16_t>::Maximum());
		return Value::USMALLINT((uint16_t)value);
	case LogicalTypeId::UINTEGER:
		D_ASSERT(value >= NumericLimits<uint32_t>::Minimum() && value <= NumericLimits<uint32_t>::Maximum());
		return Value::UINTEGER((uint32_t)value);
	case LogicalTypeId::UBIGINT:
		D_ASSERT(value >= 0);
		return Value::UBIGINT(value);
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(value);
	case LogicalTypeId::DECIMAL:
		return Value::DECIMAL(value, DecimalType::GetWidth(type), DecimalType::GetScale(type));
	case LogicalTypeId::FLOAT:
		return Value((float)value);
	case LogicalTypeId::DOUBLE:
		return Value((double)value);
	case LogicalTypeId::POINTER:
		return Value::POINTER(value);
	case LogicalTypeId::DATE:
		D_ASSERT(value >= NumericLimits<int32_t>::Minimum() && value <= NumericLimits<int32_t>::Maximum());
		return Value::DATE(date_t(value));
	case LogicalTypeId::TIME:
		return Value::TIME(dtime_t(value));
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(timestamp_t(value));
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(timestamp_t(value));
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TIMESTAMPMS(timestamp_t(value));
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(timestamp_t(value));
	case LogicalTypeId::TIME_TZ:
		return Value::TIMETZ(dtime_t(value));
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(timestamp_t(value));
	case LogicalTypeId::ENUM:
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			D_ASSERT(value >= NumericLimits<uint8_t>::Minimum() && value <= NumericLimits<uint8_t>::Maximum());
			return Value::UTINYINT((uint8_t)value);
		case PhysicalType::UINT16:
			D_ASSERT(value >= NumericLimits<uint16_t>::Minimum() && value <= NumericLimits<uint16_t>::Maximum());
			return Value::USMALLINT((uint16_t)value);
		case PhysicalType::UINT32:
			D_ASSERT(value >= NumericLimits<uint32_t>::Minimum() && value <= NumericLimits<uint32_t>::Maximum());
			return Value::UINTEGER((uint32_t)value);
		default:
			throw InternalException("Enum doesn't accept this physical type");
		}
	default:
		throw InvalidTypeException(type, "Numeric requires numeric type");
	}
}

Value Value::Numeric(const LogicalType &type, hugeint_t value) {
#ifdef DEBUG
	// perform a throwing cast to verify that the type fits
	Value::HUGEINT(value).CastAs(type);
#endif
	switch (type.id()) {
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(value);
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(Hugeint::Cast<uint64_t>(value));
	default:
		return Value::Numeric(type, Hugeint::Cast<int64_t>(value));
	}
}

//===--------------------------------------------------------------------===//
// GetValueUnsafe
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::BOOL);
	return value_.boolean;
}

template <>
int8_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT8 || type_.InternalType() == PhysicalType::BOOL);
	return value_.tinyint;
}

template <>
int16_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT16);
	return value_.smallint;
}

template <>
int32_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT32);
	return value_.integer;
}

template <>
int64_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.bigint;
}

template <>
hugeint_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT128);
	return value_.hugeint;
}

template <>
uint8_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT8);
	return value_.utinyint;
}

template <>
uint16_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT16);
	return value_.usmallint;
}

template <>
uint32_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT32);
	return value_.uinteger;
}

template <>
uint64_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT64);
	return value_.ubigint;
}

template <>
string Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::VARCHAR);
	return str_value;
}

template <>
DUCKDB_API string_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::VARCHAR);
	return string_t(str_value);
}

template <>
float Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::FLOAT);
	return value_.float_;
}

template <>
double Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::DOUBLE);
	return value_.double_;
}

template <>
date_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT32);
	return value_.date;
}

template <>
dtime_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.time;
}

template <>
timestamp_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.timestamp;
}

template <>
interval_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INTERVAL);
	return value_.interval;
}

//===--------------------------------------------------------------------===//
// GetReferenceUnsafe
//===--------------------------------------------------------------------===//
template <>
int8_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT8 || type_.InternalType() == PhysicalType::BOOL);
	return value_.tinyint;
}

template <>
int16_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT16);
	return value_.smallint;
}

template <>
int32_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT32);
	return value_.integer;
}

template <>
int64_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.bigint;
}

template <>
hugeint_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT128);
	return value_.hugeint;
}

template <>
uint8_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT8);
	return value_.utinyint;
}

template <>
uint16_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT16);
	return value_.usmallint;
}

template <>
uint32_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT32);
	return value_.uinteger;
}

template <>
uint64_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT64);
	return value_.ubigint;
}

template <>
float &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::FLOAT);
	return value_.float_;
}

template <>
double &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::DOUBLE);
	return value_.double_;
}

template <>
date_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT32);
	return value_.date;
}

template <>
dtime_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.time;
}

template <>
timestamp_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.timestamp;
}

template <>
interval_t &Value::GetReferenceUnsafe() {
	D_ASSERT(type_.InternalType() == PhysicalType::INTERVAL);
	return value_.interval;
}

//===--------------------------------------------------------------------===//
// Hash
//===--------------------------------------------------------------------===//
hash_t Value::Hash() const {
	if (IsNull()) {
		return 0;
	}
	switch (type_.InternalType()) {
	case PhysicalType::BOOL:
		return duckdb::Hash(value_.boolean);
	case PhysicalType::INT8:
		return duckdb::Hash(value_.tinyint);
	case PhysicalType::INT16:
		return duckdb::Hash(value_.smallint);
	case PhysicalType::INT32:
		return duckdb::Hash(value_.integer);
	case PhysicalType::INT64:
		return duckdb::Hash(value_.bigint);
	case PhysicalType::UINT8:
		return duckdb::Hash(value_.utinyint);
	case PhysicalType::UINT16:
		return duckdb::Hash(value_.usmallint);
	case PhysicalType::UINT32:
		return duckdb::Hash(value_.uinteger);
	case PhysicalType::UINT64:
		return duckdb::Hash(value_.ubigint);
	case PhysicalType::INT128:
		return duckdb::Hash(value_.hugeint);
	case PhysicalType::FLOAT:
		return duckdb::Hash(value_.float_);
	case PhysicalType::DOUBLE:
		return duckdb::Hash(value_.double_);
	case PhysicalType::INTERVAL:
		return duckdb::Hash(value_.interval);
	case PhysicalType::VARCHAR:
		return duckdb::Hash(string_t(StringValue::Get(*this)));
	case PhysicalType::STRUCT: {
		auto &struct_children = StructValue::GetChildren(*this);
		hash_t hash = 0;
		for (auto &entry : struct_children) {
			hash ^= entry.Hash();
		}
		return hash;
	}
	case PhysicalType::LIST: {
		auto &list_children = ListValue::GetChildren(*this);
		hash_t hash = 0;
		for (auto &entry : list_children) {
			hash ^= entry.Hash();
		}
		return hash;
	}
	default:
		throw InternalException("Unimplemented type for value hash");
	}
}

string Value::ToString() const {
	if (IsNull()) {
		return "NULL";
	}
	switch (type_.id()) {
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
	case LogicalTypeId::UTINYINT:
		return to_string(value_.utinyint);
	case LogicalTypeId::USMALLINT:
		return to_string(value_.usmallint);
	case LogicalTypeId::UINTEGER:
		return to_string(value_.uinteger);
	case LogicalTypeId::UBIGINT:
		return to_string(value_.ubigint);
	case LogicalTypeId::HUGEINT:
		return Hugeint::ToString(value_.hugeint);
	case LogicalTypeId::UUID:
		return UUID::ToString(value_.hugeint);
	case LogicalTypeId::FLOAT:
		return duckdb_fmt::format("{}", value_.float_);
	case LogicalTypeId::DOUBLE:
		return duckdb_fmt::format("{}", value_.double_);
	case LogicalTypeId::DECIMAL: {
		auto internal_type = type_.InternalType();
		auto width = DecimalType::GetWidth(type_);
		auto scale = DecimalType::GetScale(type_);
		if (internal_type == PhysicalType::INT16) {
			return Decimal::ToString(value_.smallint, width, scale);
		} else if (internal_type == PhysicalType::INT32) {
			return Decimal::ToString(value_.integer, width, scale);
		} else if (internal_type == PhysicalType::INT64) {
			return Decimal::ToString(value_.bigint, width, scale);
		} else {
			D_ASSERT(internal_type == PhysicalType::INT128);
			return Decimal::ToString(value_.hugeint, width, scale);
		}
	}
	case LogicalTypeId::DATE:
		return Date::ToString(value_.date);
	case LogicalTypeId::TIME:
		return Time::ToString(value_.time);
	case LogicalTypeId::TIMESTAMP:
		return Timestamp::ToString(value_.timestamp);
	case LogicalTypeId::TIME_TZ:
		return Time::ToString(value_.time) + Time::ToUTCOffset(0, 0);
	case LogicalTypeId::TIMESTAMP_TZ: {
		// Infinite TSTZ values do not display offsets in PG.
		auto ret = Timestamp::ToString(value_.timestamp);
		if (Timestamp::IsFinite(value_.timestamp)) {
			ret += Time::ToUTCOffset(0, 0);
		}
		return ret;
	}
	case LogicalTypeId::TIMESTAMP_SEC:
		return Timestamp::ToString(Timestamp::FromEpochSeconds(value_.timestamp.value));
	case LogicalTypeId::TIMESTAMP_MS:
		return Timestamp::ToString(Timestamp::FromEpochMs(value_.timestamp.value));
	case LogicalTypeId::TIMESTAMP_NS:
		return Timestamp::ToString(Timestamp::FromEpochNanoSeconds(value_.timestamp.value));
	case LogicalTypeId::INTERVAL:
		return Interval::ToString(value_.interval);
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR:
		return str_value;
	case LogicalTypeId::BLOB:
		return Blob::ToString(string_t(str_value));
	case LogicalTypeId::POINTER:
		return to_string(value_.pointer);
	case LogicalTypeId::STRUCT: {
		string ret = "{";
		auto &child_types = StructType::GetChildTypes(type_);
		for (size_t i = 0; i < struct_value.size(); i++) {
			auto &name = child_types[i].first;
			auto &child = struct_value[i];
			ret += "'" + name + "': " + child.ToString();
			if (i < struct_value.size() - 1) {
				ret += ", ";
			}
		}
		ret += "}";
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
	case LogicalTypeId::MAP: {
		string ret = "{";
		auto &key_list = struct_value[0].list_value;
		auto &value_list = struct_value[1].list_value;
		for (size_t i = 0; i < key_list.size(); i++) {
			ret += key_list[i].ToString() + "=" + value_list[i].ToString();
			if (i < key_list.size() - 1) {
				ret += ", ";
			}
		}
		ret += "}";
		return ret;
	}
	case LogicalTypeId::ENUM: {
		auto &values_insert_order = EnumType::GetValuesInsertOrder(type_);
		uint64_t enum_idx;
		switch (type_.InternalType()) {
		case PhysicalType::UINT8:
			enum_idx = value_.utinyint;
			break;
		case PhysicalType::UINT16:
			enum_idx = value_.usmallint;
			break;
		case PhysicalType::UINT32:
			enum_idx = value_.uinteger;
			break;
		case PhysicalType::UINT64:
			return string((const char *)value_.bigint);
		default:
			throw InternalException("ENUM can only have unsigned integers as physical types");
		}
		return values_insert_order.GetValue(enum_idx).ToString();
	}
	default:
		throw NotImplementedException("Unimplemented type for printing: %s", type_.ToString());
	}
}

string Value::ToSQLString() const {
	if (IsNull()) {
		return ToString();
	}
	switch (type_.id()) {
	case LogicalTypeId::UUID:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::BLOB:
		return "'" + ToString() + "'::" + type_.ToString();
	case LogicalTypeId::VARCHAR:
		return "'" + StringUtil::Replace(ToString(), "'", "''") + "'";
	case LogicalTypeId::STRUCT: {
		string ret = "{";
		auto &child_types = StructType::GetChildTypes(type_);
		for (size_t i = 0; i < struct_value.size(); i++) {
			auto &name = child_types[i].first;
			auto &child = struct_value[i];
			ret += "'" + name + "': " + child.ToSQLString();
			if (i < struct_value.size() - 1) {
				ret += ", ";
			}
		}
		ret += "}";
		return ret;
	}
	case LogicalTypeId::FLOAT:
		if (!FloatIsFinite(FloatValue::Get(*this))) {
			return "'" + ToString() + "'::" + type_.ToString();
		}
		return ToString();
	case LogicalTypeId::DOUBLE: {
		double val = DoubleValue::Get(*this);
		if (!DoubleIsFinite(val)) {
			if (!Value::IsNan(val)) {
				// to infinity and beyond
				return val < 0 ? "-1e1000" : "1e1000";
			}
			return "'" + ToString() + "'::" + type_.ToString();
		}
		return ToString();
	}
	case LogicalTypeId::LIST: {
		string ret = "[";
		for (size_t i = 0; i < list_value.size(); i++) {
			auto &child = list_value[i];
			ret += child.ToSQLString();
			if (i < list_value.size() - 1) {
				ret += ", ";
			}
		}
		ret += "]";
		return ret;
	}
	default:
		return ToString();
	}
}

//===--------------------------------------------------------------------===//
// Type-specific getters
//===--------------------------------------------------------------------===//
bool BooleanValue::Get(const Value &value) {
	return value.GetValueUnsafe<bool>();
}

int8_t TinyIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<int8_t>();
}

int16_t SmallIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<int16_t>();
}

int32_t IntegerValue::Get(const Value &value) {
	return value.GetValueUnsafe<int32_t>();
}

int64_t BigIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<int64_t>();
}

hugeint_t HugeIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<hugeint_t>();
}

uint8_t UTinyIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<uint8_t>();
}

uint16_t USmallIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<uint16_t>();
}

uint32_t UIntegerValue::Get(const Value &value) {
	return value.GetValueUnsafe<uint32_t>();
}

uint64_t UBigIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<uint64_t>();
}

float FloatValue::Get(const Value &value) {
	return value.GetValueUnsafe<float>();
}

double DoubleValue::Get(const Value &value) {
	return value.GetValueUnsafe<double>();
}

const string &StringValue::Get(const Value &value) {
	D_ASSERT(value.type().InternalType() == PhysicalType::VARCHAR);
	return value.str_value;
}

date_t DateValue::Get(const Value &value) {
	return value.GetValueUnsafe<date_t>();
}

dtime_t TimeValue::Get(const Value &value) {
	return value.GetValueUnsafe<dtime_t>();
}

timestamp_t TimestampValue::Get(const Value &value) {
	return value.GetValueUnsafe<timestamp_t>();
}

interval_t IntervalValue::Get(const Value &value) {
	return value.GetValueUnsafe<interval_t>();
}

const vector<Value> &StructValue::GetChildren(const Value &value) {
	D_ASSERT(value.type().InternalType() == PhysicalType::STRUCT);
	return value.struct_value;
}

const vector<Value> &ListValue::GetChildren(const Value &value) {
	D_ASSERT(value.type().InternalType() == PhysicalType::LIST);
	return value.list_value;
}

hugeint_t IntegralValue::Get(const Value &value) {
	switch (value.type().InternalType()) {
	case PhysicalType::INT8:
		return TinyIntValue::Get(value);
	case PhysicalType::INT16:
		return SmallIntValue::Get(value);
	case PhysicalType::INT32:
		return IntegerValue::Get(value);
	case PhysicalType::INT64:
		return BigIntValue::Get(value);
	case PhysicalType::INT128:
		return HugeIntValue::Get(value);
	case PhysicalType::UINT8:
		return UTinyIntValue::Get(value);
	case PhysicalType::UINT16:
		return USmallIntValue::Get(value);
	case PhysicalType::UINT32:
		return UIntegerValue::Get(value);
	case PhysicalType::UINT64:
		return UBigIntValue::Get(value);
	default:
		throw InternalException("Invalid internal type \"%s\" for IntegralValue::Get", value.type().ToString());
	}
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
	return *this == Value::Numeric(type_, rhs);
}

bool Value::operator!=(const int64_t &rhs) const {
	return *this != Value::Numeric(type_, rhs);
}

bool Value::operator<(const int64_t &rhs) const {
	return *this < Value::Numeric(type_, rhs);
}

bool Value::operator>(const int64_t &rhs) const {
	return *this > Value::Numeric(type_, rhs);
}

bool Value::operator<=(const int64_t &rhs) const {
	return *this <= Value::Numeric(type_, rhs);
}

bool Value::operator>=(const int64_t &rhs) const {
	return *this >= Value::Numeric(type_, rhs);
}

bool Value::TryCastAs(const LogicalType &target_type, Value &new_value, string *error_message, bool strict) const {
	if (type_ == target_type) {
		new_value = Copy();
		return true;
	}
	Vector input(*this);
	Vector result(target_type);
	if (!VectorOperations::TryCast(input, result, 1, error_message, strict)) {
		return false;
	}
	new_value = result.GetValue(0);
	return true;
}

Value Value::CastAs(const LogicalType &target_type, bool strict) const {
	Value new_value;
	string error_message;
	if (!TryCastAs(target_type, new_value, &error_message, strict)) {
		throw InvalidInputException("Failed to cast value: %s", error_message);
	}
	return new_value;
}

bool Value::TryCastAs(const LogicalType &target_type, bool strict) {
	Value new_value;
	string error_message;
	if (!TryCastAs(target_type, new_value, &error_message, strict)) {
		return false;
	}
	type_ = target_type;
	is_null = new_value.is_null;
	value_ = new_value.value_;
	str_value = new_value.str_value;
	struct_value = new_value.struct_value;
	list_value = new_value.list_value;
	return true;
}

void Value::Serialize(Serializer &main_serializer) const {
	FieldWriter writer(main_serializer);
	writer.WriteSerializable(type_);
	writer.WriteField<bool>(IsNull());
	if (!IsNull()) {
		auto &serializer = writer.GetSerializer();
		switch (type_.InternalType()) {
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
		case PhysicalType::UINT8:
			serializer.Write<uint8_t>(value_.utinyint);
			break;
		case PhysicalType::UINT16:
			serializer.Write<uint16_t>(value_.usmallint);
			break;
		case PhysicalType::UINT32:
			serializer.Write<uint32_t>(value_.uinteger);
			break;
		case PhysicalType::UINT64:
			serializer.Write<uint64_t>(value_.ubigint);
			break;
		case PhysicalType::INT128:
			serializer.Write<hugeint_t>(value_.hugeint);
			break;
		case PhysicalType::FLOAT:
			serializer.Write<float>(value_.float_);
			break;
		case PhysicalType::DOUBLE:
			serializer.Write<double>(value_.double_);
			break;
		case PhysicalType::INTERVAL:
			serializer.Write<interval_t>(value_.interval);
			break;
		case PhysicalType::VARCHAR:
			serializer.WriteString(str_value);
			break;
		default: {
			Vector v(*this);
			v.Serialize(1, serializer);
			break;
		}
		}
	}
	writer.Finalize();
}

Value Value::Deserialize(Deserializer &main_source) {
	FieldReader reader(main_source);
	auto type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto is_null = reader.ReadRequired<bool>();
	Value new_value = Value(type);
	if (is_null) {
		reader.Finalize();
		return new_value;
	}
	new_value.is_null = false;
	auto &source = reader.GetSource();
	switch (type.InternalType()) {
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
	case PhysicalType::UINT8:
		new_value.value_.utinyint = source.Read<uint8_t>();
		break;
	case PhysicalType::UINT16:
		new_value.value_.usmallint = source.Read<uint16_t>();
		break;
	case PhysicalType::UINT32:
		new_value.value_.uinteger = source.Read<uint32_t>();
		break;
	case PhysicalType::UINT64:
		new_value.value_.ubigint = source.Read<uint64_t>();
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
	case PhysicalType::INTERVAL:
		new_value.value_.interval = source.Read<interval_t>();
		break;
	case PhysicalType::VARCHAR:
		new_value.str_value = source.Read<string>();
		break;
	default: {
		Vector v(type);
		v.Deserialize(1, source);
		new_value = v.GetValue(0);
		break;
	}
	}
	reader.Finalize();
	return new_value;
}

void Value::Print() const {
	Printer::Print(ToString());
}

bool Value::NotDistinctFrom(const Value &lvalue, const Value &rvalue) {
	return ValueOperations::NotDistinctFrom(lvalue, rvalue);
}

bool Value::ValuesAreEqual(const Value &result_value, const Value &value) {
	if (result_value.IsNull() != value.IsNull()) {
		return false;
	}
	if (result_value.IsNull() && value.IsNull()) {
		// NULL = NULL in checking code
		return true;
	}
	switch (value.type_.id()) {
	case LogicalTypeId::FLOAT: {
		auto other = result_value.CastAs(LogicalType::FLOAT);
		float ldecimal = value.value_.float_;
		float rdecimal = other.value_.float_;
		return ApproxEqual(ldecimal, rdecimal);
	}
	case LogicalTypeId::DOUBLE: {
		auto other = result_value.CastAs(LogicalType::DOUBLE);
		double ldecimal = value.value_.double_;
		double rdecimal = other.value_.double_;
		return ApproxEqual(ldecimal, rdecimal);
	}
	case LogicalTypeId::VARCHAR: {
		auto other = result_value.CastAs(LogicalType::VARCHAR);
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
		if (result_value.type_.id() == LogicalTypeId::FLOAT || result_value.type_.id() == LogicalTypeId::DOUBLE) {
			return Value::ValuesAreEqual(value, result_value);
		}
		return value == result_value;
	}
}

} // namespace duckdb
