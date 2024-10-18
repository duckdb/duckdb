#include "duckdb/common/types/value.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

#include "duckdb/common/uhugeint.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/common/types/varint.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include <utility>
#include <cmath>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Extra Value Info
//===--------------------------------------------------------------------===//
enum class ExtraValueInfoType : uint8_t { INVALID_TYPE_INFO = 0, STRING_VALUE_INFO = 1, NESTED_VALUE_INFO = 2 };

struct ExtraValueInfo {
	explicit ExtraValueInfo(ExtraValueInfoType type) : type(type) {
	}
	virtual ~ExtraValueInfo() {
	}

	ExtraValueInfoType type;

public:
	bool Equals(ExtraValueInfo *other_p) const {
		if (!other_p) {
			return false;
		}
		if (type != other_p->type) {
			return false;
		}
		return EqualsInternal(other_p);
	}

	template <class T>
	T &Get() {
		if (type != T::TYPE) {
			throw InternalException("ExtraValueInfo type mismatch");
		}
		return (T &)*this;
	}

protected:
	virtual bool EqualsInternal(ExtraValueInfo *other_p) const {
		return true;
	}
};

//===--------------------------------------------------------------------===//
// String Value Info
//===--------------------------------------------------------------------===//
struct StringValueInfo : public ExtraValueInfo {
	static constexpr const ExtraValueInfoType TYPE = ExtraValueInfoType::STRING_VALUE_INFO;

public:
	explicit StringValueInfo(string str_p)
	    : ExtraValueInfo(ExtraValueInfoType::STRING_VALUE_INFO), str(std::move(str_p)) {
	}

	const string &GetString() {
		return str;
	}

protected:
	bool EqualsInternal(ExtraValueInfo *other_p) const override {
		return other_p->Get<StringValueInfo>().str == str;
	}

	string str;
};

//===--------------------------------------------------------------------===//
// Nested Value Info
//===--------------------------------------------------------------------===//
struct NestedValueInfo : public ExtraValueInfo {
	static constexpr const ExtraValueInfoType TYPE = ExtraValueInfoType::NESTED_VALUE_INFO;

public:
	NestedValueInfo() : ExtraValueInfo(ExtraValueInfoType::NESTED_VALUE_INFO) {
	}
	explicit NestedValueInfo(vector<Value> values_p)
	    : ExtraValueInfo(ExtraValueInfoType::NESTED_VALUE_INFO), values(std::move(values_p)) {
	}

	const vector<Value> &GetValues() {
		return values;
	}

protected:
	bool EqualsInternal(ExtraValueInfo *other_p) const override {
		return other_p->Get<NestedValueInfo>().values == values;
	}

	vector<Value> values;
};
//===--------------------------------------------------------------------===//
// Value
//===--------------------------------------------------------------------===//
Value::Value(LogicalType type) : type_(std::move(type)), is_null(true) {
}

Value::Value(int32_t val) : type_(LogicalType::INTEGER), is_null(false) {
	value_.integer = val;
}

Value::Value(bool val) : type_(LogicalType::BOOLEAN), is_null(false) {
	value_.boolean = val;
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

Value::Value(string_t val) : Value(val.GetString()) {
}

Value::Value(string val) : type_(LogicalType::VARCHAR), is_null(false) {
	if (!Value::StringIsValid(val.c_str(), val.size())) {
		throw ErrorManager::InvalidUnicodeError(val, "value construction");
	}
	value_info_ = make_shared_ptr<StringValueInfo>(std::move(val));
}

Value::~Value() {
}

Value::Value(const Value &other)
    : type_(other.type_), is_null(other.is_null), value_(other.value_), value_info_(other.value_info_) {
}

Value::Value(Value &&other) noexcept
    : type_(std::move(other.type_)), is_null(other.is_null), value_(other.value_),
      value_info_(std::move(other.value_info_)) {
}

Value &Value::operator=(const Value &other) {
	if (this == &other) {
		return *this;
	}
	type_ = other.type_;
	is_null = other.is_null;
	value_ = other.value_;
	value_info_ = other.value_info_;
	return *this;
}

Value &Value::operator=(Value &&other) noexcept {
	type_ = std::move(other.type_);
	is_null = other.is_null;
	value_ = other.value_;
	value_info_ = std::move(other.value_info_);
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
	case LogicalTypeId::UHUGEINT:
		return Value::UHUGEINT(NumericLimits<uhugeint_t>::Minimum());
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
	case LogicalTypeId::TIMESTAMP_SEC: {
		//	Casting rounds up, which will overflow
		const auto min_us = MinimumValue(LogicalType::TIMESTAMP).GetValue<timestamp_t>();
		return Value::TIMESTAMPSEC(timestamp_t(Timestamp::GetEpochSeconds(min_us)));
	}
	case LogicalTypeId::TIMESTAMP_MS: {
		//	Casting rounds up, which will overflow
		const auto min_us = MinimumValue(LogicalType::TIMESTAMP).GetValue<timestamp_t>();
		return Value::TIMESTAMPMS(timestamp_t(Timestamp::GetEpochMs(min_us)));
	}
	case LogicalTypeId::TIMESTAMP_NS: {
		// Clear the fractional day.
		auto min_ns = NumericLimits<int64_t>::Minimum();
		min_ns /= Interval::NANOS_PER_DAY;
		min_ns *= Interval::NANOS_PER_DAY;
		return Value::TIMESTAMPNS(timestamp_t(min_ns));
	}
	case LogicalTypeId::TIME_TZ:
		//	"00:00:00+1559" from the PG docs, but actually 00:00:00+15:59:59
		return Value::TIMETZ(dtime_tz_t(dtime_t(0), dtime_tz_t::MAX_OFFSET));
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
	case LogicalTypeId::VARINT:
		return Value::VARINT(Varint::VarcharToVarInt(
		    "-179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540"
		    "4589535143824642343213268894641827684675467035375169860499105765512820762454900903893289440758685084551339"
		    "42304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368"));
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
	case LogicalTypeId::UHUGEINT:
		return Value::UHUGEINT(NumericLimits<uhugeint_t>::Maximum());
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
		//	24:00:00 according to PG
		return Value::TIME(dtime_t(Interval::MICROS_PER_DAY));
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(timestamp_t(NumericLimits<int64_t>::Maximum() - 1));
	case LogicalTypeId::TIMESTAMP_MS: {
		//	Casting rounds up, which will overflow
		const auto max_us = MaximumValue(LogicalType::TIMESTAMP).GetValue<timestamp_t>();
		return Value::TIMESTAMPMS(timestamp_t(Timestamp::GetEpochMs(max_us)));
	}
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(timestamp_t(NumericLimits<int64_t>::Maximum() - 1));
	case LogicalTypeId::TIMESTAMP_SEC: {
		//	Casting rounds up, which will overflow
		const auto max_us = MaximumValue(LogicalType::TIMESTAMP).GetValue<timestamp_t>();
		return Value::TIMESTAMPSEC(timestamp_t(Timestamp::GetEpochSeconds(max_us)));
	}
	case LogicalTypeId::TIME_TZ:
		//	"24:00:00-1559" from the PG docs but actually "24:00:00-15:59:59"
		return Value::TIMETZ(dtime_tz_t(dtime_t(Interval::MICROS_PER_DAY), dtime_tz_t::MIN_OFFSET));
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
	case LogicalTypeId::ENUM: {
		auto enum_size = EnumType::GetSize(type);
		return Value::ENUM(enum_size - (enum_size ? 1 : 0), type);
	}
	case LogicalTypeId::VARINT:
		return Value::VARINT(Varint::VarcharToVarInt(
		    "1797693134862315708145274237317043567980705675258449965989174768031572607800285387605895586327668781715404"
		    "5895351438246423432132688946418276846754670353751698604991057655128207624549009038932894407586850845513394"
		    "2304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368"));
	default:
		throw InvalidTypeException(type, "MaximumValue requires numeric type");
	}
}

Value Value::Infinity(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DATE:
		return Value::DATE(date_t::infinity());
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(timestamp_t::infinity());
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TIMESTAMPMS(timestamp_t::infinity());
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(timestamp_t::infinity());
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(timestamp_t::infinity());
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(timestamp_t::infinity());
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(std::numeric_limits<float>::infinity());
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(std::numeric_limits<double>::infinity());
	default:
		throw InvalidTypeException(type, "Infinity requires numeric type");
	}
}

Value Value::NegativeInfinity(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DATE:
		return Value::DATE(date_t::ninfinity());
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(timestamp_t::ninfinity());
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TIMESTAMPMS(timestamp_t::ninfinity());
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(timestamp_t::ninfinity());
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(timestamp_t::ninfinity());
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(timestamp_t::ninfinity());
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(-std::numeric_limits<float>::infinity());
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(-std::numeric_limits<double>::infinity());
	default:
		throw InvalidTypeException(type, "NegativeInfinity requires numeric type");
	}
}

Value Value::BOOLEAN(bool value) {
	Value result(LogicalType::BOOLEAN);
	result.value_.boolean = value;
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

Value Value::UHUGEINT(uhugeint_t value) {
	Value result(LogicalType::UHUGEINT);
	result.value_.uhugeint = value;
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
	return Value::DECIMAL(int64_t(value), width, scale);
}

Value Value::DECIMAL(int32_t value, uint8_t width, uint8_t scale) {
	return Value::DECIMAL(int64_t(value), width, scale);
}

Value Value::DECIMAL(int64_t value, uint8_t width, uint8_t scale) {
	auto decimal_type = LogicalType::DECIMAL(width, scale);
	Value result(decimal_type);
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		result.value_.smallint = NumericCast<int16_t>(value);
		break;
	case PhysicalType::INT32:
		result.value_.integer = NumericCast<int32_t>(value);
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

Value Value::TIMETZ(dtime_tz_t value) {
	Value result(LogicalType::TIME_TZ);
	result.value_.timetz = value;
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

Value Value::STRUCT(const LogicalType &type, vector<Value> struct_values) {
	Value result;
	auto child_types = StructType::GetChildTypes(type);
	for (size_t i = 0; i < struct_values.size(); i++) {
		struct_values[i] = struct_values[i].DefaultCastAs(child_types[i].second);
	}
	result.value_info_ = make_shared_ptr<NestedValueInfo>(std::move(struct_values));
	result.type_ = type;
	result.is_null = false;
	return result;
}
Value Value::STRUCT(child_list_t<Value> values) {
	child_list_t<LogicalType> child_types;
	vector<Value> struct_values;
	for (auto &child : values) {
		child_types.push_back(make_pair(std::move(child.first), child.second.type()));
		struct_values.push_back(std::move(child.second));
	}
	return Value::STRUCT(LogicalType::STRUCT(child_types), std::move(struct_values));
}

Value Value::MAP(const LogicalType &child_type, vector<Value> values) { // NOLINT
	vector<Value> map_keys;
	vector<Value> map_values;
	for (auto &val : values) {
		D_ASSERT(val.type().InternalType() == PhysicalType::STRUCT);
		auto &children = StructValue::GetChildren(val);
		D_ASSERT(children.size() == 2);
		map_keys.push_back(children[0]);
		map_values.push_back(children[1]);
	}
	auto &key_type = StructType::GetChildType(child_type, 0);
	auto &value_type = StructType::GetChildType(child_type, 1);
	return Value::MAP(key_type, value_type, std::move(map_keys), std::move(map_values));
}

Value Value::MAP(const LogicalType &key_type, const LogicalType &value_type, vector<Value> keys, vector<Value> values) {
	D_ASSERT(keys.size() == values.size());
	Value result;

	result.type_ = LogicalType::MAP(key_type, value_type);
	result.is_null = false;
	for (idx_t i = 0; i < keys.size(); i++) {
		child_list_t<Value> new_children;
		new_children.reserve(2);
		new_children.push_back(std::make_pair("key", std::move(keys[i])));
		new_children.push_back(std::make_pair("value", std::move(values[i])));
		values[i] = Value::STRUCT(std::move(new_children));
	}
	result.value_info_ = make_shared_ptr<NestedValueInfo>(std::move(values));
	return result;
}

Value Value::MAP(const unordered_map<string, string> &kv_pairs) {
	Value result;
	result.type_ = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	result.is_null = false;
	vector<Value> pairs;
	for (auto &kv : kv_pairs) {
		pairs.push_back(Value::STRUCT({{"key", Value(kv.first)}, {"value", Value(kv.second)}}));
	}
	result.value_info_ = make_shared_ptr<NestedValueInfo>(std::move(pairs));
	return result;
}

Value Value::UNION(child_list_t<LogicalType> members, uint8_t tag, Value value) {
	D_ASSERT(!members.empty());
	D_ASSERT(members.size() <= UnionType::MAX_UNION_MEMBERS);
	D_ASSERT(members.size() > tag);

	D_ASSERT(value.type() == members[tag].second);

	Value result;
	result.is_null = false;
	// add the tag to the front of the struct
	vector<Value> union_values;
	union_values.emplace_back(Value::UTINYINT(tag));
	for (idx_t i = 0; i < members.size(); i++) {
		if (i != tag) {
			union_values.emplace_back(members[i].second);
		} else {
			union_values.emplace_back(nullptr);
		}
	}
	union_values[tag + 1] = std::move(value);
	result.value_info_ = make_shared_ptr<NestedValueInfo>(std::move(union_values));
	result.type_ = LogicalType::UNION(std::move(members));
	return result;
}

Value Value::LIST(vector<Value> values) {
	if (values.empty()) {
		throw InternalException("Value::LIST without providing a child-type requires a non-empty list of values. Use "
		                        "Value::LIST(child_type, list) instead.");
	}
#ifdef DEBUG
	for (idx_t i = 1; i < values.size(); i++) {
		D_ASSERT(values[i].type() == values[0].type());
	}
#endif
	Value result;
	result.type_ = LogicalType::LIST(values[0].type());
	result.value_info_ = make_shared_ptr<NestedValueInfo>(std::move(values));
	result.is_null = false;
	return result;
}

Value Value::LIST(const LogicalType &child_type, vector<Value> values) {
	if (values.empty()) {
		return Value::EMPTYLIST(child_type);
	}
	for (auto &val : values) {
		val = val.DefaultCastAs(child_type);
	}
	return Value::LIST(std::move(values));
}

Value Value::EMPTYLIST(const LogicalType &child_type) {
	Value result;
	result.type_ = LogicalType::LIST(child_type);
	result.value_info_ = make_shared_ptr<NestedValueInfo>();
	result.is_null = false;
	return result;
}

Value Value::ARRAY(vector<Value> values) {
	if (values.empty()) {
		throw InternalException("Value::ARRAY without providing a child-type requires a non-empty list of values. Use "
		                        "Value::ARRAY(child_type, list) instead.");
	}
#ifdef DEBUG
	for (idx_t i = 1; i < values.size(); i++) {
		D_ASSERT(values[i].type() == values[0].type());
	}
#endif
	Value result;
	result.type_ = LogicalType::ARRAY(values[0].type(), values.size());
	result.value_info_ = make_shared_ptr<NestedValueInfo>(std::move(values));
	result.is_null = false;
	return result;
}

Value Value::ARRAY(const LogicalType &child_type, vector<Value> values) {
	if (values.empty()) {
		return Value::EMPTYARRAY(child_type, 0);
	}
	for (auto &val : values) {
		val = val.DefaultCastAs(child_type);
	}
	return Value::ARRAY(std::move(values));
}

Value Value::EMPTYARRAY(const LogicalType &child_type, uint32_t size) {
	Value result;
	result.type_ = LogicalType::ARRAY(child_type, size);
	result.value_info_ = make_shared_ptr<NestedValueInfo>();
	result.is_null = false;
	return result;
}

Value Value::BLOB(const_data_ptr_t data, idx_t len) {
	Value result(LogicalType::BLOB);
	result.is_null = false;
	result.value_info_ = make_shared_ptr<StringValueInfo>(string(const_char_ptr_cast(data), len));
	return result;
}

Value Value::VARINT(const_data_ptr_t data, idx_t len) {
	return VARINT(string(const_char_ptr_cast(data), len));
}

Value Value::VARINT(const string &data) {
	Value result(LogicalType::VARINT);
	result.is_null = false;
	result.value_info_ = make_shared_ptr<StringValueInfo>(data);
	return result;
}

Value Value::BLOB(const string &data) {
	Value result(LogicalType::BLOB);
	result.is_null = false;
	result.value_info_ = make_shared_ptr<StringValueInfo>(Blob::ToBlob(string_t(data)));
	return result;
}

Value Value::AGGREGATE_STATE(const LogicalType &type, const_data_ptr_t data, idx_t len) { // NOLINT
	Value result(type);
	result.is_null = false;
	result.value_info_ = make_shared_ptr<StringValueInfo>(string(const_char_ptr_cast(data), len));
	return result;
}

Value Value::BIT(const_data_ptr_t data, idx_t len) {
	Value result(LogicalType::BIT);
	result.is_null = false;
	result.value_info_ = make_shared_ptr<StringValueInfo>(string(const_char_ptr_cast(data), len));
	return result;
}

Value Value::BIT(const string &data) {
	Value result(LogicalType::BIT);
	result.is_null = false;
	result.value_info_ = make_shared_ptr<StringValueInfo>(Bit::ToBit(string_t(data)));
	return result;
}

Value Value::ENUM(uint64_t value, const LogicalType &original_type) {
	D_ASSERT(original_type.id() == LogicalTypeId::ENUM);
	Value result(original_type);
	switch (original_type.InternalType()) {
	case PhysicalType::UINT8:
		result.value_.utinyint = NumericCast<uint8_t>(value);
		break;
	case PhysicalType::UINT16:
		result.value_.usmallint = NumericCast<uint16_t>(value);
		break;
	case PhysicalType::UINT32:
		result.value_.uinteger = NumericCast<uint32_t>(value);
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
Value Value::CreateValue(uhugeint_t value) {
	return Value::UHUGEINT(value);
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
Value Value::CreateValue(dtime_tz_t value) {
	return Value::TIMETZ(value);
}

template <>
Value Value::CreateValue(timestamp_t value) {
	return Value::TIMESTAMP(value);
}

template <>
Value Value::CreateValue(timestamp_sec_t value) {
	return Value::TIMESTAMPSEC(value);
}

template <>
Value Value::CreateValue(timestamp_ms_t value) {
	return Value::TIMESTAMPMS(value);
}

template <>
Value Value::CreateValue(timestamp_ns_t value) {
	return Value::TIMESTAMPNS(value);
}

template <>
Value Value::CreateValue(timestamp_tz_t value) {
	return Value::TIMESTAMPTZ(value);
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
	case LogicalTypeId::UHUGEINT:
		return Cast::Operation<uhugeint_t, T>(value_.uhugeint);
	case LogicalTypeId::DATE:
		return Cast::Operation<date_t, T>(value_.date);
	case LogicalTypeId::TIME:
		return Cast::Operation<dtime_t, T>(value_.time);
	case LogicalTypeId::TIME_TZ:
		return Cast::Operation<dtime_tz_t, T>(value_.timetz);
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
		return Cast::Operation<string_t, T>(StringValue::Get(*this).c_str());
	case LogicalTypeId::INTERVAL:
		return Cast::Operation<interval_t, T>(value_.interval);
	case LogicalTypeId::DECIMAL:
		return DefaultCastAs(LogicalType::DOUBLE).GetValueInternal<T>();
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
uhugeint_t Value::GetValue() const {
	return GetValueInternal<uhugeint_t>();
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
dtime_tz_t Value::GetValue() const {
	return GetValueInternal<dtime_tz_t>();
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
		return Value::BOOLEAN(value ? true : false);
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
		return Value::UBIGINT(NumericCast<uint64_t>(value));
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(value);
	case LogicalTypeId::UHUGEINT:
		return Value::UHUGEINT(NumericCast<uint64_t>(value));
	case LogicalTypeId::DECIMAL:
		return Value::DECIMAL(value, DecimalType::GetWidth(type), DecimalType::GetScale(type));
	case LogicalTypeId::FLOAT:
		return Value((float)value);
	case LogicalTypeId::DOUBLE:
		return Value((double)value);
	case LogicalTypeId::POINTER:
		return Value::POINTER(NumericCast<uintptr_t>(value));
	case LogicalTypeId::DATE:
		D_ASSERT(value >= NumericLimits<int32_t>::Minimum() && value <= NumericLimits<int32_t>::Maximum());
		return Value::DATE(date_t(NumericCast<int32_t>(value)));
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
	Value::HUGEINT(value).DefaultCastAs(type);
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

Value Value::Numeric(const LogicalType &type, uhugeint_t value) {
#ifdef DEBUG
	// perform a throwing cast to verify that the type fits
	Value::UHUGEINT(value).DefaultCastAs(type);
#endif
	switch (type.id()) {
	case LogicalTypeId::UHUGEINT:
		return Value::UHUGEINT(value);
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(Uhugeint::Cast<uint64_t>(value));
	default:
		return Value::Numeric(type, Uhugeint::Cast<int64_t>(value));
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
uhugeint_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT128);
	return value_.uhugeint;
}

template <>
string Value::GetValueUnsafe() const {
	return StringValue::Get(*this);
}

template <>
DUCKDB_API string_t Value::GetValueUnsafe() const {
	return string_t(StringValue::Get(*this));
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
dtime_tz_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.timetz;
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
// Hash
//===--------------------------------------------------------------------===//
hash_t Value::Hash() const {
	if (IsNull()) {
		return 0;
	}
	Vector input(*this);
	Vector result(LogicalType::HASH);
	VectorOperations::Hash(input, result, 1);

	auto data = FlatVector::GetData<hash_t>(result);
	return data[0];
}

string Value::ToString() const {
	if (IsNull()) {
		return "NULL";
	}
	return StringValue::Get(DefaultCastAs(LogicalType::VARCHAR));
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
	case LogicalTypeId::ENUM:
		return "'" + StringUtil::Replace(ToString(), "'", "''") + "'";
	case LogicalTypeId::STRUCT: {
		bool is_unnamed = StructType::IsUnnamed(type_);
		string ret = is_unnamed ? "(" : "{";
		auto &child_types = StructType::GetChildTypes(type_);
		auto &struct_values = StructValue::GetChildren(*this);
		for (idx_t i = 0; i < struct_values.size(); i++) {
			auto &name = child_types[i].first;
			auto &child = struct_values[i];
			if (is_unnamed) {
				ret += child.ToSQLString();
			} else {
				ret += "'" + name + "': " + child.ToSQLString();
			}
			if (i < struct_values.size() - 1) {
				ret += ", ";
			}
		}
		ret += is_unnamed ? ")" : "}";
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
		auto &list_values = ListValue::GetChildren(*this);
		for (idx_t i = 0; i < list_values.size(); i++) {
			auto &child = list_values[i];
			ret += child.ToSQLString();
			if (i < list_values.size() - 1) {
				ret += ", ";
			}
		}
		ret += "]";
		return ret;
	}
	case LogicalTypeId::ARRAY: {
		string ret = "[";
		auto &array_values = ArrayValue::GetChildren(*this);
		for (idx_t i = 0; i < array_values.size(); i++) {
			auto &child = array_values[i];
			ret += child.ToSQLString();
			if (i < array_values.size() - 1) {
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

uhugeint_t UhugeIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<uhugeint_t>();
}

float FloatValue::Get(const Value &value) {
	return value.GetValueUnsafe<float>();
}

double DoubleValue::Get(const Value &value) {
	return value.GetValueUnsafe<double>();
}

const string &StringValue::Get(const Value &value) {
	if (value.is_null) {
		throw InternalException("Calling StringValue::Get on a NULL value");
	}
	D_ASSERT(value.type().InternalType() == PhysicalType::VARCHAR);
	D_ASSERT(value.value_info_);
	return value.value_info_->Get<StringValueInfo>().GetString();
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
	if (value.is_null) {
		throw InternalException("Calling StructValue::GetChildren on a NULL value");
	}
	D_ASSERT(value.type().InternalType() == PhysicalType::STRUCT);
	D_ASSERT(value.value_info_);
	return value.value_info_->Get<NestedValueInfo>().GetValues();
}

const vector<Value> &MapValue::GetChildren(const Value &value) {
	if (value.is_null) {
		throw InternalException("Calling MapValue::GetChildren on a NULL value");
	}
	D_ASSERT(value.type().id() == LogicalTypeId::MAP);
	D_ASSERT(value.type().InternalType() == PhysicalType::LIST);
	D_ASSERT(value.value_info_);
	return value.value_info_->Get<NestedValueInfo>().GetValues();
}

const vector<Value> &ListValue::GetChildren(const Value &value) {
	if (value.is_null) {
		throw InternalException("Calling ListValue::GetChildren on a NULL value");
	}
	D_ASSERT(value.type().InternalType() == PhysicalType::LIST);
	D_ASSERT(value.value_info_);
	return value.value_info_->Get<NestedValueInfo>().GetValues();
}

const vector<Value> &ArrayValue::GetChildren(const Value &value) {
	if (value.is_null) {
		throw InternalException("Calling ArrayValue::GetChildren on a NULL value");
	}
	D_ASSERT(value.type().InternalType() == PhysicalType::ARRAY);
	D_ASSERT(value.value_info_);
	return value.value_info_->Get<NestedValueInfo>().GetValues();
}

const Value &UnionValue::GetValue(const Value &value) {
	D_ASSERT(value.type().id() == LogicalTypeId::UNION);
	auto &children = StructValue::GetChildren(value);
	auto tag = children[0].GetValueUnsafe<union_tag_t>();
	D_ASSERT(tag < children.size() - 1);
	return children[tag + 1];
}

union_tag_t UnionValue::GetTag(const Value &value) {
	D_ASSERT(value.type().id() == LogicalTypeId::UNION);
	auto children = StructValue::GetChildren(value);
	auto tag = children[0].GetValueUnsafe<union_tag_t>();
	D_ASSERT(tag < children.size() - 1);
	return tag;
}

const LogicalType &UnionValue::GetType(const Value &value) {
	return UnionType::GetMemberType(value.type(), UnionValue::GetTag(value));
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
		return NumericCast<int64_t>(UBigIntValue::Get(value));
	case PhysicalType::UINT128:
		return static_cast<hugeint_t>(UhugeIntValue::Get(value));
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

bool Value::TryCastAs(CastFunctionSet &set, GetCastFunctionInput &get_input, const LogicalType &target_type,
                      Value &new_value, string *error_message, bool strict) const {
	if (type_ == target_type) {
		new_value = Copy();
		return true;
	}
	Vector input(*this);
	Vector result(target_type);
	if (!VectorOperations::TryCast(set, get_input, input, result, 1, error_message, strict)) {
		return false;
	}
	new_value = result.GetValue(0);
	return true;
}

bool Value::TryCastAs(ClientContext &context, const LogicalType &target_type, Value &new_value, string *error_message,
                      bool strict) const {
	GetCastFunctionInput get_input(context);
	return TryCastAs(CastFunctionSet::Get(context), get_input, target_type, new_value, error_message, strict);
}

bool Value::DefaultTryCastAs(const LogicalType &target_type, Value &new_value, string *error_message,
                             bool strict) const {
	CastFunctionSet set;
	GetCastFunctionInput get_input;
	return TryCastAs(set, get_input, target_type, new_value, error_message, strict);
}

Value Value::CastAs(CastFunctionSet &set, GetCastFunctionInput &get_input, const LogicalType &target_type,
                    bool strict) const {
	Value new_value;
	string error_message;
	if (!TryCastAs(set, get_input, target_type, new_value, &error_message, strict)) {
		throw InvalidInputException("Failed to cast value: %s", error_message);
	}
	return new_value;
}

Value Value::CastAs(ClientContext &context, const LogicalType &target_type, bool strict) const {
	GetCastFunctionInput get_input(context);
	return CastAs(CastFunctionSet::Get(context), get_input, target_type, strict);
}

Value Value::DefaultCastAs(const LogicalType &target_type, bool strict) const {
	CastFunctionSet set;
	GetCastFunctionInput get_input;
	return CastAs(set, get_input, target_type, strict);
}

bool Value::TryCastAs(CastFunctionSet &set, GetCastFunctionInput &get_input, const LogicalType &target_type,
                      bool strict) {
	Value new_value;
	string error_message;
	if (!TryCastAs(set, get_input, target_type, new_value, &error_message, strict)) {
		return false;
	}
	type_ = target_type;
	is_null = new_value.is_null;
	value_ = new_value.value_;
	value_info_ = std::move(new_value.value_info_);
	return true;
}

bool Value::TryCastAs(ClientContext &context, const LogicalType &target_type, bool strict) {
	GetCastFunctionInput get_input(context);
	return TryCastAs(CastFunctionSet::Get(context), get_input, target_type, strict);
}

bool Value::DefaultTryCastAs(const LogicalType &target_type, bool strict) {
	CastFunctionSet set;
	GetCastFunctionInput get_input;
	return TryCastAs(set, get_input, target_type, strict);
}

void Value::Reinterpret(LogicalType new_type) {
	this->type_ = std::move(new_type);
}

void Value::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "type", type_);
	serializer.WriteProperty(101, "is_null", is_null);
	if (!IsNull()) {
		switch (type_.InternalType()) {
		case PhysicalType::BIT:
			throw InternalException("BIT type should not be serialized");
		case PhysicalType::BOOL:
			serializer.WriteProperty(102, "value", value_.boolean);
			break;
		case PhysicalType::INT8:
			serializer.WriteProperty(102, "value", value_.tinyint);
			break;
		case PhysicalType::INT16:
			serializer.WriteProperty(102, "value", value_.smallint);
			break;
		case PhysicalType::INT32:
			serializer.WriteProperty(102, "value", value_.integer);
			break;
		case PhysicalType::INT64:
			serializer.WriteProperty(102, "value", value_.bigint);
			break;
		case PhysicalType::UINT8:
			serializer.WriteProperty(102, "value", value_.utinyint);
			break;
		case PhysicalType::UINT16:
			serializer.WriteProperty(102, "value", value_.usmallint);
			break;
		case PhysicalType::UINT32:
			serializer.WriteProperty(102, "value", value_.uinteger);
			break;
		case PhysicalType::UINT64:
			serializer.WriteProperty(102, "value", value_.ubigint);
			break;
		case PhysicalType::INT128:
			serializer.WriteProperty(102, "value", value_.hugeint);
			break;
		case PhysicalType::UINT128:
			serializer.WriteProperty(102, "value", value_.uhugeint);
			break;
		case PhysicalType::FLOAT:
			serializer.WriteProperty(102, "value", value_.float_);
			break;
		case PhysicalType::DOUBLE:
			serializer.WriteProperty(102, "value", value_.double_);
			break;
		case PhysicalType::INTERVAL:
			serializer.WriteProperty(102, "value", value_.interval);
			break;
		case PhysicalType::VARCHAR: {
			if (type_.id() == LogicalTypeId::BLOB) {
				auto blob_str = Blob::ToString(StringValue::Get(*this));
				serializer.WriteProperty(102, "value", blob_str);
			} else {
				serializer.WriteProperty(102, "value", StringValue::Get(*this));
			}
		} break;
		case PhysicalType::LIST: {
			serializer.WriteObject(102, "value", [&](Serializer &serializer) {
				auto &children = ListValue::GetChildren(*this);
				serializer.WriteProperty(100, "children", children);
			});
		} break;
		case PhysicalType::STRUCT: {
			serializer.WriteObject(102, "value", [&](Serializer &serializer) {
				auto &children = StructValue::GetChildren(*this);
				serializer.WriteProperty(100, "children", children);
			});
		} break;
		case PhysicalType::ARRAY: {
			serializer.WriteObject(102, "value", [&](Serializer &serializer) {
				auto &children = ArrayValue::GetChildren(*this);
				serializer.WriteProperty(100, "children", children);
			});
		} break;
		default:
			throw NotImplementedException("Unimplemented type for Serialize");
		}
	}
}

Value Value::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<LogicalType>(100, "type");
	auto is_null = deserializer.ReadProperty<bool>(101, "is_null");
	Value new_value = Value(type);
	if (is_null) {
		return new_value;
	}
	new_value.is_null = false;
	switch (type.InternalType()) {
	case PhysicalType::BIT:
		throw InternalException("BIT type should not be deserialized");
	case PhysicalType::BOOL:
		new_value.value_.boolean = deserializer.ReadProperty<bool>(102, "value");
		break;
	case PhysicalType::UINT8:
		new_value.value_.utinyint = deserializer.ReadProperty<uint8_t>(102, "value");
		break;
	case PhysicalType::INT8:
		new_value.value_.tinyint = deserializer.ReadProperty<int8_t>(102, "value");
		break;
	case PhysicalType::UINT16:
		new_value.value_.usmallint = deserializer.ReadProperty<uint16_t>(102, "value");
		break;
	case PhysicalType::INT16:
		new_value.value_.smallint = deserializer.ReadProperty<int16_t>(102, "value");
		break;
	case PhysicalType::UINT32:
		new_value.value_.uinteger = deserializer.ReadProperty<uint32_t>(102, "value");
		break;
	case PhysicalType::INT32:
		new_value.value_.integer = deserializer.ReadProperty<int32_t>(102, "value");
		break;
	case PhysicalType::UINT64:
		new_value.value_.ubigint = deserializer.ReadProperty<uint64_t>(102, "value");
		break;
	case PhysicalType::INT64:
		new_value.value_.bigint = deserializer.ReadProperty<int64_t>(102, "value");
		break;
	case PhysicalType::UINT128:
		new_value.value_.uhugeint = deserializer.ReadProperty<uhugeint_t>(102, "value");
		break;
	case PhysicalType::INT128:
		new_value.value_.hugeint = deserializer.ReadProperty<hugeint_t>(102, "value");
		break;
	case PhysicalType::FLOAT:
		new_value.value_.float_ = deserializer.ReadProperty<float>(102, "value");
		break;
	case PhysicalType::DOUBLE:
		new_value.value_.double_ = deserializer.ReadProperty<double>(102, "value");
		break;
	case PhysicalType::INTERVAL:
		new_value.value_.interval = deserializer.ReadProperty<interval_t>(102, "value");
		break;
	case PhysicalType::VARCHAR: {
		auto str = deserializer.ReadProperty<string>(102, "value");
		if (type.id() == LogicalTypeId::BLOB) {
			new_value.value_info_ = make_shared_ptr<StringValueInfo>(Blob::ToBlob(str));
		} else {
			new_value.value_info_ = make_shared_ptr<StringValueInfo>(str);
		}
	} break;
	case PhysicalType::LIST: {
		deserializer.ReadObject(102, "value", [&](Deserializer &obj) {
			auto children = obj.ReadProperty<vector<Value>>(100, "children");
			new_value.value_info_ = make_shared_ptr<NestedValueInfo>(children);
		});
	} break;
	case PhysicalType::STRUCT: {
		deserializer.ReadObject(102, "value", [&](Deserializer &obj) {
			auto children = obj.ReadProperty<vector<Value>>(100, "children");
			new_value.value_info_ = make_shared_ptr<NestedValueInfo>(children);
		});
	} break;
	case PhysicalType::ARRAY: {
		deserializer.ReadObject(102, "value", [&](Deserializer &obj) {
			auto children = obj.ReadProperty<vector<Value>>(100, "children");
			new_value.value_info_ = make_shared_ptr<NestedValueInfo>(children);
		});
	} break;
	default:
		throw NotImplementedException("Unimplemented type for Deserialize");
	}
	return new_value;
}

void Value::Print() const {
	Printer::Print(ToString());
}

bool Value::NotDistinctFrom(const Value &lvalue, const Value &rvalue) {
	return ValueOperations::NotDistinctFrom(lvalue, rvalue);
}

static string SanitizeValue(string input) {
	// some results might contain padding spaces, e.g. when rendering
	// VARCHAR(10) and the string only has 6 characters, they will be padded
	// with spaces to 10 in the rendering. We don't do that here yet as we
	// are looking at internal structures. So just ignore any extra spaces
	// on the right
	StringUtil::RTrim(input);
	// for result checking code, replace null bytes with their escaped value (\0)
	return StringUtil::Replace(input, string("\0", 1), "\\0");
}

bool Value::ValuesAreEqual(CastFunctionSet &set, GetCastFunctionInput &get_input, const Value &result_value,
                           const Value &value) {
	if (result_value.IsNull() != value.IsNull()) {
		return false;
	}
	if (result_value.IsNull() && value.IsNull()) {
		// NULL = NULL in checking code
		return true;
	}
	switch (value.type_.id()) {
	case LogicalTypeId::FLOAT: {
		auto other = result_value.CastAs(set, get_input, LogicalType::FLOAT);
		float ldecimal = value.value_.float_;
		float rdecimal = other.value_.float_;
		return ApproxEqual(ldecimal, rdecimal);
	}
	case LogicalTypeId::DOUBLE: {
		auto other = result_value.CastAs(set, get_input, LogicalType::DOUBLE);
		double ldecimal = value.value_.double_;
		double rdecimal = other.value_.double_;
		return ApproxEqual(ldecimal, rdecimal);
	}
	case LogicalTypeId::VARCHAR: {
		auto other = result_value.CastAs(set, get_input, LogicalType::VARCHAR);
		string left = SanitizeValue(StringValue::Get(other));
		string right = SanitizeValue(StringValue::Get(value));
		return left == right;
	}
	default:
		if (result_value.type_.id() == LogicalTypeId::FLOAT || result_value.type_.id() == LogicalTypeId::DOUBLE) {
			return Value::ValuesAreEqual(set, get_input, value, result_value);
		}
		return value == result_value;
	}
}

bool Value::ValuesAreEqual(ClientContext &context, const Value &result_value, const Value &value) {
	GetCastFunctionInput get_input(context);
	return Value::ValuesAreEqual(CastFunctionSet::Get(context), get_input, result_value, value);
}
bool Value::DefaultValuesAreEqual(const Value &result_value, const Value &value) {
	CastFunctionSet set;
	GetCastFunctionInput get_input;
	return Value::ValuesAreEqual(set, get_input, result_value, value);
}

} // namespace duckdb
