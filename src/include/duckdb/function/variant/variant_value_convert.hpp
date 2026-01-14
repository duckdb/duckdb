#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/variant_visitor.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct ValueConverter {
	using result_type = Value;

	static Value VisitNull() {
		return Value(LogicalType::SQLNULL);
	}

	static Value VisitBoolean(bool val) {
		return Value::BOOLEAN(val);
	}

	template <typename T>
	static Value VisitInteger(T val) {
		throw InternalException("ValueConverter::VisitInteger not implemented!");
	}

	static Value VisitTime(dtime_t val) {
		return Value::TIME(val);
	}

	static Value VisitTimeNanos(dtime_ns_t val) {
		return Value::TIME_NS(val);
	}

	static Value VisitTimeTZ(dtime_tz_t val) {
		return Value::TIMETZ(val);
	}

	static Value VisitTimestampSec(timestamp_sec_t val) {
		return Value::TIMESTAMPSEC(val);
	}

	static Value VisitTimestampMs(timestamp_ms_t val) {
		return Value::TIMESTAMPMS(val);
	}

	static Value VisitTimestamp(timestamp_t val) {
		return Value::TIMESTAMP(val);
	}

	static Value VisitTimestampNanos(timestamp_ns_t val) {
		return Value::TIMESTAMPNS(val);
	}

	static Value VisitTimestampTZ(timestamp_tz_t val) {
		return Value::TIMESTAMPTZ(val);
	}

	static Value VisitFloat(float val) {
		return Value::FLOAT(val);
	}
	static Value VisitDouble(double val) {
		return Value::DOUBLE(val);
	}
	static Value VisitUUID(hugeint_t val) {
		return Value::UUID(val);
	}
	static Value VisitDate(date_t val) {
		return Value::DATE(val);
	}
	static Value VisitInterval(interval_t val) {
		return Value::INTERVAL(val);
	}

	static Value VisitString(const string_t &str) {
		return Value(str);
	}
	static Value VisitBlob(const string_t &str) {
		return Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	static Value VisitBignum(const string_t &str) {
		return Value::BIGNUM(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	static Value VisitGeometry(const string_t &str) {
		return Value::GEOMETRY(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	static Value VisitBitstring(const string_t &str) {
		return Value::BIT(const_data_ptr_cast(str.GetData()), str.GetSize());
	}

	template <typename T>
	static Value VisitDecimal(T val, uint32_t width, uint32_t scale) {
		if (std::is_same<T, int16_t>::value) {
			return Value::DECIMAL(val, static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else if (std::is_same<T, int32_t>::value) {
			return Value::DECIMAL(val, static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else if (std::is_same<T, int64_t>::value) {
			return Value::DECIMAL(val, static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else if (std::is_same<T, hugeint_t>::value) {
			return Value::DECIMAL(val, static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else {
			throw InternalException("Unhandled decimal type");
		}
	}

	static Value VisitArray(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data) {
		auto array_items = VariantVisitor<ValueConverter>::VisitArrayItems(variant, row, nested_data);
		return Value::LIST(LogicalType::VARIANT(), std::move(array_items));
	}

	static Value VisitObject(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data) {
		auto object_children = VariantVisitor<ValueConverter>::VisitObjectItems(variant, row, nested_data);
		return Value::STRUCT(std::move(object_children));
	}

	static Value VisitDefault(VariantLogicalType type_id, const_data_ptr_t) {
		throw InternalException("VariantLogicalType(%s) not handled", EnumUtil::ToString(type_id));
	}
};

template <>
Value ValueConverter::VisitInteger<int8_t>(int8_t val);
template <>
Value ValueConverter::VisitInteger<int16_t>(int16_t val);
template <>
Value ValueConverter::VisitInteger<int32_t>(int32_t val);
template <>
Value ValueConverter::VisitInteger<int64_t>(int64_t val);
template <>
Value ValueConverter::VisitInteger<hugeint_t>(hugeint_t val);
template <>
Value ValueConverter::VisitInteger<uint8_t>(uint8_t val);
template <>
Value ValueConverter::VisitInteger<uint16_t>(uint16_t val);
template <>
Value ValueConverter::VisitInteger<uint32_t>(uint32_t val);
template <>
Value ValueConverter::VisitInteger<uint64_t>(uint64_t val);
template <>
Value ValueConverter::VisitInteger<uhugeint_t>(uhugeint_t val);

struct TypeConverter {
	using result_type = LogicalType;

	static LogicalType VisitNull() {
		return LogicalType::SQLNULL;
	}

	static LogicalType VisitBoolean(bool val) {
		return LogicalType::BOOLEAN;
	}

	template <typename T>
	static LogicalType VisitInteger(T val) {
		throw InternalException("TypeConverter::VisitInteger not implemented!");
	}

	static LogicalType VisitTime(dtime_t val) {
		return LogicalType::TIME;
	}

	static LogicalType VisitTimeNanos(dtime_ns_t val) {
		return LogicalType::TIME_NS;
	}

	static LogicalType VisitTimeTZ(dtime_tz_t val) {
		return LogicalType::TIME_TZ;
	}

	static LogicalType VisitTimestampSec(timestamp_sec_t val) {
		return LogicalType::TIMESTAMP_S;
	}

	static LogicalType VisitTimestampMs(timestamp_ms_t val) {
		return LogicalType::TIMESTAMP_MS;
	}

	static LogicalType VisitTimestamp(timestamp_t val) {
		return LogicalType::TIMESTAMP;
	}

	static LogicalType VisitTimestampNanos(timestamp_ns_t val) {
		return LogicalType::TIMESTAMP_NS;
	}

	static LogicalType VisitTimestampTZ(timestamp_tz_t val) {
		return LogicalType::TIMESTAMP_TZ;
	}

	static LogicalType VisitFloat(float val) {
		return LogicalType::FLOAT;
	}
	static LogicalType VisitDouble(double val) {
		return LogicalType::DOUBLE;
	}
	static LogicalType VisitUUID(hugeint_t val) {
		return LogicalType::UUID;
	}
	static LogicalType VisitDate(date_t val) {
		return LogicalType::DATE;
	}
	static LogicalType VisitInterval(interval_t val) {
		return LogicalType::INTERVAL;
	}

	static LogicalType VisitString(const string_t &str) {
		return LogicalType::VARCHAR;
	}
	static LogicalType VisitBlob(const string_t &str) {
		return LogicalType::BLOB;
	}
	static LogicalType VisitBignum(const string_t &str) {
		return LogicalType::BIGNUM;
	}
	static LogicalType VisitGeometry(const string_t &str) {
		return LogicalType::GEOMETRY();
	}
	static LogicalType VisitBitstring(const string_t &str) {
		return LogicalType::BIT;
	}

	template <typename T>
	static LogicalType VisitDecimal(T, uint32_t width, uint32_t scale) {
		if (std::is_same<T, int16_t>::value) {
			return LogicalType::DECIMAL(static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else if (std::is_same<T, int32_t>::value) {
			return LogicalType::DECIMAL(static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else if (std::is_same<T, int64_t>::value) {
			return LogicalType::DECIMAL(static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else if (std::is_same<T, hugeint_t>::value) {
			return LogicalType::DECIMAL(static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else {
			throw InternalException("Unhandled decimal type");
		}
	}

	static LogicalType VisitArray(const UnifiedVariantVectorData &variant, idx_t row,
	                              const VariantNestedData &nested_data) {
		auto array_items = VariantVisitor<TypeConverter>::VisitArrayItems(variant, row, nested_data);
		if (array_items.empty()) {
			return LogicalType::LIST(LogicalType::SQLNULL);
		}
		auto &child_type = array_items[0];
		for (idx_t i = 1; i < array_items.size(); i++) {
			if (child_type != array_items[i]) {
				return LogicalType::LIST(LogicalType::VARIANT());
			}
		}
		return LogicalType::LIST(child_type);
	}

	static LogicalType VisitObject(const UnifiedVariantVectorData &variant, idx_t row,
	                               const VariantNestedData &nested_data) {
		auto object_children = VariantVisitor<TypeConverter>::VisitObjectItems(variant, row, nested_data);
		return LogicalType::STRUCT(std::move(object_children));
	}

	static LogicalType VisitDefault(VariantLogicalType type_id, const_data_ptr_t) {
		throw InternalException("VariantLogicalType(%s) not handled", EnumUtil::ToString(type_id));
	}
};

template <>
LogicalType TypeConverter::VisitInteger<int8_t>(int8_t val);
template <>
LogicalType TypeConverter::VisitInteger<int16_t>(int16_t val);
template <>
LogicalType TypeConverter::VisitInteger<int32_t>(int32_t val);
template <>
LogicalType TypeConverter::VisitInteger<int64_t>(int64_t val);
template <>
LogicalType TypeConverter::VisitInteger<hugeint_t>(hugeint_t val);
template <>
LogicalType TypeConverter::VisitInteger<uint8_t>(uint8_t val);
template <>
LogicalType TypeConverter::VisitInteger<uint16_t>(uint16_t val);
template <>
LogicalType TypeConverter::VisitInteger<uint32_t>(uint32_t val);
template <>
LogicalType TypeConverter::VisitInteger<uint64_t>(uint64_t val);
template <>
LogicalType TypeConverter::VisitInteger<uhugeint_t>(uhugeint_t val);

} // namespace duckdb
