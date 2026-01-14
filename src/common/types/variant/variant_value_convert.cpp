#include "duckdb/function/variant/variant_value_convert.hpp"

namespace duckdb {

template <>
Value ValueConverter::VisitInteger<int8_t>(int8_t val) {
	return Value::TINYINT(val);
}

template <>
Value ValueConverter::VisitInteger<int16_t>(int16_t val) {
	return Value::SMALLINT(val);
}

template <>
Value ValueConverter::VisitInteger<int32_t>(int32_t val) {
	return Value::INTEGER(val);
}

template <>
Value ValueConverter::VisitInteger<int64_t>(int64_t val) {
	return Value::BIGINT(val);
}

template <>
Value ValueConverter::VisitInteger<hugeint_t>(hugeint_t val) {
	return Value::HUGEINT(val);
}

template <>
Value ValueConverter::VisitInteger<uint8_t>(uint8_t val) {
	return Value::UTINYINT(val);
}

template <>
Value ValueConverter::VisitInteger<uint16_t>(uint16_t val) {
	return Value::USMALLINT(val);
}

template <>
Value ValueConverter::VisitInteger<uint32_t>(uint32_t val) {
	return Value::UINTEGER(val);
}

template <>
Value ValueConverter::VisitInteger<uint64_t>(uint64_t val) {
	return Value::UBIGINT(val);
}

template <>
Value ValueConverter::VisitInteger<uhugeint_t>(uhugeint_t val) {
	return Value::UHUGEINT(val);
}

template <>
LogicalType TypeConverter::VisitInteger<int8_t>(int8_t val) {
	return LogicalType::TINYINT;
}

template <>
LogicalType TypeConverter::VisitInteger<int16_t>(int16_t val) {
	return LogicalType::SMALLINT;
}

template <>
LogicalType TypeConverter::VisitInteger<int32_t>(int32_t val) {
	return LogicalType::INTEGER;
}

template <>
LogicalType TypeConverter::VisitInteger<int64_t>(int64_t val) {
	return LogicalType::BIGINT;
}

template <>
LogicalType TypeConverter::VisitInteger<hugeint_t>(hugeint_t val) {
	return LogicalType::HUGEINT;
}

template <>
LogicalType TypeConverter::VisitInteger<uint8_t>(uint8_t val) {
	return LogicalType::UTINYINT;
}

template <>
LogicalType TypeConverter::VisitInteger<uint16_t>(uint16_t val) {
	return LogicalType::USMALLINT;
}

template <>
LogicalType TypeConverter::VisitInteger<uint32_t>(uint32_t val) {
	return LogicalType::UINTEGER;
}

template <>
LogicalType TypeConverter::VisitInteger<uint64_t>(uint64_t val) {
	return LogicalType::UBIGINT;
}

template <>
LogicalType TypeConverter::VisitInteger<uhugeint_t>(uhugeint_t val) {
	return LogicalType::UHUGEINT;
}

} // namespace duckdb
