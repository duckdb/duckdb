#include "duckdb_python/numpy/numpy_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/string_util.hpp"
#include <exception>

namespace duckdb {

static bool IsDateTime(NumpyNullableType type) {
	switch (type) {
	case NumpyNullableType::DATETIME_NS:
	case NumpyNullableType::DATETIME_S:
	case NumpyNullableType::DATETIME_MS:
	case NumpyNullableType::DATETIME_US:
		return true;
	default:
		return false;
	};
}

static NumpyNullableType ConvertNumpyTypeInternal(const string &col_type_str) {
	if (col_type_str == "bool" || col_type_str == "boolean") {
		return NumpyNullableType::BOOL;
	}
	if (col_type_str == "uint8" || col_type_str == "UInt8") {
		return NumpyNullableType::UINT_8;
	}
	if (col_type_str == "uint16" || col_type_str == "UInt16") {
		return NumpyNullableType::UINT_16;
	}
	if (col_type_str == "uint32" || col_type_str == "UInt32") {
		return NumpyNullableType::UINT_32;
	}
	if (col_type_str == "uint64" || col_type_str == "UInt64") {
		return NumpyNullableType::UINT_64;
	}
	if (col_type_str == "int8" || col_type_str == "Int8") {
		return NumpyNullableType::INT_8;
	}
	if (col_type_str == "int16" || col_type_str == "Int16") {
		return NumpyNullableType::INT_16;
	}
	if (col_type_str == "int32" || col_type_str == "Int32") {
		return NumpyNullableType::INT_32;
	}
	if (col_type_str == "int64" || col_type_str == "Int64") {
		return NumpyNullableType::INT_64;
	}
	if (col_type_str == "float16" || col_type_str == "Float16") {
		return NumpyNullableType::FLOAT_16;
	}
	if (col_type_str == "float32" || col_type_str == "Float32") {
		return NumpyNullableType::FLOAT_32;
	}
	if (col_type_str == "float64" || col_type_str == "Float64") {
		return NumpyNullableType::FLOAT_64;
	}
	if (col_type_str == "string") {
		return NumpyNullableType::STRING;
	}
	if (col_type_str == "object") {
		return NumpyNullableType::OBJECT;
	}
	if (col_type_str == "timedelta64[ns]") {
		return NumpyNullableType::TIMEDELTA;
	}
	// We use 'StartsWith' because it might have ', tz' at the end, indicating timezone
	if (StringUtil::StartsWith(col_type_str, "datetime64[ns")) {
		return NumpyNullableType::DATETIME_NS;
	}
	if (StringUtil::StartsWith(col_type_str, "datetime64[us")) {
		return NumpyNullableType::DATETIME_US;
	}
	if (StringUtil::StartsWith(col_type_str, "datetime64[ms")) {
		return NumpyNullableType::DATETIME_MS;
	}
	if (StringUtil::StartsWith(col_type_str, "datetime64[s")) {
		return NumpyNullableType::DATETIME_S;
	}
	// Legacy datetime type indicators
	if (StringUtil::StartsWith(col_type_str, "<M8[ns")) {
		return NumpyNullableType::DATETIME_NS;
	}
	if (StringUtil::StartsWith(col_type_str, "<M8[s")) {
		return NumpyNullableType::DATETIME_S;
	}
	if (StringUtil::StartsWith(col_type_str, "<M8[us")) {
		return NumpyNullableType::DATETIME_US;
	}
	if (StringUtil::StartsWith(col_type_str, "<M8[ms")) {
		return NumpyNullableType::DATETIME_MS;
	}
	if (col_type_str == "category") {
		return NumpyNullableType::CATEGORY;
	}
	throw NotImplementedException("Data type '%s' not recognized", col_type_str);
}

NumpyType ConvertNumpyType(const py::handle &col_type) {
	auto col_type_str = string(py::str(col_type));
	NumpyType numpy_type;

	numpy_type.type = ConvertNumpyTypeInternal(col_type_str);
	if (IsDateTime(numpy_type.type)) {
		if (hasattr(col_type, "tz")) {
			// The datetime has timezone information.
			numpy_type.has_timezone = true;
		}
	}
	return numpy_type;
}

LogicalType NumpyToLogicalType(const NumpyType &col_type) {
	switch (col_type.type) {
	case NumpyNullableType::BOOL:
		return LogicalType::BOOLEAN;
	case NumpyNullableType::INT_8:
		return LogicalType::TINYINT;
	case NumpyNullableType::UINT_8:
		return LogicalType::UTINYINT;
	case NumpyNullableType::INT_16:
		return LogicalType::SMALLINT;
	case NumpyNullableType::UINT_16:
		return LogicalType::USMALLINT;
	case NumpyNullableType::INT_32:
		return LogicalType::INTEGER;
	case NumpyNullableType::UINT_32:
		return LogicalType::UINTEGER;
	case NumpyNullableType::INT_64:
		return LogicalType::BIGINT;
	case NumpyNullableType::UINT_64:
		return LogicalType::UBIGINT;
	case NumpyNullableType::FLOAT_16:
		return LogicalType::FLOAT;
	case NumpyNullableType::FLOAT_32:
		return LogicalType::FLOAT;
	case NumpyNullableType::FLOAT_64:
		return LogicalType::DOUBLE;
	case NumpyNullableType::STRING:
		return LogicalType::VARCHAR;
	case NumpyNullableType::OBJECT:
		return LogicalType::VARCHAR;
	case NumpyNullableType::TIMEDELTA:
		return LogicalType::INTERVAL;
	case NumpyNullableType::DATETIME_MS: {
		if (col_type.has_timezone) {
			return LogicalType::TIMESTAMP_TZ;
		}
		return LogicalType::TIMESTAMP_MS;
	}
	case NumpyNullableType::DATETIME_NS: {
		if (col_type.has_timezone) {
			return LogicalType::TIMESTAMP_TZ;
		}
		return LogicalType::TIMESTAMP_NS;
	}
	case NumpyNullableType::DATETIME_S: {
		if (col_type.has_timezone) {
			return LogicalType::TIMESTAMP_TZ;
		}
		return LogicalType::TIMESTAMP_S;
	}
	case NumpyNullableType::DATETIME_US: {
		if (col_type.has_timezone) {
			return LogicalType::TIMESTAMP_TZ;
		}
		return LogicalType::TIMESTAMP;
	}
	default:
		throw InternalException("No known conversion for NumpyNullableType '%d' to LogicalType");
	}
}

} // namespace duckdb
