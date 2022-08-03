#include "duckdb_python/pandas_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/string_util.hpp"
#include <exception>

namespace duckdb {

PandasType ConvertPandasType(const py::object &col_type) {
	auto col_type_str = string(py::str(col_type));

	if (col_type_str == "bool" || col_type_str == "boolean") {
		return PandasType::BOOL;
	} else if (col_type_str == "uint8" || col_type_str == "Uint8") {
		return PandasType::UINT_8;
	} else if (col_type_str == "uint16" || col_type_str == "Uint16") {
		return PandasType::UINT_16;
	} else if (col_type_str == "uint32" || col_type_str == "Uint32") {
		return PandasType::UINT_32;
	} else if (col_type_str == "uint64" || col_type_str == "Uint64") {
		return PandasType::UINT_64;
	} else if (col_type_str == "int8" || col_type_str == "Int8") {
		return PandasType::INT_8;
	} else if (col_type_str == "int16" || col_type_str == "Int16") {
		return PandasType::INT_16;
	} else if (col_type_str == "int32" || col_type_str == "Int32") {
		return PandasType::INT_32;
	} else if (col_type_str == "int64" || col_type_str == "Int64") {
		return PandasType::INT_64;
	} else if (col_type_str == "float32" || col_type_str == "Float32") {
		return PandasType::FLOAT_32;
	} else if (col_type_str == "float64" || col_type_str == "Float64") {
		return PandasType::FLOAT_64;
	} else if (col_type_str == "object" || col_type_str == "string") {
		//! this better be castable to strings
		return PandasType::OBJECT;
	} else if (col_type_str == "timedelta64[ns]") {
		return PandasType::TIMEDELTA;
	} else if (StringUtil::StartsWith(col_type_str, "datetime64[ns") || col_type_str == "<M8[ns]") {
		if (hasattr(col_type, "tz")) {
			// The datetime has timezone information.
			return PandasType::DATETIME_TZ;
		}
		return PandasType::DATETIME;
	} else if (col_type_str == "category") {
		return PandasType::CATEGORY;
	} else {
		throw NotImplementedException("Data type '%s' not recognized", col_type_str);
	}
}

LogicalType PandasToLogicalType(const PandasType &col_type) {
	switch (col_type) {
	case PandasType::BOOL:
		return LogicalType::BOOLEAN;
	case PandasType::INT_8:
		return LogicalType::TINYINT;
	case PandasType::UINT_8:
		return LogicalType::UTINYINT;
	case PandasType::INT_16:
		return LogicalType::SMALLINT;
	case PandasType::UINT_16:
		return LogicalType::USMALLINT;
	case PandasType::INT_32:
		return LogicalType::INTEGER;
	case PandasType::UINT_32:
		return LogicalType::UINTEGER;
	case PandasType::INT_64:
		return LogicalType::BIGINT;
	case PandasType::UINT_64:
		return LogicalType::UBIGINT;
	case PandasType::FLOAT_32:
		return LogicalType::FLOAT;
	case PandasType::FLOAT_64:
		return LogicalType::DOUBLE;
	case PandasType::OBJECT:
		return LogicalType::VARCHAR;
	case PandasType::TIMEDELTA:
		return LogicalType::INTERVAL;
	case PandasType::DATETIME:
		return LogicalType::TIMESTAMP;
	case PandasType::DATETIME_TZ:
		return LogicalType::TIMESTAMP_TZ;
	default:
		throw InternalException("No known conversion for PandasType '%d' to LogicalType");
	}
}

} // namespace duckdb
