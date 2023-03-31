#include "duckdb_python/numpy/numpy_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/string_util.hpp"
#include <exception>

namespace duckdb {

static bool IsDateTime(const string &col_type_str) {
	if (StringUtil::StartsWith(col_type_str, "datetime64[ns")) {
		return true;
	}
	if (StringUtil::StartsWith(col_type_str, "datetime64[us")) {
		return true;
	}
	if (StringUtil::StartsWith(col_type_str, "datetime64[ms")) {
		return true;
	}
	if (StringUtil::StartsWith(col_type_str, "datetime64[s")) {
		return true;
	}
	if (col_type_str == "<M8[ns]") {
		return true;
	}
	return false;
}

NumpyNullableType ConvertNumpyType(const py::handle &col_type) {
	auto col_type_str = string(py::str(col_type));

	// pandas.core.dtypes.base.StorageExtensionDtype
	// this object also contains a 'numpy_dtype' attribute, which might be useful

	if (col_type_str == "bool" || col_type_str == "boolean") {
		return NumpyNullableType::BOOL;
	} else if (col_type_str == "uint8" || col_type_str == "UInt8") {
		return NumpyNullableType::UINT_8;
	} else if (col_type_str == "uint16" || col_type_str == "UInt16") {
		return NumpyNullableType::UINT_16;
	} else if (col_type_str == "uint32" || col_type_str == "UInt32") {
		return NumpyNullableType::UINT_32;
	} else if (col_type_str == "uint64" || col_type_str == "UInt64") {
		return NumpyNullableType::UINT_64;
	} else if (col_type_str == "int8" || col_type_str == "Int8") {
		return NumpyNullableType::INT_8;
	} else if (col_type_str == "int16" || col_type_str == "Int16") {
		return NumpyNullableType::INT_16;
	} else if (col_type_str == "int32" || col_type_str == "Int32") {
		return NumpyNullableType::INT_32;
	} else if (col_type_str == "int64" || col_type_str == "Int64") {
		return NumpyNullableType::INT_64;
	} else if (col_type_str == "float16" || col_type_str == "Float16") {
		return NumpyNullableType::FLOAT_16;
	} else if (col_type_str == "float32" || col_type_str == "Float32") {
		return NumpyNullableType::FLOAT_32;
	} else if (col_type_str == "float64" || col_type_str == "Float64") {
		return NumpyNullableType::FLOAT_64;
	} else if (col_type_str == "object" || col_type_str == "string") {
		//! this better be castable to strings
		return NumpyNullableType::OBJECT;
	} else if (col_type_str == "timedelta64[ns]") {
		return NumpyNullableType::TIMEDELTA;
	} else if (IsDateTime(col_type_str)) {
		if (hasattr(col_type, "tz")) {
			// The datetime has timezone information.
			return NumpyNullableType::DATETIME_TZ;
		}
		return NumpyNullableType::DATETIME;
	} else if (col_type_str == "category") {
		return NumpyNullableType::CATEGORY;
	} else {
		throw NotImplementedException("Data type '%s' not recognized", col_type_str);
	}
}

LogicalType NumpyToLogicalType(const NumpyNullableType &col_type) {
	switch (col_type) {
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
	case NumpyNullableType::OBJECT:
		return LogicalType::VARCHAR;
	case NumpyNullableType::TIMEDELTA:
		return LogicalType::INTERVAL;
	case NumpyNullableType::DATETIME:
		return LogicalType::TIMESTAMP;
	case NumpyNullableType::DATETIME_TZ:
		return LogicalType::TIMESTAMP_TZ;
	default:
		throw InternalException("No known conversion for NumpyNullableType '%d' to LogicalType");
	}
}

} // namespace duckdb
