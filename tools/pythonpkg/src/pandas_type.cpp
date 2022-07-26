#include "duckdb_python/pandas_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/string_util.hpp"
#include <exception>

namespace duckdb {

PandasType ConvertPandasType(const string &col_type) {
	if (col_type == "bool") {
		return PandasType::BOOL;
	} else if (col_type == "boolean") {
		return PandasType::PANDA_BOOL;
	} else if (col_type == "uint8") {
		return PandasType::UINT_8;
	} else if (col_type == "Uint8") {
		return PandasType::PANDA_UINT8;
	} else if (col_type == "uint16") {
		return PandasType::UINT_16;
	} else if (col_type == "Uint16") {
		return PandasType::PANDA_UINT16;
	} else if (col_type == "uint32") {
		return PandasType::UINT_32;
	} else if (col_type == "Uint32") {
		return PandasType::PANDA_UINT32;
	} else if (col_type == "uint64") {
		return PandasType::UINT_64;
	} else if (col_type == "Uint64") {
		return PandasType::PANDA_UINT64;
	} else if (col_type == "int8") {
		return PandasType::INT_8;
	} else if (col_type == "Int8") {
		return PandasType::PANDA_INT8;
	} else if (col_type == "int16") {
		return PandasType::INT_16;
	} else if (col_type == "Int16") {
		return PandasType::PANDA_INT16;
	} else if (col_type == "int32") {
		return PandasType::INT_32;
	} else if (col_type == "Int32") {
		return PandasType::PANDA_INT32;
	} else if (col_type == "int64") {
		return PandasType::INT_64;
	} else if (col_type == "Int64") {
		return PandasType::PANDA_INT64;
	} else if (col_type == "float32") {
		return PandasType::FLOAT_32;
	} else if (col_type == "float64") {
		return PandasType::FLOAT_64;
	} else if (col_type == "Float32") {
		return PandasType::PANDA_FLOAT32;
	} else if (col_type == "Float64") {
		return PandasType::PANDA_FLOAT64;
	} else if (col_type == "object") {
		//! this better be castable to strings
		return PandasType::OBJECT;
	} else if (col_type == "string") {
		return PandasType::PANDA_STRING;
	} else if (col_type == "timedelta64[ns]") {
		return PandasType::PANDA_INTERVAL;
	} else if (StringUtil::StartsWith(col_type, "datetime64[ns") || col_type == "<M8[ns]") {
		return PandasType::PANDA_DATETIME;
	} else if (col_type == "category") {
		return PandasType::PANDA_CATEGORY;
	} else {
		throw NotImplementedException("Data type '%s' not recognized", col_type);
	}
}

LogicalType PandasToLogicalType(const PandasType &col_type) {
	switch (col_type) {
	case PandasType::BOOL:
	case PandasType::PANDA_BOOL: {
		return LogicalType::BOOLEAN;
	}
	case PandasType::PANDA_INT8:
	case PandasType::INT_8: {
		return LogicalType::TINYINT;
	}
	case PandasType::PANDA_UINT8:
	case PandasType::UINT_8: {
		return LogicalType::UTINYINT;
	}
	case PandasType::PANDA_INT16:
	case PandasType::INT_16: {
		return LogicalType::SMALLINT;
	}
	case PandasType::PANDA_UINT16:
	case PandasType::UINT_16: {
		return LogicalType::USMALLINT;
	}
	case PandasType::PANDA_INT32:
	case PandasType::INT_32: {
		return LogicalType::INTEGER;
	}
	case PandasType::PANDA_UINT32:
	case PandasType::UINT_32: {
		return LogicalType::UINTEGER;
	}
	case PandasType::PANDA_INT64:
	case PandasType::INT_64: {
		return LogicalType::BIGINT;
	}
	case PandasType::PANDA_UINT64:
	case PandasType::UINT_64: {
		return LogicalType::UBIGINT;
	}
	case PandasType::PANDA_FLOAT32:
	case PandasType::FLOAT_32: {
		return LogicalType::FLOAT;
	}
	case PandasType::PANDA_FLOAT64:
	case PandasType::LONG_FLOAT_64:
	case PandasType::FLOAT_64: {
		return LogicalType::DOUBLE;
	}
	case PandasType::OBJECT: {
		return LogicalType::VARCHAR;
	}
	case PandasType::PANDA_STRING: {
		return LogicalType::VARCHAR;
	}
	case PandasType::PANDA_INTERVAL:
	case PandasType::TIMEDELTA: {
		return LogicalType::INTERVAL;
	}
	case PandasType::PANDA_DATETIME:
	case PandasType::DATETIME: {
		return LogicalType::TIMESTAMP;
	}
	default: {
		throw InternalException("No known conversion for PandasType '%d' to LogicalType");
	}
	}
}

} // namespace duckdb
