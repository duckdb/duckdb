#include "duckdb_python/pandas_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/to_string.hpp"
#include <exception>

namespace duckdb {

PandasType GetPandasType(py::handle dtype) {
	int64_t extended_type;

	if (py::hasattr(dtype, "num")) {
		extended_type = py::int_(dtype.attr("num"));
	} else {
		auto type_str = string(py::str(dtype));
		if (type_str == "boolean") {
			return PandasType::PANDA_BOOL;
		} else if (type_str == "category") {
			return PandasType::PANDA_CATEGORY;
		} else if (type_str == "Int8") {
			return PandasType::PANDA_INT8;
		} else if (type_str == "Int16") {
			return PandasType::PANDA_INT16;
		} else if (type_str == "Int32") {
			return PandasType::PANDA_INT32;
		} else if (type_str == "Int64") {
			return PandasType::PANDA_INT64;
		} else if (type_str == "UInt8") {
			return PandasType::PANDA_UINT8;
		} else if (type_str == "UInt16") {
			return PandasType::PANDA_UINT16;
		} else if (type_str == "UInt32") {
			return PandasType::PANDA_UINT32;
		} else if (type_str == "UInt64") {
			return PandasType::PANDA_UINT64;
		} else if (type_str == "Float32") {
			return PandasType::PANDA_FLOAT32;
		} else if (type_str == "Float64") {
			return PandasType::PANDA_FLOAT64;
		} else if (type_str == "string") {
			return PandasType::PANDA_STRING;
		} else {
			throw std::runtime_error("Unknown dtype (" + type_str + ")");
		}
	}
	// 100 (PANDA_EXTENSION_TYPE) is potentially used by multiple dtypes, need to figure out which one it is exactly.
	if (extended_type == (int64_t)PandasType::PANDA_EXTENSION_TYPE) {
		auto extension_type_str = string(py::str(dtype));
		if (extension_type_str == "category") {
			return PandasType::PANDA_CATEGORY;
		} else {
			throw std::runtime_error("Unknown extension dtype (" + extension_type_str + ")");
		}
	}
	// Since people can extend the dtypes with their own custom stuff, it's probably best to check if it falls out of
	// the supported range of dtypes. (little hardcoded though)
	if (!(extended_type >= 0 && extended_type <= 23) && !(extended_type >= 100 && extended_type <= 103)) {
		throw std::runtime_error("Dtype num " + to_string(extended_type) + " is not supported");
	}
	return (PandasType)extended_type;
}

LogicalType ConvertPandasType(const PandasType &col_type) {
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
	case PandasType::TIMEDELTA: {
		return LogicalType::INTERVAL;
	}
	case PandasType::PANDA_DATETIME:
	case PandasType::DATETIME: {
		return LogicalType::TIMESTAMP;
	}
	default: {
		throw std::runtime_error("Failed to convert dtype num " + to_string((uint8_t)col_type) +
		                         " to duckdb LogicalType");
	}
	}
}

} // namespace duckdb
