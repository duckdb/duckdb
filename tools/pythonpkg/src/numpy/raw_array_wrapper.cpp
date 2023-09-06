#include "duckdb_python/numpy/raw_array_wrapper.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/python_objects.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

RawArrayWrapper::RawArrayWrapper(const LogicalType &type) : data(nullptr), type(type), count(0) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		type_width = sizeof(bool);
		break;
	case LogicalTypeId::UTINYINT:
		type_width = sizeof(uint8_t);
		break;
	case LogicalTypeId::USMALLINT:
		type_width = sizeof(uint16_t);
		break;
	case LogicalTypeId::UINTEGER:
		type_width = sizeof(uint32_t);
		break;
	case LogicalTypeId::UBIGINT:
		type_width = sizeof(uint64_t);
		break;
	case LogicalTypeId::TINYINT:
		type_width = sizeof(int8_t);
		break;
	case LogicalTypeId::SMALLINT:
		type_width = sizeof(int16_t);
		break;
	case LogicalTypeId::INTEGER:
		type_width = sizeof(int32_t);
		break;
	case LogicalTypeId::BIGINT:
		type_width = sizeof(int64_t);
		break;
	case LogicalTypeId::FLOAT:
		type_width = sizeof(float);
		break;
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		type_width = sizeof(double);
		break;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::TIMESTAMP_TZ:
		type_width = sizeof(int64_t);
		break;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BIT:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::UNION:
	case LogicalTypeId::UUID:
		type_width = sizeof(PyObject *);
		break;
	default:
		throw NotImplementedException("Unsupported type \"%s\" for DuckDB -> NumPy conversion", type.ToString());
	}
}

string RawArrayWrapper::DuckDBToNumpyDtype(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "bool";
	case LogicalTypeId::TINYINT:
		return "int8";
	case LogicalTypeId::SMALLINT:
		return "int16";
	case LogicalTypeId::INTEGER:
		return "int32";
	case LogicalTypeId::BIGINT:
		return "int64";
	case LogicalTypeId::UTINYINT:
		return "uint8";
	case LogicalTypeId::USMALLINT:
		return "uint16";
	case LogicalTypeId::UINTEGER:
		return "uint32";
	case LogicalTypeId::UBIGINT:
		return "uint64";
	case LogicalTypeId::FLOAT:
		return "float32";
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return "float64";
	case LogicalTypeId::TIMESTAMP:
		return "datetime64[us]";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "datetime64[us]";
	case LogicalTypeId::TIMESTAMP_NS:
		return "datetime64[ns]";
	case LogicalTypeId::TIMESTAMP_MS:
		return "datetime64[ms]";
	case LogicalTypeId::TIMESTAMP_SEC:
		return "datetime64[s]";
	case LogicalTypeId::DATE:
		// FIXME: should this not be 'date64[ns]' ?
		return "datetime64[ns]";
	case LogicalTypeId::INTERVAL:
		return "timedelta64[ns]";
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BIT:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::UNION:
	case LogicalTypeId::UUID:
		return "object";
	case LogicalTypeId::ENUM: {
		auto size = EnumType::GetSize(type);
		if (size <= (idx_t)NumericLimits<int8_t>::Maximum()) {
			return "int8";
		} else if (size <= (idx_t)NumericLimits<int16_t>::Maximum()) {
			return "int16";
		} else if (size <= (idx_t)NumericLimits<int32_t>::Maximum()) {
			return "int32";
		} else {
			throw InternalException("Size not supported on ENUM types");
		}
	}
	default:
		throw NotImplementedException("Unsupported type \"%s\"", type.ToString());
	}
}

void RawArrayWrapper::Initialize(idx_t capacity) {
	string dtype = DuckDBToNumpyDtype(type);

	array = py::array(py::dtype(dtype), capacity);
	data = data_ptr_cast(array.mutable_data());
}

void RawArrayWrapper::Resize(idx_t new_capacity) {
	vector<py::ssize_t> new_shape {py::ssize_t(new_capacity)};
	array.resize(new_shape, false);
	data = data_ptr_cast(array.mutable_data());
}

} // namespace duckdb
