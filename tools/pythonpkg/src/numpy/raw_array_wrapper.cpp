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

static idx_t GetNumpyTypeWidth(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return sizeof(bool);
	case LogicalTypeId::UTINYINT:
		return sizeof(uint8_t);
	case LogicalTypeId::USMALLINT:
		return sizeof(uint16_t);
	case LogicalTypeId::UINTEGER:
		return sizeof(uint32_t);
	case LogicalTypeId::UBIGINT:
		return sizeof(uint64_t);
	case LogicalTypeId::TINYINT:
		return sizeof(int8_t);
	case LogicalTypeId::SMALLINT:
		return sizeof(int16_t);
	case LogicalTypeId::INTEGER:
		return sizeof(int32_t);
	case LogicalTypeId::BIGINT:
		return sizeof(int64_t);
	case LogicalTypeId::FLOAT:
		return sizeof(float);
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return sizeof(double);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::TIMESTAMP_TZ:
		return sizeof(int64_t);
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
	case LogicalTypeId::ARRAY:
		return sizeof(PyObject *);
	default:
		throw NotImplementedException("Unsupported type \"%s\" for DuckDB -> NumPy conversion", type.ToString());
	}
}

RawArrayWrapper::RawArrayWrapper(const LogicalType &type) : data(nullptr), type(type), count(0) {
	type_width = GetNumpyTypeWidth(type);
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
		return "datetime64[us]";
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
	case LogicalTypeId::ARRAY:
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
