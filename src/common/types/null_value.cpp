#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/string_type.hpp"

#include "duckdb/common/exception.hpp"

#include <cstring>

using namespace std;

namespace duckdb {

bool IsNullValue(data_ptr_t ptr, PhysicalType type) {
	data_t data[100];
	SetNullValue(data, type);
	return memcmp(ptr, data, GetTypeIdSize(type)) == 0;
}

//! Writes NullValue<T> value of a specific type to a memory address
void SetNullValue(data_ptr_t ptr, PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		*((int8_t *)ptr) = NullValue<int8_t>();
		break;
	case PhysicalType::INT16:
		*((int16_t *)ptr) = NullValue<int16_t>();
		break;
	case PhysicalType::INT32:
		*((int32_t *)ptr) = NullValue<int32_t>();
		break;
	case PhysicalType::INT64:
		*((int64_t *)ptr) = NullValue<int64_t>();
		break;
	case PhysicalType::FLOAT:
		*((float *)ptr) = NullValue<float>();
		break;
	case PhysicalType::DOUBLE:
		*((double *)ptr) = NullValue<double>();
		break;
	case PhysicalType::VARCHAR:
		*((string_t *)ptr) = string_t(NullValue<const char *>());
		break;
	default:
		throw InvalidTypeException(type, "Unsupported type for SetNullValue!");
	}
}

} // namespace duckdb
