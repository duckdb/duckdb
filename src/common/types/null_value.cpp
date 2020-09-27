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
		Store<int8_t>(NullValue<int8_t>(), ptr);
		break;
	case PhysicalType::INT16:
		Store<int16_t>(NullValue<int16_t>(), ptr);
		break;
	case PhysicalType::INT32:
		Store<int32_t>(NullValue<int32_t>(), ptr);
		break;
	case PhysicalType::INT64:
		Store<int64_t>(NullValue<int64_t>(), ptr);
		break;
	case PhysicalType::FLOAT:
		Store<float>(NullValue<float>(), ptr);
		break;
	case PhysicalType::DOUBLE:
		Store<double>(NullValue<double>(), ptr);
		break;
	case PhysicalType::VARCHAR:
		Store<string_t>(string_t(NullValue<const char *>()), ptr);
		break;
	default:
		throw InvalidTypeException(type, "Unsupported type for SetNullValue!");
	}
}

} // namespace duckdb
