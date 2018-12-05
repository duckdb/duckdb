#include "common/types.hpp"

#include "common/exception.hpp"

using namespace std;

namespace duckdb {

size_t GetTypeIdSize(TypeId type) {
	switch (type) {
	case TypeId::PARAMETER_OFFSET:
		return sizeof(uint64_t);
	case TypeId::BOOLEAN:
		return sizeof(bool);
	case TypeId::TINYINT:
		return sizeof(int8_t);
	case TypeId::SMALLINT:
		return sizeof(int16_t);
	case TypeId::INTEGER:
		return sizeof(int32_t);
	case TypeId::BIGINT:
		return sizeof(int64_t);
	case TypeId::DECIMAL:
		return sizeof(double);
	case TypeId::POINTER:
		return sizeof(uint64_t);
	case TypeId::TIMESTAMP:
		return sizeof(int64_t);
	case TypeId::DATE:
		return sizeof(int32_t);
	case TypeId::VARCHAR:
		return sizeof(void *);
	case TypeId::VARBINARY:
		return sizeof(void *);
	case TypeId::ARRAY:
		return sizeof(void *);
	case TypeId::UDT:
		return sizeof(void *);
	default:
		throw OutOfRangeException("Invalid type ID size!");
	}
}

bool TypeIsConstantSize(TypeId type) {
	return type < TypeId::VARCHAR;
}
bool TypeIsIntegral(TypeId type) {
	return type >= TypeId::TINYINT && type <= TypeId::TIMESTAMP;
}
bool TypeIsNumeric(TypeId type) {
	return type >= TypeId::TINYINT && type <= TypeId::DECIMAL;
}
bool TypeIsInteger(TypeId type) {
	return type >= TypeId::TINYINT && type <= TypeId::BIGINT;
}

} // namespace duckdb
