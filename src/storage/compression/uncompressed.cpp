#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/function/compression/compression.hpp"

namespace duckdb {

CompressionFunction UncompressedFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::UINT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::LIST:
	case PhysicalType::INTERVAL:
		return FixedSizeUncompressed::GetFunction(type);
	case PhysicalType::BIT:
		return ValidityUncompressed::GetFunction(type);
	case PhysicalType::VARCHAR:
		return StringUncompressed::GetFunction(type);
	default:
		throw InternalException("Unsupported type for Uncompressed");
	}
}

bool UncompressedFun::TypeIsSupported(const PhysicalType) {
	return true;
}

} // namespace duckdb
