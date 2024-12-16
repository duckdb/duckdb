#include "duckdb/function/compression/compression.hpp"
#include "duckdb/storage/compression/empty_validity.hpp"

namespace duckdb {

CompressionFunction EmptyValidityCompressionFun::GetFunction(PhysicalType type) {
	return EmptyValidityCompression::CreateFunction();
}

bool EmptyValidityCompressionFun::TypeIsSupported(const PhysicalType physical_type) {
	D_ASSERT(physical_type == PhysicalType::BIT);
	return true;
}

} // namespace duckdb
