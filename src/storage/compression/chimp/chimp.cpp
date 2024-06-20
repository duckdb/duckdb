#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/compression/chimp/chimp_analyze.hpp"
#include "duckdb/storage/compression/chimp/chimp_compress.hpp"
#include "duckdb/storage/compression/chimp/chimp_fetch.hpp"
#include "duckdb/storage/compression/chimp/chimp_scan.hpp"

namespace duckdb {

template <class T>
CompressionFunction GetChimpFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_CHIMP, data_type, ChimpInitAnalyze<T>, ChimpAnalyze<T>,
	                           ChimpFinalAnalyze<T>, ChimpInitCompression<T>, ChimpCompress<T>,
	                           ChimpFinalizeCompress<T>, ChimpInitScan<T>, ChimpScan<T>, ChimpScanPartial<T>,
	                           ChimpFetchRow<T>, ChimpSkip<T>);
}

CompressionFunction ChimpCompressionFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::FLOAT:
		return GetChimpFunction<float>(type);
	case PhysicalType::DOUBLE:
		return GetChimpFunction<double>(type);
	default:
		throw InternalException("Unsupported type for Chimp");
	}
}

bool ChimpCompressionFun::TypeIsSupported(const CompressionInfo &info) {
	switch (info.GetPhysicalType()) {
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
