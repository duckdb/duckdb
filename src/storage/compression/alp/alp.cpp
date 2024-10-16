#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/compression/alp/alp_analyze.hpp"
#include "duckdb/storage/compression/alp/alp_compress.hpp"
#include "duckdb/storage/compression/alp/alp_fetch.hpp"
#include "duckdb/storage/compression/alp/alp_scan.hpp"

namespace duckdb {

template <class T>
CompressionFunction GetAlpFunction(PhysicalType data_type) {
	throw NotImplementedException("GetAlpFunction not implemented for the given datatype");
}

template <>
CompressionFunction GetAlpFunction<float>(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_ALP, data_type, AlpInitAnalyze<float>, AlpAnalyze<float>,
	                           AlpFinalAnalyze<float>, AlpInitCompression<float>, AlpCompress<float>,
	                           AlpFinalizeCompress<float>, AlpInitScan<float>, AlpScan<float>, AlpScanPartial<float>,
	                           AlpFetchRow<float>, AlpSkip<float>);
}

template <>
CompressionFunction GetAlpFunction<double>(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_ALP, data_type, AlpInitAnalyze<double>, AlpAnalyze<double>,
	                           AlpFinalAnalyze<double>, AlpInitCompression<double>, AlpCompress<double>,
	                           AlpFinalizeCompress<double>, AlpInitScan<double>, AlpScan<double>,
	                           AlpScanPartial<double>, AlpFetchRow<double>, AlpSkip<double>);
}

CompressionFunction AlpCompressionFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::FLOAT:
		return GetAlpFunction<float>(type);
	case PhysicalType::DOUBLE:
		return GetAlpFunction<double>(type);
	default:
		throw InternalException("Unsupported type for Alp");
	}
}

bool AlpCompressionFun::TypeIsSupported(const PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
