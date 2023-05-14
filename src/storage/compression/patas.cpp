#include "duckdb/storage/compression/patas/patas.hpp"
#include "duckdb/storage/compression/patas/patas_compress.hpp"
#include "duckdb/storage/compression/patas/patas_scan.hpp"
#include "duckdb/storage/compression/patas/patas_fetch.hpp"
#include "duckdb/storage/compression/patas/patas_analyze.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include <functional>

namespace duckdb {

template <class T>
CompressionFunction GetPatasFunction(PhysicalType data_type) {
	throw NotImplementedException("GetPatasFunction not implemented for the given datatype");
}

template <>
CompressionFunction GetPatasFunction<float>(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_PATAS, data_type, PatasInitAnalyze<float>,
	                           PatasAnalyze<float>, PatasFinalAnalyze<float>, PatasInitCompression<float>,
	                           PatasCompress<float>, PatasFinalizeCompress<float>, PatasInitScan<float>,
	                           PatasScan<float>, PatasScanPartial<float>, PatasFetchRow<float>, PatasSkip<float>);
}

template <>
CompressionFunction GetPatasFunction<double>(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_PATAS, data_type, PatasInitAnalyze<double>,
	                           PatasAnalyze<double>, PatasFinalAnalyze<double>, PatasInitCompression<double>,
	                           PatasCompress<double>, PatasFinalizeCompress<double>, PatasInitScan<double>,
	                           PatasScan<double>, PatasScanPartial<double>, PatasFetchRow<double>, PatasSkip<double>);
}

CompressionFunction PatasCompressionFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::FLOAT:
		return GetPatasFunction<float>(type);
	case PhysicalType::DOUBLE:
		return GetPatasFunction<double>(type);
	default:
		throw InternalException("Unsupported type for Patas");
	}
}

bool PatasCompressionFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
