#include "duckdb/common/chimp/chimp.hpp"
#include "duckdb/common/chimp/chimp_compress.hpp"
#include "duckdb/common/chimp/chimp_scan.hpp"
#include "duckdb/common/chimp/chimp_fetch.hpp"
#include "duckdb/common/chimp/chimp_analyze.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include <functional>

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

bool ChimpCompressionFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
