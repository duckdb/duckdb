#include "duckdb/function/compression/compression.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/validity_statistics.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
unique_ptr<SegmentScanState> ConstantInitScan(ColumnSegment &segment) {
	return nullptr;
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void ConstantScanFunctionValidity(ColumnSegment &segment, ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result) {
	auto &validity = (ValidityStatistics &)*segment.stats.statistics;
	if (validity.has_null) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
	}
}

template <class T>
void ConstantScanFunction(ColumnSegment &segment, ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result) {
	auto &nstats = (NumericStatistics &)*segment.stats.statistics;

	auto data = FlatVector::GetData<T>(result);
	data[0] = nstats.min.GetValueUnsafe<T>();
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
}

//===--------------------------------------------------------------------===//
// Scan Partial
//===--------------------------------------------------------------------===//
void FillFunctionValidity(ColumnSegment &segment, Vector &result, idx_t start_idx, idx_t count) {
	auto &validity = (ValidityStatistics &)*segment.stats.statistics;
	if (validity.has_null) {
		auto &mask = FlatVector::Validity(result);
		for (idx_t i = 0; i < count; i++) {
			mask.SetInvalid(start_idx + i);
		}
	}
}

template <class T>
void FillFunction(ColumnSegment &segment, Vector &result, idx_t start_idx, idx_t count) {
	auto &nstats = (NumericStatistics &)*segment.stats.statistics;

	auto data = FlatVector::GetData<T>(result);
	auto constant_value = nstats.min.GetValueUnsafe<T>();
	for (idx_t i = 0; i < count; i++) {
		data[start_idx + i] = constant_value;
	}
}

void ConstantScanPartialValidity(ColumnSegment &segment, ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result, idx_t result_offset) {
	FillFunctionValidity(segment, result, result_offset, scan_count);
}

template<class T>
void ConstantScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result, idx_t result_offset) {
	FillFunction<T>(segment, result, result_offset, scan_count);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void ConstantFetchRowValidity(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	FillFunctionValidity(segment, result, result_idx, 1);
}

template<class T>
void ConstantFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	FillFunction<T>(segment, result, result_idx, 1);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction ConstantGetFunctionValidity(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::BIT);
	return CompressionFunction(
		CompressionType::COMPRESSION_CONSTANT,
		data_type,
		nullptr,
		nullptr,
		nullptr,
		nullptr,
		nullptr,
		nullptr,
		ConstantInitScan,
		ConstantScanFunctionValidity,
		ConstantScanPartialValidity,
		ConstantFetchRowValidity,
		nullptr,
		nullptr,
		nullptr
	);
}

template<class T>
CompressionFunction ConstantGetFunction(PhysicalType data_type) {
	return CompressionFunction(
		CompressionType::COMPRESSION_CONSTANT,
		data_type,
		nullptr,
		nullptr,
		nullptr,
		nullptr,
		nullptr,
		nullptr,
		ConstantInitScan,
		ConstantScanFunction<T>,
		ConstantScanPartial<T>,
		ConstantFetchRow<T>,
		nullptr,
		nullptr,
		nullptr
	);
}

CompressionFunction ConstantFun::GetFunction(PhysicalType data_type) {
	switch(data_type) {
	case PhysicalType::BIT:
		return ConstantGetFunctionValidity(data_type);
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return ConstantGetFunction<int8_t>(data_type);
	case PhysicalType::INT16:
		return ConstantGetFunction<int16_t>(data_type);
	case PhysicalType::INT32:
		return ConstantGetFunction<int32_t>(data_type);
	case PhysicalType::INT64:
		return ConstantGetFunction<int64_t>(data_type);
	case PhysicalType::UINT8:
		return ConstantGetFunction<uint8_t>(data_type);
	case PhysicalType::UINT16:
		return ConstantGetFunction<uint16_t>(data_type);
	case PhysicalType::UINT32:
		return ConstantGetFunction<uint32_t>(data_type);
	case PhysicalType::UINT64:
		return ConstantGetFunction<uint64_t>(data_type);
	case PhysicalType::INT128:
		return ConstantGetFunction<hugeint_t>(data_type);
	case PhysicalType::FLOAT:
		return ConstantGetFunction<float>(data_type);
	case PhysicalType::DOUBLE:
		return ConstantGetFunction<double>(data_type);
	case PhysicalType::INTERVAL:
		return ConstantGetFunction<interval_t>(data_type);
	case PhysicalType::LIST:
		return ConstantGetFunction<list_entry_t>(data_type);
	default:
		throw InternalException("Unsupported type for ConstantUncompressed::GetFunction");
	}
}

bool ConstantFun::TypeIsSupported(PhysicalType type) {
	switch(type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::INTERVAL:
	case PhysicalType::LIST:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
