#include "duckdb/storage/constant_segment.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/validity_statistics.hpp"

namespace duckdb {

static ConstantSegment::scan_function_t GetScanFunction(PhysicalType type);
static ConstantSegment::fetch_function_t GetFetchFunction(PhysicalType type);

ConstantSegment::ConstantSegment(DatabaseInstance &db, SegmentStatistics &stats, idx_t row_start)
    : UncompressedSegment(db, stats.type.InternalType(), row_start), stats(stats) {
	scan_function = GetScanFunction(stats.type.InternalType());
	fetch_function = GetFetchFunction(stats.type.InternalType());
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void ScanFunctionValidity(ConstantSegment &segment, Vector &result) {
	auto &validity = (ValidityStatistics &) *segment.stats.statistics;
	if (validity.has_null) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
	}
}

template<class T>
void ScanFunction(ConstantSegment &segment, Vector &result) {
	auto &nstats = (NumericStatistics &) *segment.stats.statistics;

	auto data = FlatVector::GetData<T>(result);
	data[0] = nstats.min.GetValueUnsafe<T>();
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
}

ConstantSegment::scan_function_t GetScanFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return ScanFunctionValidity;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return ScanFunction<int8_t>;
	case PhysicalType::INT16:
		return ScanFunction<int16_t>;
	case PhysicalType::INT32:
		return ScanFunction<int32_t>;
	case PhysicalType::INT64:
		return ScanFunction<int64_t>;
	case PhysicalType::UINT8:
		return ScanFunction<uint8_t>;
	case PhysicalType::UINT16:
		return ScanFunction<uint16_t>;
	case PhysicalType::UINT32:
		return ScanFunction<uint32_t>;
	case PhysicalType::UINT64:
		return ScanFunction<uint64_t>;
	case PhysicalType::INT128:
		return ScanFunction<hugeint_t>;
	case PhysicalType::FLOAT:
		return ScanFunction<float>;
	case PhysicalType::DOUBLE:
		return ScanFunction<double>;
	case PhysicalType::INTERVAL:
		return ScanFunction<interval_t>;
	default:
		throw NotImplementedException("Unimplemented type for constant segment");
	}
}

void ConstantSegment::InitializeScan(ColumnScanState &state) {
}

void ConstantSegment::Scan(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result, idx_t result_offset) {
	scan_function(*this, result);
}

//===--------------------------------------------------------------------===//
// Fetch Row
//===--------------------------------------------------------------------===//
void FetchFunctionValidity(ConstantSegment &segment, Vector &result, idx_t result_idx) {
	auto &validity = (ValidityStatistics &) *segment.stats.statistics;
	if (validity.has_null) {
		auto &mask = FlatVector::Validity(result);
		mask.SetInvalid(result_idx);
	}
}

template<class T>
void FetchFunction(ConstantSegment &segment, Vector &result, idx_t result_idx) {
	auto &nstats = (NumericStatistics &) *segment.stats.statistics;

	auto data = FlatVector::GetData<T>(result);
	data[result_idx] = nstats.min.GetValueUnsafe<T>();
}

ConstantSegment::fetch_function_t GetFetchFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return FetchFunctionValidity;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return FetchFunction<int8_t>;
	case PhysicalType::INT16:
		return FetchFunction<int16_t>;
	case PhysicalType::INT32:
		return FetchFunction<int32_t>;
	case PhysicalType::INT64:
		return FetchFunction<int64_t>;
	case PhysicalType::UINT8:
		return FetchFunction<uint8_t>;
	case PhysicalType::UINT16:
		return FetchFunction<uint16_t>;
	case PhysicalType::UINT32:
		return FetchFunction<uint32_t>;
	case PhysicalType::UINT64:
		return FetchFunction<uint64_t>;
	case PhysicalType::INT128:
		return FetchFunction<hugeint_t>;
	case PhysicalType::FLOAT:
		return FetchFunction<float>;
	case PhysicalType::DOUBLE:
		return FetchFunction<double>;
	case PhysicalType::INTERVAL:
		return FetchFunction<interval_t>;
	default:
		throw NotImplementedException("Unimplemented type for constant segment");
	}
}

void ConstantSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	fetch_function(*this, result, result_idx);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
idx_t ConstantSegment::Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) {
	throw InternalException("Cannot append to a constant segment");
}

}

