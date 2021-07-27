#include "duckdb/storage/constant_segment.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/validity_statistics.hpp"

namespace duckdb {

static ConstantSegment::scan_function_t GetScanFunction(PhysicalType type);
static ConstantSegment::fill_function_t GetFillFunction(PhysicalType type);

ConstantSegment::ConstantSegment(DatabaseInstance &db, SegmentStatistics &stats, idx_t row_start)
    : BaseSegment(db, stats.type.InternalType(), row_start), stats(stats) {
	scan_function = GetScanFunction(stats.type.InternalType());
	fill_function = GetFillFunction(stats.type.InternalType());
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void ScanFunctionValidity(ConstantSegment &segment, Vector &result) {
	auto &validity = (ValidityStatistics &)*segment.stats.statistics;
	if (validity.has_null) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
	}
}

template <class T>
void ScanFunction(ConstantSegment &segment, Vector &result) {
	auto &nstats = (NumericStatistics &)*segment.stats.statistics;

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

void ConstantSegment::Scan(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result) {
	// fresh scan: emit a constant vector
	scan_function(*this, result);
}

void ConstantSegment::ScanPartial(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result,
                                  idx_t result_offset) {
	// partial scan: fill in the rows as required
	fill_function(*this, result, result_offset, scan_count);
}

//===--------------------------------------------------------------------===//
// Fetch Row
//===--------------------------------------------------------------------===//
void FillFunctionValidity(ConstantSegment &segment, Vector &result, idx_t start_idx, idx_t count) {
	auto &validity = (ValidityStatistics &)*segment.stats.statistics;
	if (validity.has_null) {
		auto &mask = FlatVector::Validity(result);
		for (idx_t i = 0; i < count; i++) {
			mask.SetInvalid(start_idx + i);
		}
	}
}

template <class T>
void FillFunction(ConstantSegment &segment, Vector &result, idx_t start_idx, idx_t count) {
	auto &nstats = (NumericStatistics &)*segment.stats.statistics;

	auto data = FlatVector::GetData<T>(result);
	auto constant_value = nstats.min.GetValueUnsafe<T>();
	for (idx_t i = 0; i < count; i++) {
		data[start_idx + i] = constant_value;
	}
}

ConstantSegment::fill_function_t GetFillFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return FillFunctionValidity;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return FillFunction<int8_t>;
	case PhysicalType::INT16:
		return FillFunction<int16_t>;
	case PhysicalType::INT32:
		return FillFunction<int32_t>;
	case PhysicalType::INT64:
		return FillFunction<int64_t>;
	case PhysicalType::UINT8:
		return FillFunction<uint8_t>;
	case PhysicalType::UINT16:
		return FillFunction<uint16_t>;
	case PhysicalType::UINT32:
		return FillFunction<uint32_t>;
	case PhysicalType::UINT64:
		return FillFunction<uint64_t>;
	case PhysicalType::INT128:
		return FillFunction<hugeint_t>;
	case PhysicalType::FLOAT:
		return FillFunction<float>;
	case PhysicalType::DOUBLE:
		return FillFunction<double>;
	case PhysicalType::INTERVAL:
		return FillFunction<interval_t>;
	default:
		throw NotImplementedException("Unimplemented type for constant segment");
	}
}

void ConstantSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	fill_function(*this, result, result_idx, 1);
}

} // namespace duckdb
