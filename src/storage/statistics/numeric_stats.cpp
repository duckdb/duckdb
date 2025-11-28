#include "duckdb/storage/statistics/numeric_stats.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include <cstdio>
#include <atomic>

namespace duckdb {

// for column imprint
static atomic<bool> column_imprint_enabled {false};

// stats for column imprint
static atomic<idx_t> g_imprint_checks_total(0);
static atomic<idx_t> g_imprint_pruned_segments(0);
static atomic<idx_t> g_imprint_equality_checks(0);
static atomic<idx_t> g_imprint_greater_than_checks(0);
static atomic<idx_t> g_total_segments_checked(0);
static atomic<idx_t> g_total_segments_skipped(0);

// for debug
#define IMPRINT_LOG(msg) fprintf(stderr, "%s\n", (msg).c_str())
static atomic<uint64_t> imprint_prune_counter {0};

// stats getters
idx_t NumericStats::GetImprintChecksTotal() {
	return g_imprint_checks_total.load();
}

idx_t NumericStats::GetImprintPrunedSegments() {
	return g_imprint_pruned_segments.load();
}

idx_t NumericStats::GetImprintEqualityChecks() {
	return g_imprint_equality_checks.load();
}

idx_t NumericStats::GetImprintGreaterThanChecks() {
	return g_imprint_greater_than_checks.load();
}

void NumericStats::ResetImprintStatistics() {
	g_imprint_checks_total = 0;
	g_imprint_pruned_segments = 0;
	g_imprint_equality_checks = 0;
	g_imprint_greater_than_checks = 0;
	g_total_segments_checked = 0;
	g_total_segments_skipped = 0;
}

idx_t NumericStats::GetTotalSegmentsChecked() {
	return g_total_segments_checked.load();
}

idx_t NumericStats::GetTotalSegmentsSkipped() {
	return g_total_segments_skipped.load();
}

void NumericStats::IncrementSegmentsChecked() {
	g_total_segments_checked++;
}

void NumericStats::IncrementSegmentsSkipped() {
	g_total_segments_skipped++;
}

BaseStatistics NumericStats::CreateUnknown(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();
	SetMin(result, Value(result.GetType()));
	SetMax(result, Value(result.GetType()));

	ResetImprint(result);

	return result;
}

BaseStatistics NumericStats::CreateEmpty(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();
	SetMin(result, Value::MaximumValue(result.GetType()));
	SetMax(result, Value::MinimumValue(result.GetType()));

	ResetImprint(result);

	return result;
}

NumericStatsData &NumericStats::GetDataUnsafe(BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::NUMERIC_STATS);
	return stats.stats_union.numeric_data;
}

const NumericStatsData &NumericStats::GetDataUnsafe(const BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::NUMERIC_STATS);
	return stats.stats_union.numeric_data;
}

void NumericStats::SetImprint(BaseStatistics &stats, uint64_t bitmap, uint8_t bins) {
	auto &data = NumericStats::GetDataUnsafe(stats);
	data.imprint_bitmap = bitmap;
	data.imprint_bins = bins;
}

void NumericStats::ResetImprint(BaseStatistics &stats) {
	SetImprint(stats, 0, 0);
}

uint64_t NumericStats::GetImprintBitmapUnsafe(const BaseStatistics &stats) {
	return NumericStats::GetDataUnsafe(stats).imprint_bitmap;
}

uint8_t NumericStats::GetImprintBinsUnsafe(const BaseStatistics &stats) {
	return NumericStats::GetDataUnsafe(stats).imprint_bins;
}

// when bins > 0, confirm the imprint is live
bool NumericStats::HasImprint(const BaseStatistics &stats) {
	auto &data = NumericStats::GetDataUnsafe(stats);
	return data.imprint_bins > 0;
}

void NumericStats::SetColumnImprintEnabled(bool enabled) {
	column_imprint_enabled.store(enabled);
}

bool NumericStats::IsColumnImprintEnabled() {
	return column_imprint_enabled.load();
}

void NumericStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}
	D_ASSERT(stats.GetType() == other.GetType());
	if (NumericStats::HasMin(other) && NumericStats::HasMin(stats)) {
		auto other_min = NumericStats::Min(other);
		if (other_min < NumericStats::Min(stats)) {
			NumericStats::SetMin(stats, other_min);
		}
	} else {
		NumericStats::SetMin(stats, Value());
	}
	if (NumericStats::HasMax(other) && NumericStats::HasMax(stats)) {
		auto other_max = NumericStats::Max(other);
		if (other_max > NumericStats::Max(stats)) {
			NumericStats::SetMax(stats, other_max);
		}
	} else {
		NumericStats::SetMax(stats, Value());
	}
	// we assume imprints only exist on segments, so invalidate it for global stats
	ResetImprint(stats);
}

struct GetNumericValueUnion {
	template <class T>
	static T Operation(const NumericValueUnion &v);
};

template <>
int8_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.tinyint;
}

template <>
int16_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.smallint;
}

template <>
int32_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.integer;
}

template <>
int64_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.bigint;
}

template <>
hugeint_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.hugeint;
}

template <>
uhugeint_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.uhugeint;
}

template <>
uint8_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.utinyint;
}

template <>
uint16_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.usmallint;
}

template <>
uint32_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.uinteger;
}

template <>
uint64_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.ubigint;
}

template <>
float GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.float_;
}

template <>
double GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.double_;
}

template <class T>
T NumericStats::GetMinUnsafe(const BaseStatistics &stats) {
	return GetNumericValueUnion::Operation<T>(NumericStats::GetDataUnsafe(stats).min);
}

template <class T>
T NumericStats::GetMaxUnsafe(const BaseStatistics &stats) {
	return GetNumericValueUnion::Operation<T>(NumericStats::GetDataUnsafe(stats).max);
}

template <class T>
uint8_t NumericStats::ComputeBinIndex(T value, T min, T max, uint8_t bins) {
	if (min == max || bins <= 1) {
		return 0;
	}
	if (LessThanEquals::Operation(value, min)) {
		return 0;
	}
	if (GreaterThanEquals::Operation(value, max)) {
		return bins - 1;
	}
	long double range = static_cast<long double>(max) - static_cast<long double>(min);
	long double normalized = static_cast<long double>(value) - static_cast<long double>(min);
	auto idx = static_cast<uint8_t>((normalized / range) * bins);

	return idx;
}

// build a bitmap for a single segment from a column
template <class T>
static void BuildImprintForSegmentInternal(BaseStatistics &stats, ColumnData &col_data,
                                           SegmentNode<ColumnSegment> &segment_node) {
	// invalid the imprint if the segment does not have valid min/max stats
	if (!NumericStats::HasMinMax(stats)) {
		NumericStats::ResetImprint(stats);
		return;
	}
	auto min = NumericStats::GetMinUnsafe<T>(stats);
	auto max = NumericStats::GetMaxUnsafe<T>(stats);

	constexpr uint8_t bins = 64;
	uint64_t bitmap = 0;

	// if only one value exist, set the first bit
	if (min == max) {
		NumericStats::SetImprint(stats, 1ULL, bins);
		return;
	}

	// get the ref to the column segment passed in
	auto &segment = *segment_node.node;
	ColumnScanState scan_state(nullptr);
	scan_state.current = &segment_node;
	segment.InitializeScan(scan_state);

	// temp vector to hold a batch from the segment
	Vector scan_vector(col_data.type);

	// scan the entire segment
	for (idx_t base_row = 0; base_row < segment.count; base_row += STANDARD_VECTOR_SIZE) {
		// number of rows to read inside a batch
		const auto count = MinValue<idx_t>(segment.count - base_row, STANDARD_VECTOR_SIZE);

		// calculate the offset
		scan_state.offset_in_column = segment_node.row_start + base_row;

		// fill the scan_vector with the batch data
		col_data.CheckpointScan(segment, scan_state, count, scan_vector);

		// convert the vector to a unified format
		UnifiedVectorFormat vdata;
		scan_vector.ToUnifiedFormat(count, vdata);
		auto data_ptr = UnifiedVectorFormat::GetData<T>(vdata);

		// iterate over every value in the batch, compute the bin index and update bitmap
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				continue;
			}

			auto bin_index =
			    MinValue<uint8_t>(NumericStats::ComputeBinIndex<T>(data_ptr[idx], min, max, bins), bins - 1);
			bitmap |= (uint64_t(1) << bin_index);
		}
	}

	// set the imprint
	NumericStats::SetImprint(stats, bitmap, bins);

	// for debug
	IMPRINT_LOG(StringUtil::Format("[imprint-build] col_idx=%lld row_start=%lld count=%lld bins=%d bitmap=0x%016llx",
	                               static_cast<long long>(col_data.column_index),
	                               static_cast<long long>(segment_node.row_start),
	                               static_cast<long long>(segment.count), bins, static_cast<long long>(bitmap)));
}

template <class T>
bool ConstantExactRange(T min, T max, T constant) {
	return Equals::Operation(constant, min) && Equals::Operation(constant, max);
}

template <class T>
bool ConstantValueInRange(T min, T max, T constant) {
	return !(LessThan::Operation(constant, min) || GreaterThan::Operation(constant, max));
}

template <class T>
FilterPropagateResult CheckZonemapTemplated(const BaseStatistics &stats, ExpressionType comparison_type, T min_value,
                                            T max_value, T constant) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		if (ConstantExactRange(min_value, max_value, constant)) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
		if (ConstantValueInRange(min_value, max_value, constant)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_DISTINCT_FROM:
		if (!ConstantValueInRange(min_value, max_value, constant)) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (ConstantExactRange(min_value, max_value, constant)) {
			// corner case of a cluster with one numeric equal to the target constant
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// GreaterThanEquals::Operation(X, C)
		// this can be true only if max(X) >= C
		// if min(X) >= C, then this is always true
		if (GreaterThanEquals::Operation(min_value, constant)) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (GreaterThanEquals::Operation(max_value, constant)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_GREATERTHAN:
		// GreaterThan::Operation(X, C)
		// this can be true only if max(X) > C
		// if min(X) > C, then this is always true
		if (GreaterThan::Operation(min_value, constant)) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (GreaterThan::Operation(max_value, constant)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// LessThanEquals::Operation(X, C)
		// this can be true only if min(X) <= C
		// if max(X) <= C, then this is always true
		if (LessThanEquals::Operation(max_value, constant)) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (LessThanEquals::Operation(min_value, constant)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_LESSTHAN:
		// LessThan::Operation(X, C)
		// this can be true only if min(X) < C
		// if max(X) < C, then this is always true
		if (LessThan::Operation(max_value, constant)) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (LessThan::Operation(min_value, constant)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	default:
		throw InternalException("Expression type in zonemap check not implemented");
	}
}

template <class T>
FilterPropagateResult CheckZonemapTemplated(const BaseStatistics &stats, ExpressionType comparison_type,
                                            array_ptr<const Value> constants) {
	T min_value = NumericStats::GetMinUnsafe<T>(stats);
	T max_value = NumericStats::GetMaxUnsafe<T>(stats);
	for (auto &constant_value : constants) {
		D_ASSERT(constant_value.type() == stats.GetType());
		D_ASSERT(!constant_value.IsNull());
		T constant = constant_value.GetValueUnsafe<T>();
		auto prune_result = CheckZonemapTemplated(stats, comparison_type, min_value, max_value, constant);
		if (prune_result == FilterPropagateResult::NO_PRUNING_POSSIBLE) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else if (prune_result == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
	}
	return FilterPropagateResult::FILTER_ALWAYS_FALSE;
}

// for single value
template <class T>
FilterPropagateResult CheckImprintTemplated(const BaseStatistics &stats, ExpressionType comparison_type, T constant) {
	auto bins = NumericStats::GetImprintBinsUnsafe(stats);
	auto bitmap = NumericStats::GetImprintBitmapUnsafe(stats);

	// if no imprint stats exist, return
	if (bins == 0 || bitmap == 0) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	// stats counter - increment only when imprint is actually used
	g_imprint_checks_total++;

	auto min = NumericStats::GetMinUnsafe<T>(stats);
	auto max = NumericStats::GetMaxUnsafe<T>(stats);

	auto bin_index = MinValue<uint8_t>(NumericStats::ComputeBinIndex<T>(constant, min, max, bins), bins - 1);

	if (bin_index >= bins) {
		bin_index = bins - 1;
	}

	if (comparison_type == ExpressionType::COMPARE_EQUAL) {
		g_imprint_equality_checks++;

		// no matching bit, prune
		if ((bitmap & (uint64_t(1) << bin_index)) == 0) {
			g_imprint_pruned_segments++;

			string min_str = std::to_string(min);
			string max_str = std::to_string(max);
			IMPRINT_LOG(StringUtil::Format(
			    "[imprint-check][equal] prune: bitmap=0x%016llx bin=%d constant=%s min=%s max=%s", bitmap, bin_index,
			    Value::CreateValue(constant).ToString().c_str(), min_str.c_str(), max_str.c_str()));
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	} else if (comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
	           comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
		g_imprint_greater_than_checks++;

		// set all bits to 1's from [bin_index, bins-1]
		uint64_t all_bits =
		    (bins == 64) ? UINT64_MAX : ((1ULL << bins) - 1); // just in case we need to change the bin size later
		uint64_t lower_bits = (bin_index == 0) ? 0 : ((1ULL << bin_index) - 1);
		uint64_t greater_than_mask = all_bits & ~lower_bits;

		// print greater_than_mask for debugging
		string min_str = std::to_string(min);
		string max_str = std::to_string(max);
		IMPRINT_LOG(StringUtil::Format(
		    "[imprint-check][greater_than] stats: bitmap=0x%016llx bin_index=%d bins=%d all_bits=0x%016llx "
		    "lower_bits=0x%016llx greater_than_mask=0x%016llx constant=%s min=%s max=%s",
		    bitmap, bin_index, bins, all_bits, lower_bits, greater_than_mask,
		    Value::CreateValue(constant).ToString().c_str(), min_str.c_str(), max_str.c_str()));

		// if no matching bins, prune
		if ((bitmap & greater_than_mask) == 0) {
			g_imprint_pruned_segments++;

			IMPRINT_LOG(StringUtil::Format(
			    "[imprint-check][greater_than] prune: bitmap=0x%016llx bin=%d constant=%s min=%s max=%s", bitmap,
			    bin_index, Value::CreateValue(constant).ToString().c_str(), min_str.c_str(), max_str.c_str()));
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	}
	// TODO: ADD MORE CONDITIONS HERE

	// otherwise, still need to scan
	string min_str = std::to_string(min);
	string max_str = std::to_string(max);
	IMPRINT_LOG(StringUtil::Format("[imprint-check] need scanning: bitmap=0x%016llx bin=%d constant=%s min=%s max=%s",
	                               bitmap, bin_index, Value::CreateValue(constant).ToString().c_str(), min_str.c_str(),
	                               max_str.c_str()));
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

template <class T>
FilterPropagateResult CheckImprintTemplated(const BaseStatistics &stats, ExpressionType comparison_type,
                                            array_ptr<const Value> constants) {
	// check if the comparison type is supported
	if (comparison_type != ExpressionType::COMPARE_EQUAL &&
	    comparison_type != ExpressionType::COMPARE_NOT_DISTINCT_FROM &&
	    comparison_type != ExpressionType::COMPARE_GREATERTHAN &&
	    comparison_type != ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	// iterate over the array, check the value
	for (auto &constant_value : constants) {
		D_ASSERT(constant_value.type() == stats.GetType());

		if (constant_value.IsNull()) {
			continue;
		}

		auto prune_result = CheckImprintTemplated<T>(stats, comparison_type, constant_value.GetValueUnsafe<T>());

		if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
			auto prunes = imprint_prune_counter.fetch_add(1) + 1;
			IMPRINT_LOG(StringUtil::Format("[imprint-prune] total=%llu constant=%s",
			                               static_cast<long long unsigned>(prunes), constant_value.ToString()));
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

FilterPropagateResult NumericStats::CheckZonemap(const BaseStatistics &stats, ExpressionType comparison_type,
                                                 array_ptr<const Value> constants) {
	if (!NumericStats::HasMinMax(stats)) {
		// no zone map, no pruning
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	// first check zonemap (min/max)
	FilterPropagateResult zonemap_result;
	auto internal_type = stats.GetType().InternalType();
	switch (internal_type) {
	case PhysicalType::INT8:
		zonemap_result = CheckZonemapTemplated<int8_t>(stats, comparison_type, constants);
		break;
	case PhysicalType::INT16:
		zonemap_result = CheckZonemapTemplated<int16_t>(stats, comparison_type, constants);
		break;
	case PhysicalType::INT32:
		zonemap_result = CheckZonemapTemplated<int32_t>(stats, comparison_type, constants);
		break;
	case PhysicalType::INT64:
		zonemap_result = CheckZonemapTemplated<int64_t>(stats, comparison_type, constants);
		break;
	case PhysicalType::UINT8:
		zonemap_result = CheckZonemapTemplated<uint8_t>(stats, comparison_type, constants);
		break;
	case PhysicalType::UINT16:
		zonemap_result = CheckZonemapTemplated<uint16_t>(stats, comparison_type, constants);
		break;
	case PhysicalType::UINT32:
		zonemap_result = CheckZonemapTemplated<uint32_t>(stats, comparison_type, constants);
		break;
	case PhysicalType::UINT64:
		zonemap_result = CheckZonemapTemplated<uint64_t>(stats, comparison_type, constants);
		break;
	case PhysicalType::INT128:
		zonemap_result = CheckZonemapTemplated<hugeint_t>(stats, comparison_type, constants);
		break;
	case PhysicalType::UINT128:
		zonemap_result = CheckZonemapTemplated<uhugeint_t>(stats, comparison_type, constants);
		break;
	case PhysicalType::FLOAT:
		zonemap_result = CheckZonemapTemplated<float>(stats, comparison_type, constants);
		break;
	case PhysicalType::DOUBLE:
		zonemap_result = CheckZonemapTemplated<double>(stats, comparison_type, constants);
		break;
	default:
		throw InternalException("Unsupported type for NumericStats::CheckZonemap");
	}

	if (zonemap_result != FilterPropagateResult::NO_PRUNING_POSSIBLE || !IsColumnImprintEnabled() ||
	    !HasImprint(stats)) {
		return zonemap_result;
	}

	// if (comparison_type != ExpressionType::COMPARE_EQUAL &&
	//     comparison_type != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
	// 	return zonemap_result;
	// }

	IMPRINT_LOG(StringUtil::Format("[imprint-check-enter] type=%s bitmap=0x%016llx bins=%d", stats.GetType().ToString(),
	                               NumericStats::GetImprintBitmapUnsafe(stats),
	                               NumericStats::GetImprintBinsUnsafe(stats)));

	switch (internal_type) {
	case PhysicalType::INT8:
		return CheckImprintTemplated<int8_t>(stats, comparison_type, constants);
	case PhysicalType::INT16:
		return CheckImprintTemplated<int16_t>(stats, comparison_type, constants);
	case PhysicalType::INT32:
		return CheckImprintTemplated<int32_t>(stats, comparison_type, constants);
	case PhysicalType::INT64:
		return CheckImprintTemplated<int64_t>(stats, comparison_type, constants);
	case PhysicalType::UINT8:
		return CheckImprintTemplated<uint8_t>(stats, comparison_type, constants);
	case PhysicalType::UINT16:
		return CheckImprintTemplated<uint16_t>(stats, comparison_type, constants);
	case PhysicalType::UINT32:
		return CheckImprintTemplated<uint32_t>(stats, comparison_type, constants);
	case PhysicalType::UINT64:
		return CheckImprintTemplated<uint64_t>(stats, comparison_type, constants);
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	default:
		return zonemap_result;
	}
}

bool NumericStats::IsConstant(const BaseStatistics &stats) {
	return NumericStats::Max(stats) <= NumericStats::Min(stats);
}

void SetNumericValueInternal(const Value &input, const LogicalType &type, NumericValueUnion &val, bool &has_val) {
	if (input.IsNull()) {
		has_val = false;
		return;
	}
	if (input.type().InternalType() != type.InternalType()) {
		throw InternalException("SetMin or SetMax called with Value that does not match statistics' column value");
	}
	has_val = true;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		val.value_.boolean = BooleanValue::Get(input);
		break;
	case PhysicalType::INT8:
		val.value_.tinyint = TinyIntValue::Get(input);
		break;
	case PhysicalType::INT16:
		val.value_.smallint = SmallIntValue::Get(input);
		break;
	case PhysicalType::INT32:
		val.value_.integer = IntegerValue::Get(input);
		break;
	case PhysicalType::INT64:
		val.value_.bigint = BigIntValue::Get(input);
		break;
	case PhysicalType::UINT8:
		val.value_.utinyint = UTinyIntValue::Get(input);
		break;
	case PhysicalType::UINT16:
		val.value_.usmallint = USmallIntValue::Get(input);
		break;
	case PhysicalType::UINT32:
		val.value_.uinteger = UIntegerValue::Get(input);
		break;
	case PhysicalType::UINT64:
		val.value_.ubigint = UBigIntValue::Get(input);
		break;
	case PhysicalType::INT128:
		val.value_.hugeint = HugeIntValue::Get(input);
		break;
	case PhysicalType::UINT128:
		val.value_.uhugeint = UhugeIntValue::Get(input);
		break;
	case PhysicalType::FLOAT:
		val.value_.float_ = FloatValue::Get(input);
		break;
	case PhysicalType::DOUBLE:
		val.value_.double_ = DoubleValue::Get(input);
		break;
	default:
		throw InternalException("Unsupported type for NumericStatistics::SetValueInternal");
	}
}

void NumericStats::SetMin(BaseStatistics &stats, const Value &new_min) {
	auto &data = NumericStats::GetDataUnsafe(stats);
	SetNumericValueInternal(new_min, stats.GetType(), data.min, data.has_min);
}

void NumericStats::SetMax(BaseStatistics &stats, const Value &new_max) {
	auto &data = NumericStats::GetDataUnsafe(stats);
	SetNumericValueInternal(new_max, stats.GetType(), data.max, data.has_max);
}

Value NumericValueUnionToValueInternal(const LogicalType &type, const NumericValueUnion &val) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return Value::BOOLEAN(val.value_.boolean);
	case PhysicalType::INT8:
		return Value::TINYINT(val.value_.tinyint);
	case PhysicalType::INT16:
		return Value::SMALLINT(val.value_.smallint);
	case PhysicalType::INT32:
		return Value::INTEGER(val.value_.integer);
	case PhysicalType::INT64:
		return Value::BIGINT(val.value_.bigint);
	case PhysicalType::UINT8:
		return Value::UTINYINT(val.value_.utinyint);
	case PhysicalType::UINT16:
		return Value::USMALLINT(val.value_.usmallint);
	case PhysicalType::UINT32:
		return Value::UINTEGER(val.value_.uinteger);
	case PhysicalType::UINT64:
		return Value::UBIGINT(val.value_.ubigint);
	case PhysicalType::INT128:
		return Value::HUGEINT(val.value_.hugeint);
	case PhysicalType::UINT128:
		return Value::UHUGEINT(val.value_.uhugeint);
	case PhysicalType::FLOAT:
		return Value::FLOAT(val.value_.float_);
	case PhysicalType::DOUBLE:
		return Value::DOUBLE(val.value_.double_);
	default:
		throw InternalException("Unsupported type for NumericValueUnionToValue");
	}
}

Value NumericValueUnionToValue(const LogicalType &type, const NumericValueUnion &val) {
	Value result = NumericValueUnionToValueInternal(type, val);
	result.GetTypeMutable() = type;
	return result;
}

bool NumericStats::HasMinMax(const BaseStatistics &stats) {
	return NumericStats::HasMin(stats) && NumericStats::HasMax(stats) &&
	       NumericStats::Min(stats) <= NumericStats::Max(stats);
}

bool NumericStats::HasMin(const BaseStatistics &stats) {
	if (stats.GetType().id() == LogicalTypeId::SQLNULL) {
		return false;
	}
	return NumericStats::GetDataUnsafe(stats).has_min;
}

bool NumericStats::HasMax(const BaseStatistics &stats) {
	if (stats.GetType().id() == LogicalTypeId::SQLNULL) {
		return false;
	}
	return NumericStats::GetDataUnsafe(stats).has_max;
}

Value NumericStats::Min(const BaseStatistics &stats) {
	if (!NumericStats::HasMin(stats)) {
		throw InternalException("Min() called on statistics that does not have min");
	}
	return NumericValueUnionToValue(stats.GetType(), NumericStats::GetDataUnsafe(stats).min);
}

Value NumericStats::Max(const BaseStatistics &stats) {
	if (!NumericStats::HasMax(stats)) {
		throw InternalException("Max() called on statistics that does not have max");
	}
	return NumericValueUnionToValue(stats.GetType(), NumericStats::GetDataUnsafe(stats).max);
}

Value NumericStats::MinOrNull(const BaseStatistics &stats) {
	if (!NumericStats::HasMin(stats)) {
		return Value(stats.GetType());
	}
	return NumericStats::Min(stats);
}

Value NumericStats::MaxOrNull(const BaseStatistics &stats) {
	if (!NumericStats::HasMax(stats)) {
		return Value(stats.GetType());
	}
	return NumericStats::Max(stats);
}

static void SerializeNumericStatsValue(const LogicalType &type, NumericValueUnion val, bool has_value,
                                       Serializer &serializer) {
	serializer.WriteProperty(100, "has_value", has_value);
	if (!has_value) {
		return;
	}
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		serializer.WriteProperty(101, "value", val.value_.boolean);
		break;
	case PhysicalType::INT8:
		serializer.WriteProperty(101, "value", val.value_.tinyint);
		break;
	case PhysicalType::INT16:
		serializer.WriteProperty(101, "value", val.value_.smallint);
		break;
	case PhysicalType::INT32:
		serializer.WriteProperty(101, "value", val.value_.integer);
		break;
	case PhysicalType::INT64:
		serializer.WriteProperty(101, "value", val.value_.bigint);
		break;
	case PhysicalType::UINT8:
		serializer.WriteProperty(101, "value", val.value_.utinyint);
		break;
	case PhysicalType::UINT16:
		serializer.WriteProperty(101, "value", val.value_.usmallint);
		break;
	case PhysicalType::UINT32:
		serializer.WriteProperty(101, "value", val.value_.uinteger);
		break;
	case PhysicalType::UINT64:
		serializer.WriteProperty(101, "value", val.value_.ubigint);
		break;
	case PhysicalType::INT128:
		serializer.WriteProperty(101, "value", val.value_.hugeint);
		break;
	case PhysicalType::UINT128:
		serializer.WriteProperty(101, "value", val.value_.uhugeint);
		break;
	case PhysicalType::FLOAT:
		serializer.WriteProperty(101, "value", val.value_.float_);
		break;
	case PhysicalType::DOUBLE:
		serializer.WriteProperty(101, "value", val.value_.double_);
		break;
	default:
		throw InternalException("Unsupported type for serializing numeric statistics");
	}
}

static void DeserializeNumericStatsValue(const LogicalType &type, NumericValueUnion &result, bool &has_stats,
                                         Deserializer &deserializer) {
	auto has_value = deserializer.ReadProperty<bool>(100, "has_value");
	if (!has_value) {
		has_stats = false;
		return;
	}
	has_stats = true;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		result.value_.boolean = deserializer.ReadProperty<bool>(101, "value");
		break;
	case PhysicalType::INT8:
		result.value_.tinyint = deserializer.ReadProperty<int8_t>(101, "value");
		break;
	case PhysicalType::INT16:
		result.value_.smallint = deserializer.ReadProperty<int16_t>(101, "value");
		break;
	case PhysicalType::INT32:
		result.value_.integer = deserializer.ReadProperty<int32_t>(101, "value");
		break;
	case PhysicalType::INT64:
		result.value_.bigint = deserializer.ReadProperty<int64_t>(101, "value");
		break;
	case PhysicalType::UINT8:
		result.value_.utinyint = deserializer.ReadProperty<uint8_t>(101, "value");
		break;
	case PhysicalType::UINT16:
		result.value_.usmallint = deserializer.ReadProperty<uint16_t>(101, "value");
		break;
	case PhysicalType::UINT32:
		result.value_.uinteger = deserializer.ReadProperty<uint32_t>(101, "value");
		break;
	case PhysicalType::UINT64:
		result.value_.ubigint = deserializer.ReadProperty<uint64_t>(101, "value");
		break;
	case PhysicalType::INT128:
		result.value_.hugeint = deserializer.ReadProperty<hugeint_t>(101, "value");
		break;
	case PhysicalType::UINT128:
		result.value_.uhugeint = deserializer.ReadProperty<uhugeint_t>(101, "value");
		break;
	case PhysicalType::FLOAT:
		result.value_.float_ = deserializer.ReadProperty<float>(101, "value");
		break;
	case PhysicalType::DOUBLE:
		result.value_.double_ = deserializer.ReadProperty<double>(101, "value");
		break;
	default:
		throw InternalException("Unsupported type for serializing numeric statistics");
	}
}

void NumericStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &numeric_stats = NumericStats::GetDataUnsafe(stats);
	serializer.WriteObject(200, "max", [&](Serializer &object) {
		SerializeNumericStatsValue(stats.GetType(), numeric_stats.min, numeric_stats.has_min, object);
	});
	serializer.WriteObject(201, "min", [&](Serializer &object) {
		SerializeNumericStatsValue(stats.GetType(), numeric_stats.max, numeric_stats.has_max, object);
	});
	serializer.WriteProperty(202, "imprint_bins", numeric_stats.imprint_bins);
	serializer.WriteProperty(203, "imprint_bitmap", numeric_stats.imprint_bitmap);
}

void NumericStats::Deserialize(Deserializer &deserializer, BaseStatistics &result) {
	auto &numeric_stats = NumericStats::GetDataUnsafe(result);

	deserializer.ReadObject(200, "max", [&](Deserializer &object) {
		DeserializeNumericStatsValue(result.GetType(), numeric_stats.min, numeric_stats.has_min, object);
	});
	deserializer.ReadObject(201, "min", [&](Deserializer &object) {
		DeserializeNumericStatsValue(result.GetType(), numeric_stats.max, numeric_stats.has_max, object);
	});
	deserializer.ReadPropertyWithExplicitDefault<uint8_t>(202, "imprint_bins", numeric_stats.imprint_bins, 0);
	deserializer.ReadPropertyWithExplicitDefault<uint64_t>(203, "imprint_bitmap", numeric_stats.imprint_bitmap, 0);
}

string NumericStats::ToString(const BaseStatistics &stats) {
	return StringUtil::Format("[Min: %s, Max: %s]", NumericStats::MinOrNull(stats).ToString(),
	                          NumericStats::MaxOrNull(stats).ToString());
}

template <class T>
void NumericStats::TemplatedVerify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel,
                                   idx_t count) {
	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);

	auto data = UnifiedVectorFormat::GetData<T>(vdata);
	auto min_value = NumericStats::MinOrNull(stats);
	auto max_value = NumericStats::MaxOrNull(stats);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		if (!vdata.validity.RowIsValid(index)) {
			continue;
		}
		if (!min_value.IsNull() && LessThan::Operation(data[index], min_value.GetValueUnsafe<T>())) { // LCOV_EXCL_START
			throw InternalException("Statistics mismatch: value is smaller than min.\nStatistics: %s\nVector: %s",
			                        stats.ToString(), vector.ToString(count));
		} // LCOV_EXCL_STOP
		if (!max_value.IsNull() && GreaterThan::Operation(data[index], max_value.GetValueUnsafe<T>())) {
			throw InternalException("Statistics mismatch: value is bigger than max.\nStatistics: %s\nVector: %s",
			                        stats.ToString(), vector.ToString(count));
		}
	}
}

void NumericStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	auto &type = stats.GetType();
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		break;
	case PhysicalType::INT8:
		TemplatedVerify<int8_t>(stats, vector, sel, count);
		break;
	case PhysicalType::INT16:
		TemplatedVerify<int16_t>(stats, vector, sel, count);
		break;
	case PhysicalType::INT32:
		TemplatedVerify<int32_t>(stats, vector, sel, count);
		break;
	case PhysicalType::INT64:
		TemplatedVerify<int64_t>(stats, vector, sel, count);
		break;
	case PhysicalType::UINT8:
		TemplatedVerify<uint8_t>(stats, vector, sel, count);
		break;
	case PhysicalType::UINT16:
		TemplatedVerify<uint16_t>(stats, vector, sel, count);
		break;
	case PhysicalType::UINT32:
		TemplatedVerify<uint32_t>(stats, vector, sel, count);
		break;
	case PhysicalType::UINT64:
		TemplatedVerify<uint64_t>(stats, vector, sel, count);
		break;
	case PhysicalType::INT128:
		TemplatedVerify<hugeint_t>(stats, vector, sel, count);
		break;
	case PhysicalType::UINT128:
		TemplatedVerify<uhugeint_t>(stats, vector, sel, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedVerify<float>(stats, vector, sel, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedVerify<double>(stats, vector, sel, count);
		break;
	default:
		throw InternalException("Unsupported type %s for numeric statistics verify", type.ToString());
	}
}

void NumericStats::BuildImprintForSegment(ColumnData &col_data, SegmentNode<ColumnSegment> &segment_node) {
	auto &segment = *segment_node.node;
	auto &stats = segment.stats.statistics;

	// only support persistent segments
	if (segment.segment_type != ColumnSegmentType::PERSISTENT) {
		NumericStats::ResetImprint(stats);
		return;
	}

	switch (segment.type.InternalType()) {
	case PhysicalType::INT8:
		BuildImprintForSegmentInternal<int8_t>(stats, col_data, segment_node);
		break;
	case PhysicalType::INT16:
		BuildImprintForSegmentInternal<int16_t>(stats, col_data, segment_node);
		break;
	case PhysicalType::INT32:
		BuildImprintForSegmentInternal<int32_t>(stats, col_data, segment_node);
		break;
	case PhysicalType::INT64:
		BuildImprintForSegmentInternal<int64_t>(stats, col_data, segment_node);
		break;
	case PhysicalType::UINT8:
		BuildImprintForSegmentInternal<uint8_t>(stats, col_data, segment_node);
		break;
	case PhysicalType::UINT16:
		BuildImprintForSegmentInternal<uint16_t>(stats, col_data, segment_node);
		break;
	case PhysicalType::UINT32:
		BuildImprintForSegmentInternal<uint32_t>(stats, col_data, segment_node);
		break;
	case PhysicalType::UINT64:
		BuildImprintForSegmentInternal<uint64_t>(stats, col_data, segment_node);
		break;
	default:
		// for unsupported types, invalid imprint stats
		NumericStats::ResetImprint(stats);
		break;
	}
}

} // namespace duckdb
