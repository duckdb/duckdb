//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/numeric_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/statistics/numeric_stats_union.hpp"
#include "duckdb/common/array_ptr.hpp"

// for column imprints
#include <type_traits>

namespace duckdb {
class BaseStatistics;
struct SelectionVector;
class Vector;

struct NumericStatsData {
	//! Whether or not the value has a max value
	bool has_min;
	//! Whether or not the segment has a min value
	bool has_max;
	//! The minimum value of the segment
	NumericValueUnion min;
	//! The maximum value of the segment
	NumericValueUnion max;

	// for column imprints
	uint64_t imprint_bitmap; // 64-bit imprint
	uint8_t imprint_bins;    // how many bins are used

	// Accessors:
	void SetImprint(uint64_t bitmap, uint8_t bins) {
		imprint_bitmap = bitmap;
		imprint_bins = bins;
	}
	uint64_t GetImprintBitmap() const {
		return imprint_bitmap;
	}
	uint8_t GetImprintBins() const {
		return imprint_bins;
	}
};

// the API for numeric statistics
struct NumericStats {
	//! Unknown statistics - i.e. "has_min" is false, "has_max" is false
	DUCKDB_API static BaseStatistics CreateUnknown(LogicalType type);
	//! Empty statistics - i.e. "min = MaxValue<type>, max = MinValue<type>"
	DUCKDB_API static BaseStatistics CreateEmpty(LogicalType type);

	DUCKDB_API static bool IsConstant(const BaseStatistics &stats);
	DUCKDB_API static bool HasMinMax(const BaseStatistics &stats);
	DUCKDB_API static bool HasMin(const BaseStatistics &stats);
	DUCKDB_API static bool HasMax(const BaseStatistics &stats);
	DUCKDB_API static Value Min(const BaseStatistics &stats);
	DUCKDB_API static Value Max(const BaseStatistics &stats);
	DUCKDB_API static void SetMin(BaseStatistics &stats, const Value &val);
	DUCKDB_API static void SetMax(BaseStatistics &stats, const Value &val);

	template <class T>
	static void SetMax(BaseStatistics &stats, T val) {
		auto &nstats = GetDataUnsafe(stats);
		nstats.has_max = true;
		nstats.max.GetReferenceUnsafe<T>() = val;
	}

	template <class T>
	static void SetMin(BaseStatistics &stats, T val) {
		auto &nstats = GetDataUnsafe(stats);
		nstats.has_min = true;
		nstats.min.GetReferenceUnsafe<T>() = val;
	}

	DUCKDB_API static FilterPropagateResult CheckZonemap(const BaseStatistics &stats, ExpressionType comparison_type,
	                                                     array_ptr<const Value> constants);

	// column imprint configuration helpers
	DUCKDB_API static bool IsColumnImprintEnabled();
	DUCKDB_API static void SetColumnImprintEnabled(bool enabled);

	// column imprint stats
	DUCKDB_API static idx_t GetImprintChecksTotal();
	DUCKDB_API static idx_t GetImprintPrunedSegments();
	DUCKDB_API static idx_t GetImprintEqualityChecks();
	DUCKDB_API static idx_t GetImprintGreaterThanChecks();
	DUCKDB_API static void ResetImprintStatistics();

	// segment pruning stats
	DUCKDB_API static idx_t GetTotalSegmentsChecked();
	DUCKDB_API static idx_t GetTotalSegmentsSkipped();
	DUCKDB_API static void IncrementSegmentsChecked();
	DUCKDB_API static void IncrementSegmentsSkipped();

	DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other_p);

	DUCKDB_API static void Serialize(const BaseStatistics &stats, Serializer &serializer);
	DUCKDB_API static void Deserialize(Deserializer &deserializer, BaseStatistics &stats);

	DUCKDB_API static string ToString(const BaseStatistics &stats);

	template <class T>
	static inline void UpdateValue(T new_value, T &min, T &max) {
		min = LessThan::Operation(new_value, min) ? new_value : min;
		max = GreaterThan::Operation(new_value, max) ? new_value : max;
	}

	template <class T>
	static inline void Update(NumericStatsData &nstats, T new_value) {
		// update min/max
		UpdateValue<T>(new_value, nstats.min.GetReferenceUnsafe<T>(), nstats.max.GetReferenceUnsafe<T>());

		// update imprint
		UpdateImprint<T>(nstats, new_value);
	}

	// for column imprint
	// not supported for numeric types over 8 bytes
	template <class T>
	static inline uint8_t ComputeBinIndexImpl(T value, T min, T max, uint8_t bins) {
		double value_d = static_cast<double>(value);
		double min_d = static_cast<double>(min);
		double max_d = static_cast<double>(max);

		if (max_d == min_d) {
			return 0;
		}

		double ratio = (value_d - min_d) / (max_d - min_d);
		if (ratio < 0.0) {
			ratio = 0.0;
		} else if (ratio > 1.0) {
			ratio = 1.0;
		}

		uint8_t idx = static_cast<uint8_t>(ratio * (bins - 1));
		return idx;
	}

	template <class T>
	static inline uint8_t ComputeBinIndex(T value, T min, T max, uint8_t bins) {
		if (min == max) {
			return 0;
		}
		return ComputeBinIndexImpl<T>(value, min, max, bins);
	}

	static void Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);

	template <class T>
	static T GetMin(const BaseStatistics &stats) {
		return NumericStats::Min(stats).GetValueUnsafe<T>();
	}
	template <class T>
	static T GetMax(const BaseStatistics &stats) {
		return NumericStats::Max(stats).GetValueUnsafe<T>();
	}
	template <class T>
	static T GetMinUnsafe(const BaseStatistics &stats);
	template <class T>
	static T GetMaxUnsafe(const BaseStatistics &stats);

	static uint64_t GetImprintBitmapUnsafe(const BaseStatistics &stats);
	static uint8_t GetImprintBinsUnsafe(const BaseStatistics &stats);

private:
	static NumericStatsData &GetDataUnsafe(BaseStatistics &stats);
	static const NumericStatsData &GetDataUnsafe(const BaseStatistics &stats);

	static Value MinOrNull(const BaseStatistics &stats);
	static Value MaxOrNull(const BaseStatistics &stats);
	template <class T>
	static void TemplatedVerify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);

	// for column imprint: determine which types are supported
	// currently only support numeric types with size <= 8 bytes
	template <class T>
	struct ImprintSupported {
		static const bool value = std::is_integral<T>::value && sizeof(T) <= 8;
	};

	template <class T>
	static typename std::enable_if<ImprintSupported<T>::value, void>::type UpdateImprint(NumericStatsData &nstats,
	                                                                                     T new_value) {
		if (!nstats.has_min || !nstats.has_max || nstats.imprint_bins == 0) {
			return;
		}
		T &min_val = nstats.min.GetReferenceUnsafe<T>();
		T &max_val = nstats.max.GetReferenceUnsafe<T>();
		if (min_val == max_val) {
			return;
		}
		uint8_t bin_idx = ComputeBinIndex<T>(new_value, min_val, max_val, nstats.imprint_bins);
		nstats.imprint_bitmap |= (uint64_t(1) << bin_idx);
	}

	// if the type is not supported, do nothing
	template <class T>
	static typename std::enable_if<!ImprintSupported<T>::value, void>::type UpdateImprint(NumericStatsData &, T) {
	}
};

} // namespace duckdb
