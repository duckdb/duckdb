//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/string_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {
class BaseStatistics;
struct SelectionVector;
class Vector;

struct StringStatsData {
	constexpr static uint32_t MAX_STRING_MINMAX_SIZE = 8;

	//! The minimum value of the segment, potentially truncated
	data_t min[MAX_STRING_MINMAX_SIZE];
	//! The maximum value of the segment, potentially truncated
	data_t max[MAX_STRING_MINMAX_SIZE];
	//! Whether or not the column can contain unicode characters
	bool has_unicode;
	//! Whether or not the maximum string length is known
	bool has_max_string_length;
	//! The maximum string length in bytes
	uint32_t max_string_length;
};

struct StringStats {
	//! Unknown statistics - i.e. "has_unicode" is true, "max_string_length" is unknown, "min" is \0, max is \xFF
	DUCKDB_API static BaseStatistics CreateUnknown(LogicalType type);
	//! Empty statistics - i.e. "has_unicode" is false, "max_string_length" is 0, "min" is \xFF, max is \x00
	DUCKDB_API static BaseStatistics CreateEmpty(LogicalType type);
	//! Whether or not the statistics have a maximum string length defined
	DUCKDB_API static bool HasMaxStringLength(const BaseStatistics &stats);
	//! Returns the maximum string length, or throws an exception if !HasMaxStringLength()
	DUCKDB_API static uint32_t MaxStringLength(const BaseStatistics &stats);
	//! Whether or not the strings can contain unicode
	DUCKDB_API static bool CanContainUnicode(const BaseStatistics &stats);
	//! Returns the min value (up to a length of StringStatsData::MAX_STRING_MINMAX_SIZE)
	DUCKDB_API static string Min(const BaseStatistics &stats);
	//! Returns the max value (up to a length of StringStatsData::MAX_STRING_MINMAX_SIZE)
	DUCKDB_API static string Max(const BaseStatistics &stats);

	//! Resets the max string length so HasMaxStringLength() is false
	DUCKDB_API static void ResetMaxStringLength(BaseStatistics &stats);
	//! FIXME: make this part of Set on statistics
	DUCKDB_API static void SetContainsUnicode(BaseStatistics &stats);

	DUCKDB_API static void Serialize(const BaseStatistics &stats, Serializer &serializer);
	DUCKDB_API static void Deserialize(Deserializer &deserializer, BaseStatistics &base);

	DUCKDB_API static string ToString(const BaseStatistics &stats);

	DUCKDB_API static FilterPropagateResult CheckZonemap(const BaseStatistics &stats, ExpressionType comparison_type,
	                                                     const string &value);
	DUCKDB_API static FilterPropagateResult CheckZonemap(const_data_ptr_t min_data, idx_t min_len,
	                                                     const_data_ptr_t max_data, idx_t max_len,
	                                                     ExpressionType comparison_type, const string &value);

	DUCKDB_API static void Update(BaseStatistics &stats, const string_t &value);
	DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other);
	DUCKDB_API static void Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);

private:
	static StringStatsData &GetDataUnsafe(BaseStatistics &stats);
	static const StringStatsData &GetDataUnsafe(const BaseStatistics &stats);
};

} // namespace duckdb
