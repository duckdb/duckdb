#pragma once

#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types/selection_vector.hpp"

namespace duckdb {
class BaseStatistics;

enum class VariantStatsShreddingState : uint8_t {
	//! Uninitialized, not unshredded/shredded
	UNINITIALIZED,
	//! No shredding applied yet
	NOT_SHREDDED,
	//! Shredded consistently
	SHREDDED,
	//! Result from combining incompatible shreddings
	INCONSISTENT
};

struct VariantStatsData {
	//! Whether the VARIANT is stored in shredded form
	VariantStatsShreddingState shredding_state;
};

struct VariantShreddedStats {
public:
	DUCKDB_API static bool IsFullyShredded(const BaseStatistics &stats);
};

//! VARIANT as a type can hold arbitrarily typed values within the same column.
//! In storage, we apply shredding to the VARIANT column, this means that we find the most common type among all these
//! values. And for those values we store them separately from the rest of the values, in a structured way (like you
//! would store any other column).
struct VariantStats {
public:
	DUCKDB_API static void Construct(BaseStatistics &stats);

public:
	DUCKDB_API static BaseStatistics CreateUnknown(LogicalType type);
	DUCKDB_API static BaseStatistics CreateEmpty(LogicalType type);
	DUCKDB_API static BaseStatistics CreateShredded(const LogicalType &shredded_type);

public:
	//! Stats related to the 'unshredded' column, which holds all data that doesn't fit in the structure of the shredded
	//! column (if IsShredded())
	DUCKDB_API static void CreateUnshreddedStats(BaseStatistics &stats);
	DUCKDB_API static const BaseStatistics &GetUnshreddedStats(const BaseStatistics &stats);
	DUCKDB_API static BaseStatistics &GetUnshreddedStats(BaseStatistics &stats);

	DUCKDB_API static void SetUnshreddedStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats);
	DUCKDB_API static void SetUnshreddedStats(BaseStatistics &stats, const BaseStatistics &new_stats);
	DUCKDB_API static void MarkAsNotShredded(BaseStatistics &stats);

public:
	//! Stats related to the 'shredded' column, which holds all structured data created during shredding
	//! Returns the LogicalType that represents the shredding as a single DuckDB LogicalType (i.e STRUCT(col1 VARCHAR))
	DUCKDB_API LogicalType GetShreddedStructuredType(const BaseStatistics &stats);
	DUCKDB_API static void CreateShreddedStats(BaseStatistics &stats, const LogicalType &shredded_type);
	DUCKDB_API static bool IsShredded(const BaseStatistics &stats);
	DUCKDB_API static const BaseStatistics &GetShreddedStats(const BaseStatistics &stats);
	DUCKDB_API static BaseStatistics &GetShreddedStats(BaseStatistics &stats);

	DUCKDB_API static void SetShreddedStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats);
	DUCKDB_API static void SetShreddedStats(BaseStatistics &stats, const BaseStatistics &new_stats);

	DUCKDB_API static bool MergeShredding(BaseStatistics &stats, const BaseStatistics &other,
	                                      BaseStatistics &new_stats);

public:
	DUCKDB_API static void Serialize(const BaseStatistics &stats, Serializer &serializer);
	DUCKDB_API static void Deserialize(Deserializer &deserializer, BaseStatistics &base);

	DUCKDB_API static string ToString(const BaseStatistics &stats);

	DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other);
	DUCKDB_API static void Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);
	DUCKDB_API static void Copy(BaseStatistics &stats, const BaseStatistics &other);

private:
	static VariantStatsData &GetDataUnsafe(BaseStatistics &stats);
	static const VariantStatsData &GetDataUnsafe(const BaseStatistics &stats);
};

} // namespace duckdb
