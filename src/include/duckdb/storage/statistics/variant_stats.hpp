#pragma once

#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types/selection_vector.hpp"

namespace duckdb {
class BaseStatistics;

struct VariantStatsData {
	//! Whether the VARIANT is stored in shredded form
	bool is_shredded;
};

struct VariantStats {
public:
	DUCKDB_API static void CreateUnshreddedStats(BaseStatistics &stats);
	DUCKDB_API static void Construct(BaseStatistics &stats);
	DUCKDB_API static BaseStatistics CreateUnknown(LogicalType type);
	DUCKDB_API static BaseStatistics CreateEmpty(LogicalType type);
	DUCKDB_API static BaseStatistics CreateShredded(const LogicalType &shredded_type);

	DUCKDB_API static const BaseStatistics &GetUnshreddedStats(const BaseStatistics &stats);
	DUCKDB_API static BaseStatistics &GetUnshreddedStats(BaseStatistics &stats);

	DUCKDB_API static const BaseStatistics &GetShreddedStats(const BaseStatistics &stats);
	DUCKDB_API static BaseStatistics &GetShreddedStats(BaseStatistics &stats);

	DUCKDB_API static void SetUnshreddedStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats);
	DUCKDB_API static void SetUnshreddedStats(BaseStatistics &stats, const BaseStatistics &new_stats);

	DUCKDB_API static void SetShreddedStats(BaseStatistics &stats, const BaseStatistics &new_stats);

	DUCKDB_API static void Serialize(const BaseStatistics &stats, Serializer &serializer);
	DUCKDB_API static void Deserialize(Deserializer &deserializer, BaseStatistics &base);

	DUCKDB_API static string ToString(const BaseStatistics &stats);

	DUCKDB_API static void Update(BaseStatistics &stats, Vector &vec, idx_t count);
	DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other);
	DUCKDB_API static void Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);
	DUCKDB_API static void Copy(BaseStatistics &stats, const BaseStatistics &other);

private:
	static VariantStatsData &GetDataUnsafe(BaseStatistics &stats);
	static const VariantStatsData &GetDataUnsafe(const BaseStatistics &stats);
};

} // namespace duckdb
