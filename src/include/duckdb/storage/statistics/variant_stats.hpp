#pragma once

#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
class BaseStatistics;
struct VariantStatsData;

struct VariantColumnStatsData {
public:
	VariantColumnStatsData() = default;

public:
	void SetType(VariantLogicalType type);
	VariantColumnStatsData &GetOrCreateElement(VariantStatsData &stats);
	VariantColumnStatsData &GetOrCreateField(VariantStatsData &stats, const string &name);

public:
	//! Count of each variant type encountered
	idx_t type_counts[static_cast<uint8_t>(VariantLogicalType::ENUM_SIZE)] = {0};
	//! For decimals, track physical type distribution
	idx_t decimal_physical_types[3] = {0}; // INT16, INT32, INT64, INT128
	//! indices into the top-level 'columns' vector where the stats for the field/element live
	case_insensitive_map_t<idx_t> field_stats;
	idx_t element_stats = DConstants::INVALID_INDEX;
};

struct VariantStatsData {
public:
	void SetEmpty();
	void SetUnknown();
	void Merge(const VariantStatsData &other);
	void Update(const Value &value);

	VariantColumnStatsData &GetColumnStats(idx_t index);

public:
	//! Nested type analysis
	vector<VariantColumnStatsData> columns;
};

struct VariantStats {
public:
	DUCKDB_API static LogicalType GetUnshreddedType();
	DUCKDB_API static LogicalType GetShreddedType(const BaseStatistics &stats);

	DUCKDB_API static void CreateUnshreddedStats(BaseStatistics &stats);
	DUCKDB_API static void Construct(BaseStatistics &stats);
	DUCKDB_API static BaseStatistics CreateUnknown(LogicalType type);
	DUCKDB_API static BaseStatistics CreateEmpty(LogicalType type);

	DUCKDB_API static const BaseStatistics &GetUnshreddedStats(const BaseStatistics &stats);
	DUCKDB_API static BaseStatistics &GetUnshreddedStats(BaseStatistics &stats);

	DUCKDB_API static void SetUnshreddedStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats);
	DUCKDB_API static void SetUnshreddedStats(BaseStatistics &stats, const BaseStatistics &new_stats);

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
	//! Determine optimal shredding schema based on collected stats
	// LogicalType GetOptimalShreddedType(double shredding_threshold = 0.8) const;
};

} // namespace duckdb
