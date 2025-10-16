#pragma once

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/array.hpp"

namespace duckdb {

using variant_type_map = array<idx_t, static_cast<uint8_t>(VariantLogicalType::ENUM_SIZE)>;

struct ObjectStatsData;
struct ArrayStatsData;

struct VariantStatsData {
public:
	VariantStatsData() = default;

public:
	void SetEmpty();
	void SetUnknown();
	void Merge(const VariantStatsData &other);
	void Update(VariantLogicalType type_id);

public:
	//! Count of each variant type encountered
	variant_type_map type_counts = {};
	//! For decimals, track physical type distribution
	array<idx_t, 3> decimal_physical_types = {}; // INT16, INT32, INT64, INT128
	//! Nested type analysis
	unique_ptr<ObjectStatsData> object_stats = nullptr;
	unique_ptr<ArrayStatsData> array_stats = nullptr;
};

struct ObjectStatsData {
public:
	ObjectStatsData() = default;

public:
	//! Per-field analysis for object shredding decisions
	case_insensitive_map_t<VariantStatsData> field_stats;
	//! Track field frequency for shredding priority
	case_insensitive_map_t<idx_t> field_frequencies;
};

struct ArrayStatsData {
public:
	ArrayStatsData() = default;

public:
	//! Analysis of array element types
	VariantStatsData element_stats;
	//! Array size distribution for optimization decisions
	unordered_map<idx_t, idx_t> size_distribution;
};

class VariantStats : public BaseStatistics {
public:
	//! Unknown statistics
	DUCKDB_API static BaseStatistics CreateUnknown(LogicalType type);
	//! Empty statistics
	DUCKDB_API static BaseStatistics CreateEmpty(LogicalType type);

	DUCKDB_API static void Serialize(const BaseStatistics &stats, Serializer &serializer);
	DUCKDB_API static void Deserialize(Deserializer &deserializer, BaseStatistics &base);

	DUCKDB_API static string ToString(const BaseStatistics &stats);

	DUCKDB_API static void Update(BaseStatistics &stats, const Value &value);
	DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other);
	DUCKDB_API static void Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);

private:
	static VariantStatsData &GetDataUnsafe(BaseStatistics &stats);
	static const VariantStatsData &GetDataUnsafe(const BaseStatistics &stats);
	//! Determine optimal shredding schema based on collected stats
	LogicalType GetOptimalShreddedType(double shredding_threshold = 0.8) const;
};

} // namespace duckdb
