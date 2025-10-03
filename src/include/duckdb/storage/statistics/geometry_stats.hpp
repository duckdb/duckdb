//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/geometry_stats.hpp
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
#include "duckdb/common/array_ptr.hpp"
#include "duckdb/common/types/geometry.hpp"

namespace duckdb {
class BaseStatistics;
struct SelectionVector;

class GeometryTypeSet {
public:
	static constexpr auto VERT_TYPES = 4;
	static constexpr auto PART_TYPES = 8;

	static GeometryTypeSet Unknown() {
		GeometryTypeSet result;
		for (idx_t i = 0; i < VERT_TYPES; i++) {
			result.sets[i] = 0xFF;
		}
		return result;
	}
	static GeometryTypeSet Empty() {
		GeometryTypeSet result;
		for (idx_t i = 0; i < VERT_TYPES; i++) {
			result.sets[i] = 0;
		}
		return result;
	}

	bool IsEmpty() const {
		for (idx_t i = 0; i < VERT_TYPES; i++) {
			if (sets[i] != 0) {
				return false;
			}
		}
		return true;
	}

	bool IsUnknown() const {
		for (idx_t i = 0; i < VERT_TYPES; i++) {
			if (sets[i] != 0xFF) {
				return false;
			}
		}
		return true;
	}

	void Add(GeometryType geom_type, VertexType vert_type) {
		const auto vert_idx = static_cast<uint8_t>(vert_type);
		const auto geom_idx = static_cast<uint8_t>(geom_type);
		D_ASSERT(vert_idx < VERT_TYPES);
		D_ASSERT(geom_idx < PART_TYPES);
		sets[vert_idx] |= (1 << geom_idx);
	}

	void Merge(const GeometryTypeSet &other) {
		for (idx_t i = 0; i < VERT_TYPES; i++) {
			sets[i] |= other.sets[i];
		}
	}

	vector<int> ToWKBList() const {
		vector<int> result;
		for (uint8_t vert_idx = 0; vert_idx < VERT_TYPES; vert_idx++) {
			for (uint8_t geom_idx = 1; geom_idx < PART_TYPES; geom_idx++) {
				if (sets[vert_idx] & (1 << geom_idx)) {
					result.push_back(geom_idx + vert_idx * 1000);
				}
			}
		}
		return result;
	}

	vector<string> ToString(bool snake_case) const;

	uint8_t sets[VERT_TYPES];
};

struct GeometryStatsData {

	GeometryTypeSet types;
	GeometryExtent extent;

	void SetEmpty() {
		types = GeometryTypeSet::Empty();
		extent = GeometryExtent::Empty();
	}

	void SetUnknown() {
		types = GeometryTypeSet::Unknown();
		extent = GeometryExtent::Unknown();
	}

	void Merge(const GeometryStatsData &other) {
		types.Merge(other.types);
		extent.Merge(other.extent);
	}

	void Update(const string_t &geom_blob) {

		// Parse type
		const auto type_info = Geometry::GetType(geom_blob);
		types.Add(type_info.first, type_info.second);

		// Update extent
		Geometry::GetExtent(geom_blob, extent);
	}
};

struct GeometryStats {
	//! Unknown statistics
	DUCKDB_API static BaseStatistics CreateUnknown(LogicalType type);
	//! Empty statistics
	DUCKDB_API static BaseStatistics CreateEmpty(LogicalType type);

	DUCKDB_API static void Serialize(const BaseStatistics &stats, Serializer &serializer);
	DUCKDB_API static void Deserialize(Deserializer &deserializer, BaseStatistics &base);

	DUCKDB_API static string ToString(const BaseStatistics &stats);

	DUCKDB_API static void Update(BaseStatistics &stats, const string_t &value);
	DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other);
	DUCKDB_API static void Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);

private:
	static GeometryStatsData &GetDataUnsafe(BaseStatistics &stats);
	static const GeometryStatsData &GetDataUnsafe(const BaseStatistics &stats);
};

} // namespace duckdb
