#include "duckdb/storage/statistics/geometry_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

vector<string> GeometryTypeSet::ToString(bool snake_case) const {
	vector<string> result;
	for (auto d = 0; d < VERT_TYPES; d++) {
		for (auto i = 0; i < PART_TYPES; i++) {
			if (sets[d] & (1 << i)) {
				string str;
				switch (i) {
				case 1:
					str = snake_case ? "point" : "Point";
					break;
				case 2:
					str = snake_case ? "linestring" : "LineString";
					break;
				case 3:
					str = snake_case ? "polygon" : "Polygon";
					break;
				case 4:
					str = snake_case ? "multipoint" : "MultiPoint";
					break;
				case 5:
					str = snake_case ? "multilinestring" : "MultiLineString";
					break;
				case 6:
					str = snake_case ? "multipolygon" : "MultiPolygon";
					break;
				case 7:
					str = snake_case ? "geometrycollection" : "GeometryCollection";
					break;
				default:
					str = snake_case ? "unknown" : "Unknown";
					break;
				}
				switch (d) {
				case 1:
					str += snake_case ? "_z" : " Z";
					break;
				case 2:
					str += snake_case ? "_m" : " M";
					break;
				case 3:
					str += snake_case ? "_zm" : " ZM";
					break;
				default:
					break;
				}

				result.push_back(str);
			}
		}
	}
	return result;
}

BaseStatistics GeometryStats::CreateUnknown(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();
	GetDataUnsafe(result).SetUnknown();
	return result;
}

BaseStatistics GeometryStats::CreateEmpty(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();
	GetDataUnsafe(result).SetEmpty();
	return result;
}

void GeometryStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	const auto &data = GetDataUnsafe(stats);

	// Write extent
	serializer.WriteObject(200, "extent", [&](Serializer &extent) {
		extent.WriteProperty<double>(101, "x_min", data.extent.x_min);
		extent.WriteProperty<double>(102, "x_max", data.extent.x_max);
		extent.WriteProperty<double>(103, "y_min", data.extent.y_min);
		extent.WriteProperty<double>(104, "y_max", data.extent.y_max);
		extent.WriteProperty<double>(105, "z_min", data.extent.z_min);
		extent.WriteProperty<double>(106, "z_max", data.extent.z_max);
		extent.WriteProperty<double>(107, "m_min", data.extent.m_min);
		extent.WriteProperty<double>(108, "m_max", data.extent.m_max);
	});

	// Write types
	serializer.WriteObject(201, "types", [&](Serializer &types) {
		types.WriteProperty<uint8_t>(101, "types_xy", data.types.sets[0]);
		types.WriteProperty<uint8_t>(102, "types_xyz", data.types.sets[1]);
		types.WriteProperty<uint8_t>(103, "types_xym", data.types.sets[2]);
		types.WriteProperty<uint8_t>(104, "types_xyzm", data.types.sets[3]);
	});
}

void GeometryStats::Deserialize(Deserializer &deserializer, BaseStatistics &base) {
	auto &data = GetDataUnsafe(base);

	// Read extent
	deserializer.ReadObject(200, "extent", [&](Deserializer &extent) {
		extent.ReadProperty<double>(101, "x_min", data.extent.x_min);
		extent.ReadProperty<double>(102, "x_max", data.extent.x_max);
		extent.ReadProperty<double>(103, "y_min", data.extent.y_min);
		extent.ReadProperty<double>(104, "y_max", data.extent.y_max);
		extent.ReadProperty<double>(105, "z_min", data.extent.z_min);
		extent.ReadProperty<double>(106, "z_max", data.extent.z_max);
		extent.ReadProperty<double>(107, "m_min", data.extent.m_min);
		extent.ReadProperty<double>(108, "m_max", data.extent.m_max);
	});

	// Read types
	deserializer.ReadObject(201, "types", [&](Deserializer &types) {
		types.ReadProperty<uint8_t>(101, "types_xy", data.types.sets[0]);
		types.ReadProperty<uint8_t>(102, "types_xyz", data.types.sets[1]);
		types.ReadProperty<uint8_t>(103, "types_xym", data.types.sets[2]);
		types.ReadProperty<uint8_t>(104, "types_xyzm", data.types.sets[3]);
	});
}

string GeometryStats::ToString(const BaseStatistics &stats) {
	const auto &data = GetDataUnsafe(stats);
	string result;

	result += "[";
	result += StringUtil::Format("Extent: [X: [%f, %f], Y: [%f, %f], Z: [%f, %f], M: [%f, %f]", data.extent.x_min,
	                             data.extent.x_max, data.extent.y_min, data.extent.y_max, data.extent.z_min,
	                             data.extent.z_max, data.extent.m_min, data.extent.m_max);
	result += StringUtil::Format("], Types: [%s]", StringUtil::Join(data.types.ToString(true), ", "));

	result += "]";
	return result;
}

void GeometryStats::Update(BaseStatistics &stats, const string_t &value) {
	auto &data = GetDataUnsafe(stats);
	data.Update(value);
}

void GeometryStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}
	if (other.GetType().id() == LogicalTypeId::SQLNULL) {
		return;
	}

	auto &target = GetDataUnsafe(stats);
	auto &source = GetDataUnsafe(other);
	target.Merge(source);
}

void GeometryStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	// TODO: Verify stats
}

const GeometryStatsData &GeometryStats::GetDataUnsafe(const BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::GEOMETRY_STATS);
	return stats.stats_union.geometry_data;
}

GeometryStatsData &GeometryStats::GetDataUnsafe(BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::GEOMETRY_STATS);
	return stats.stats_union.geometry_data;
}

} // namespace duckdb
