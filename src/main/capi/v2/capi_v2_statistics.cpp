#include "duckdb/main/capi_v2/capi_v2_function_internal.hpp"

#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/storage/statistics/array_stats.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"
#include "duckdb/storage/statistics/variant_stats.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb::capiv2 {

//! Casts a bound value to the type of the statistics: a NULL stays NULL. Throws when the value does not cast.
static Value StatisticsValue(const char *function, const BaseStatistics &stats, const Value &input) {
	if (input.IsNull()) {
		return Value(stats.GetType());
	}
	string error;
	auto result = input.DefaultTryCastAs(stats.GetType(), &error, true);
	if (!result) {
		throw InvalidInputException("%s: the value '%s' of type %s does not cast to the type %s of the statistics",
		                            function, input.ToString(), input.type().ToString(), stats.GetType().ToString());
	}
	return std::move(*result);
}

//! Throws unless the statistics are of the expected kind (e.g. a numeric minimum needs numeric statistics).
static void ExpectStatsType(const char *function, const BaseStatistics &stats, StatisticsType expected,
                            const char *what) {
	if (stats.GetStatsType() != expected) {
		throw InvalidInputException("%s: statistics of type %s have no %s", function, stats.GetType().ToString(), what);
	}
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

using duckdb::ArrayStats;
using duckdb::BaseStatistics;
using duckdb::GeometryStats;
using duckdb::GeometryTypeSet;
using duckdb::InvalidInputException;
using duckdb::ListStats;
using duckdb::LogicalType;
using duckdb::LogicalTypeId;
using duckdb::NumericStats;
using duckdb::StatisticsType;
using duckdb::StatsInfo;
using duckdb::StringStats;
using duckdb::StringStatsType;
using duckdb::StructStats;
using duckdb::StructType;
using duckdb::Value;
using duckdb::VariantStats;

DUCKDB_V2_ERROR duckdb_v2_statistics_create(duckdb_v2_factory_handle factory, duckdb_v2_logical_type_handle type,
                                            duckdb_v2_statistics_handle *statistics, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(factory);
	DUCKDB_CHECK_ARG(type);
	DUCKDB_CHECK_ARG(statistics);
	*statistics = nullptr;
	return WithErrorHandler(err, [&]() {
		auto type_ref = Convert(type);
		const auto &logical_type = *type_ref;
		if (logical_type.id() == duckdb::LogicalTypeId::INVALID) {
			throw InvalidInputException("duckdb_v2_statistics_create: cannot create statistics of an INVALID type");
		}
		*statistics = Convert(new BaseStatistics(BaseStatistics::CreateUnknown(logical_type)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_statistics_create_variant_shredded(duckdb_v2_factory_handle factory,
                                                             duckdb_v2_statistics_handle shredded_statistics,
                                                             duckdb_v2_statistics_handle *statistics,
                                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(factory);
	DUCKDB_CHECK_ARG(shredded_statistics);
	DUCKDB_CHECK_ARG(statistics);
	*statistics = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &shredded = *Convert(shredded_statistics);
		auto result = VariantStats::CreateShredded(shredded.GetType());
		VariantStats::SetShreddedStats(result, shredded);
		*statistics = Convert(new BaseStatistics(std::move(result)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_statistics_variant_shredding_type(duckdb_v2_factory_handle factory,
                                                            duckdb_v2_logical_type_handle structured_type,
                                                            duckdb_v2_logical_type_handle *type,
                                                            duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(factory);
	DUCKDB_CHECK_ARG(structured_type);
	DUCKDB_CHECK_ARG(type);
	*type = nullptr;
	return WithErrorHandler(err, [&]() {
		auto structured_ref = Convert(structured_type);
		*type = Convert(VariantStats::GetShreddingType(*structured_ref));
	});
}

DUCKDB_V2_ERROR duckdb_v2_statistics_set_has_null(duckdb_v2_statistics_handle statistics, bool has_null,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	return WithErrorHandler(err, [&]() {
		Convert(statistics)
		    ->Set(has_null ? duckdb::StatsInfo::CAN_HAVE_NULL_VALUES : duckdb::StatsInfo::CANNOT_HAVE_NULL_VALUES);
	});
}

DUCKDB_V2_ERROR duckdb_v2_statistics_set_has_no_null(duckdb_v2_statistics_handle statistics, bool has_no_null,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	return WithErrorHandler(err, [&]() {
		Convert(statistics)
		    ->Set(has_no_null ? duckdb::StatsInfo::CAN_HAVE_VALID_VALUES : duckdb::StatsInfo::CANNOT_HAVE_VALID_VALUES);
	});
}

DUCKDB_V2_ERROR duckdb_v2_statistics_set_min(duckdb_v2_statistics_handle statistics, duckdb_v2_value_handle value,
                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	DUCKDB_CHECK_ARG(value);
	return WithErrorHandler(err, [&]() {
		auto &stats = *Convert(statistics);
		ExpectStatsType("duckdb_v2_statistics_set_min", stats, StatisticsType::NUMERIC_STATS, "numeric minimum");
		auto min = StatisticsValue("duckdb_v2_statistics_set_min", stats, *Convert(value));
		NumericStats::SetMin(stats, min);
	});
}

DUCKDB_V2_ERROR duckdb_v2_statistics_set_max(duckdb_v2_statistics_handle statistics, duckdb_v2_value_handle value,
                                             duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	DUCKDB_CHECK_ARG(value);
	return WithErrorHandler(err, [&]() {
		auto &stats = *Convert(statistics);
		ExpectStatsType("duckdb_v2_statistics_set_max", stats, StatisticsType::NUMERIC_STATS, "numeric maximum");
		auto max = StatisticsValue("duckdb_v2_statistics_set_max", stats, *Convert(value));
		NumericStats::SetMax(stats, max);
	});
}

DUCKDB_V2_ERROR duckdb_v2_statistics_set_string_min(duckdb_v2_statistics_handle statistics, duckdb_v2_str *value,
                                                    bool is_exact, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	DUCKDB_CHECK_ARG(value);
	return WithErrorHandler(err, [&]() {
		auto &stats = *Convert(statistics);
		ExpectStatsType("duckdb_v2_statistics_set_string_min", stats, StatisticsType::STRING_STATS, "string minimum");
		auto bytes = Convert(value);
		duckdb::string_t text(bytes.data(), duckdb::UnsafeNumericCast<uint32_t>(bytes.size()));
		StringStats::SetMin(stats, text, is_exact ? StringStatsType::EXACT_STATS : StringStatsType::TRUNCATED_STATS);
	});
}

DUCKDB_V2_ERROR duckdb_v2_statistics_set_string_max(duckdb_v2_statistics_handle statistics, duckdb_v2_str *value,
                                                    bool is_exact, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	DUCKDB_CHECK_ARG(value);
	return WithErrorHandler(err, [&]() {
		auto &stats = *Convert(statistics);
		ExpectStatsType("duckdb_v2_statistics_set_string_max", stats, StatisticsType::STRING_STATS, "string maximum");
		auto bytes = Convert(value);
		duckdb::string_t text(bytes.data(), duckdb::UnsafeNumericCast<uint32_t>(bytes.size()));
		StringStats::SetMax(stats, text, is_exact ? StringStatsType::EXACT_STATS : StringStatsType::TRUNCATED_STATS);
	});
}

DUCKDB_V2_ERROR duckdb_v2_statistics_set_constant(duckdb_v2_statistics_handle statistics, duckdb_v2_value_handle value,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	DUCKDB_CHECK_ARG(value);
	return WithErrorHandler(err, [&]() {
		auto &stats = *Convert(statistics);
		auto constant = StatisticsValue("duckdb_v2_statistics_set_constant", stats, *Convert(value));
		stats = BaseStatistics::FromConstant(constant);
	});
}

DUCKDB_V2_ERROR duckdb_v2_statistics_set_distinct_count(duckdb_v2_statistics_handle statistics, idx_t count,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	return WithErrorHandler(err, [&]() { Convert(statistics)->SetDistinctCount(count); });
}

DUCKDB_V2_ERROR duckdb_v2_statistics_set_child(duckdb_v2_statistics_handle statistics, idx_t child_index,
                                               duckdb_v2_statistics_handle child, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	return WithErrorHandler(err, [&]() {
		const char *const function = "duckdb_v2_statistics_set_child";
		auto &stats = *Convert(statistics);
		auto *new_child = child ? Convert(child) : nullptr;
		const auto stats_type = stats.GetStatsType();

		// Find the current statistics of the child, to validate the new child's type and to know the type a null
		// child is reset to.
		const BaseStatistics *current = nullptr;
		switch (stats_type) {
		case StatisticsType::LIST_STATS:
		case StatisticsType::ARRAY_STATS:
			if (child_index != 0) {
				throw InvalidInputException("%s: statistics of type %s have a single child (index 0), not child %llu",
				                            function, stats.GetType().ToString(),
				                            static_cast<unsigned long long>(child_index));
			}
			current = &(stats_type == StatisticsType::LIST_STATS ? ListStats::GetChildStats(stats)
			                                                     : ArrayStats::GetChildStats(stats));
			break;
		case StatisticsType::STRUCT_STATS:
			if (child_index >= StructType::GetChildCount(stats.GetType())) {
				throw InvalidInputException("%s: statistics of type %s have no child %llu", function,
				                            stats.GetType().ToString(), static_cast<unsigned long long>(child_index));
			}
			current = &StructStats::GetChildStats(stats, child_index);
			break;
		default:
			throw InvalidInputException("%s: statistics of type %s have no children", function,
			                            stats.GetType().ToString());
		}

		if (new_child && new_child->GetType() != current->GetType()) {
			throw InvalidInputException("%s: child %llu of statistics of type %s is of type %s, not %s", function,
			                            static_cast<unsigned long long>(child_index), stats.GetType().ToString(),
			                            current->GetType().ToString(), new_child->GetType().ToString());
		}

		// A null child resets the child's statistics to unknown.
		auto replacement =
		    new_child ? new_child->ToUnique() : BaseStatistics::CreateUnknown(current->GetType()).ToUnique();
		switch (stats_type) {
		case StatisticsType::LIST_STATS:
			ListStats::SetChildStats(stats, std::move(replacement));
			break;
		case StatisticsType::ARRAY_STATS:
			ArrayStats::SetChildStats(stats, std::move(replacement));
			break;
		default:
			StructStats::SetChildStats(stats, child_index, std::move(replacement));
			break;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_statistics_merge(duckdb_v2_statistics_handle statistics, duckdb_v2_statistics_handle other,
                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	DUCKDB_CHECK_ARG(other);
	return WithErrorHandler(err, [&]() {
		auto &stats = *Convert(statistics);
		auto &other_stats = *Convert(other);
		if (stats.GetType() != other_stats.GetType()) {
			throw InvalidInputException(
			    "duckdb_v2_statistics_merge: cannot merge statistics of type %s into statistics of type %s",
			    other_stats.GetType().ToString(), stats.GetType().ToString());
		}
		stats.Merge(other_stats);
	});
}

DUCKDB_V2_ERROR duckdb_v2_statistics_set_geometry_extent(duckdb_v2_statistics_handle statistics, double x_min,
                                                         double x_max, double y_min, double y_max, bool has_z,
                                                         double z_min, double z_max, bool has_m, double m_min,
                                                         double m_max, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	return WithErrorHandler(err, [&]() {
		auto &stats = *Convert(statistics);
		ExpectStatsType("duckdb_v2_statistics_set_geometry_extent", stats, StatisticsType::GEOMETRY_STATS,
		                "bounding box");
		if (x_min > x_max || y_min > y_max || (has_z && z_min > z_max) || (has_m && m_min > m_max)) {
			throw InvalidInputException(
			    "duckdb_v2_statistics_set_geometry_extent: a minimum of the bounding box is larger than its maximum");
		}
		auto &extent = GeometryStats::GetExtent(stats);
		extent.x_min = x_min;
		extent.x_max = x_max;
		extent.y_min = y_min;
		extent.y_max = y_max;
		if (has_z) {
			extent.z_min = z_min;
			extent.z_max = z_max;
		}
		if (has_m) {
			extent.m_min = m_min;
			extent.m_max = m_max;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_statistics_set_geometry_types(duckdb_v2_statistics_handle statistics,
                                                        const int32_t *wkb_types, idx_t count,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	if (count > 0) {
		DUCKDB_CHECK_ARG(wkb_types);
	}
	return WithErrorHandler(err, [&]() {
		auto &stats = *Convert(statistics);
		ExpectStatsType("duckdb_v2_statistics_set_geometry_types", stats, StatisticsType::GEOMETRY_STATS,
		                "geometry types");
		if (count == 0) {
			// No types given: the column may hold a geometry of any type.
			return;
		}
		for (idx_t i = 0; i < count; i++) {
			const auto code = wkb_types[i];
			const bool valid =
			    code >= 0 && (code / 1000) < GeometryTypeSet::VERT_TYPES && (code % 1000) < GeometryTypeSet::PART_TYPES;
			if (!valid) {
				throw InvalidInputException(
				    "duckdb_v2_statistics_set_geometry_types: %d is not an ISO WKB geometry type", code);
			}
		}
		auto &types = GeometryStats::GetTypes(stats);
		types.Clear();
		for (idx_t i = 0; i < count; i++) {
			types.AddWKBType(wkb_types[i]);
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_statistics_get_type(duckdb_v2_statistics_handle statistics,
                                              duckdb_v2_logical_type_handle *type, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	DUCKDB_CHECK_ARG(type);
	*type = nullptr;
	return WithErrorHandler(err, [&]() { *type = Convert(Convert(statistics)->GetType()); });
}

DUCKDB_V2_ERROR duckdb_v2_statistics_destroy(duckdb_v2_statistics_handle *statistics) {
	return WithErrorHandler(nullptr, [&]() {
		if (!statistics) {
			return;
		}
		if (*statistics) {
			delete Convert(*statistics);
			*statistics = nullptr;
		}
	});
}
