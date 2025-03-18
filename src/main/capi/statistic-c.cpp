#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/main/capi/capi_internal.hpp"

#include <string.h>

duckdb_base_statistic duckdb_create_base_statistic(duckdb_logical_type type) {
	auto dtype = reinterpret_cast<duckdb::LogicalType *>(type);
	auto stats = new duckdb::BaseStatistics(duckdb::BaseStatistics::CreateEmpty(*dtype));
	return reinterpret_cast<duckdb_base_statistic>(stats);
}

void duckdb_destroy_base_statistic(duckdb_base_statistic *statistic) {
	if (statistic && *statistic) {
		auto dstatistic = reinterpret_cast<duckdb::BaseStatistics *>(statistic);
		delete dstatistic;
		statistic = nullptr;
	}
}

void duckdb_statistic_set_min(duckdb_base_statistic statistic, duckdb_value min, bool is_truncated) {
	auto dstatistic = reinterpret_cast<duckdb::BaseStatistics *>(statistic);
	auto dmin = reinterpret_cast<duckdb::Value *>(min);
	switch (dstatistic->GetStatsType()) {
	case duckdb::StatisticsType::NUMERIC_STATS:
		duckdb::NumericStats::SetMin(*dstatistic, *dmin);
		break;
	case duckdb::StatisticsType::STRING_STATS: {
		// Update uses a comparison to decide if this is a min or max.
		auto str = duckdb::StringValue::Get(*dmin);
		duckdb::StringStats::Update(*dstatistic, duckdb::string_t(str));
		if (is_truncated) {
			duckdb::StringStats::ResetMaxStringLength(*dstatistic);
		}
		break;
	}
	default:
		throw duckdb::NotImplementedException("Stats only implemented for numeric and string types");
	}
}

void duckdb_statistic_set_max(duckdb_base_statistic statistic, duckdb_value max, bool is_truncated) {
	auto dstatistic = reinterpret_cast<duckdb::BaseStatistics *>(statistic);
	auto dmax = reinterpret_cast<duckdb::Value *>(max);
	switch (dstatistic->GetStatsType()) {
	case duckdb::StatisticsType::NUMERIC_STATS:
		duckdb::NumericStats::SetMax(*dstatistic, *dmax);
		break;
	case duckdb::StatisticsType::STRING_STATS: {
		auto str = duckdb::StringValue::Get(*dmax);
		duckdb::StringStats::Update(*dstatistic, duckdb::string_t(str));
		if (is_truncated) {
			duckdb::StringStats::ResetMaxStringLength(*dstatistic);
		}
		break;
	}
	default:
		throw duckdb::NotImplementedException("Stats only implemented for numeric and string types");
	}
}

void duckdb_statistic_set_has_nulls(duckdb_base_statistic statistic) {
	auto dstatistic = reinterpret_cast<duckdb::BaseStatistics *>(statistic);
	dstatistic->SetHasNull();
}

void duckdb_statistic_set_has_no_nulls(duckdb_base_statistic statistic) {
	auto dstatistic = reinterpret_cast<duckdb::BaseStatistics *>(statistic);
	dstatistic->SetHasNoNull();
}
