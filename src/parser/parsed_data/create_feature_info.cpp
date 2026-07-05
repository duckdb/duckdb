#include "duckdb/parser/parsed_data/create_feature_info.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/interval.hpp"

namespace duckdb {

static bool IntervalEquals(const interval_t &left, const interval_t &right) {
	return left.months == right.months && left.days == right.days && left.micros == right.micros;
}

CreateFeatureInfo::CreateFeatureInfo()
    : CreateInfo(CatalogType::FEATURE_ENTRY, INVALID_SCHEMA), window_interval(interval_t {0, 1, 0}),
      watermark_interval(interval_t {0, 0, 0}), refresh_mode(FeatureRefreshMode::FULL), retain_versions(1),
      current_version(0), has_schedule(false), schedule_interval(interval_t {0, 0, 0}), schedule_enabled(true) {
}

unique_ptr<CreateInfo> CreateFeatureInfo::Copy() const {
	auto result = make_uniq<CreateFeatureInfo>();
	CopyProperties(*result);
	result->feature_name = feature_name;
	result->source_table = source_table;
	result->entity_columns = entity_columns;
	result->timestamp_column = timestamp_column;
	result->window_interval = window_interval;
	result->watermark_interval = watermark_interval;
	result->refresh_mode = refresh_mode;
	result->retain_versions = retain_versions;
	result->current_version = current_version;
	result->has_schedule = has_schedule;
	result->schedule_interval = schedule_interval;
	result->schedule_enabled = schedule_enabled;
	result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	result->result_names = result_names;
	result->result_types = result_types;
	return std::move(result);
}

string CreateFeatureInfo::ToString() const {
	string result;
	result += "CREATE";
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		result += " OR REPLACE";
	}
	result += " FEATURE ";
	if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		result += "IF NOT EXISTS ";
	}
	result += feature_name;
	result += " TIMESTAMP " + timestamp_column;
	result += " WINDOW INTERVAL '" + Interval::ToString(window_interval) + "'";
	if (!IntervalEquals(watermark_interval, interval_t {0, 0, 0})) {
		result += " WATERMARK INTERVAL '" + Interval::ToString(watermark_interval) + "'";
	}
	result += " REFRESH ";
	switch (refresh_mode) {
	case FeatureRefreshMode::FULL:
		result += "FULL";
		break;
	case FeatureRefreshMode::INCREMENTAL:
		result += "INCREMENTAL";
		break;
	}
	if (has_schedule) {
		result += " EVERY INTERVAL '" + Interval::ToString(schedule_interval) + "'";
	}
	result += " RETAIN " + duckdb::to_string(retain_versions);
	result += " AS (" + query->ToString() + ")";
	return result;
}

void CreateFeatureInfo::FinalizeDeserialization() {
	if (IntervalEquals(window_interval, interval_t {0, 0, 0})) {
		window_interval = interval_t {0, 1, 0};
	}
}

} // namespace duckdb
