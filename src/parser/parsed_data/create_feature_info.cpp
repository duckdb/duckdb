#include "duckdb/parser/parsed_data/create_feature_info.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

CreateFeatureInfo::CreateFeatureInfo()
    : CreateInfo(CatalogType::FEATURE_ENTRY, INVALID_SCHEMA), granularity(FeatureGranularity::DAY), window_size(7),
      refresh_mode(FeatureRefreshMode::FULL), retain_versions(1) {
}

unique_ptr<CreateInfo> CreateFeatureInfo::Copy() const {
	auto result = make_uniq<CreateFeatureInfo>();
	CopyProperties(*result);
	result->feature_name = feature_name;
	result->source_table = source_table;
	result->entity_column = entity_column;
	result->timestamp_column = timestamp_column;
	result->granularity = granularity;
	result->window_size = window_size;
	result->refresh_mode = refresh_mode;
	result->retain_versions = retain_versions;
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
	result += " ON " + source_table;
	result += " ENTITY " + entity_column;
	result += " TIMESTAMP " + timestamp_column;
	result += " GRANULARITY ";
	switch (granularity) {
	case FeatureGranularity::DAY:
		result += "DAY";
		break;
	case FeatureGranularity::HOUR:
		result += "HOUR";
		break;
	case FeatureGranularity::MINUTE:
		result += "MINUTE";
		break;
	}
	result += " WINDOW " + duckdb::to_string(window_size);
	result += " REFRESH ";
	switch (refresh_mode) {
	case FeatureRefreshMode::FULL:
		result += "FULL";
		break;
	case FeatureRefreshMode::INCREMENTAL:
		result += "INCREMENTAL";
		break;
	}
	result += " RETAIN " + duckdb::to_string(retain_versions);
	result += " AS (" + query->ToString() + ")";
	return result;
}

} // namespace duckdb
