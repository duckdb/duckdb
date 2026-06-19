#include "duckdb/parser/parsed_data/alter_feature_info.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/sql_identifier.hpp"

namespace duckdb {

AlterFeatureInfo::AlterFeatureInfo(AlterEntryData data, int64_t new_version)
    : AlterInfo(AlterType::ALTER_FEATURE, std::move(data.catalog), std::move(data.schema), std::move(data.name),
                data.if_not_found),
      alter_feature_type(AlterFeatureType::BUMP_VERSION), new_version(new_version), schedule_interval({0, 0, 0}) {
}

AlterFeatureInfo::AlterFeatureInfo(AlterEntryData data, AlterFeatureType type, interval_t schedule_interval)
    : AlterInfo(AlterType::ALTER_FEATURE, std::move(data.catalog), std::move(data.schema), std::move(data.name),
                data.if_not_found),
      alter_feature_type(type), new_version(0), schedule_interval(schedule_interval) {
}

AlterFeatureInfo::AlterFeatureInfo()
    : AlterInfo(AlterType::ALTER_FEATURE), alter_feature_type(AlterFeatureType::INVALID), new_version(0),
      schedule_interval({0, 0, 0}) {
}

AlterFeatureInfo::~AlterFeatureInfo() {
}

CatalogType AlterFeatureInfo::GetCatalogType() const {
	return CatalogType::FEATURE_ENTRY;
}

unique_ptr<AlterInfo> AlterFeatureInfo::Copy() const {
	unique_ptr<AlterFeatureInfo> result;
	if (alter_feature_type == AlterFeatureType::BUMP_VERSION) {
		result = make_uniq<AlterFeatureInfo>(GetAlterEntryData(), new_version);
	} else {
		result = make_uniq<AlterFeatureInfo>(GetAlterEntryData(), alter_feature_type, schedule_interval);
	}
	return std::move(result);
}

string AlterFeatureInfo::ToString() const {
	string result = "ALTER FEATURE ";
	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		result += "IF EXISTS ";
	}
	result += SQLIdentifier::ToString(name);
	switch (alter_feature_type) {
	case AlterFeatureType::SET_SCHEDULE:
		result += " SET SCHEDULE EVERY INTERVAL '" + Value::INTERVAL(schedule_interval).ToString() + "'";
		break;
	case AlterFeatureType::ENABLE_SCHEDULE:
		result += " ENABLE SCHEDULE";
		break;
	case AlterFeatureType::DISABLE_SCHEDULE:
		result += " DISABLE SCHEDULE";
		break;
	default:
		throw NotImplementedException("Cannot render this AlterFeatureType to SQL");
	}
	result += ";";
	return result;
}

} // namespace duckdb
