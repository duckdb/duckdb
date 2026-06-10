#include "duckdb/parser/parsed_data/alter_feature_info.hpp"

namespace duckdb {

AlterFeatureInfo::AlterFeatureInfo(AlterEntryData data, int64_t new_version)
    : AlterInfo(AlterType::ALTER_FEATURE, std::move(data.catalog), std::move(data.schema), std::move(data.name),
                data.if_not_found),
      new_version(new_version) {
}

AlterFeatureInfo::AlterFeatureInfo() : AlterInfo(AlterType::ALTER_FEATURE), new_version(0) {
}

AlterFeatureInfo::~AlterFeatureInfo() {
}

CatalogType AlterFeatureInfo::GetCatalogType() const {
	return CatalogType::FEATURE_ENTRY;
}

unique_ptr<AlterInfo> AlterFeatureInfo::Copy() const {
	return make_uniq_base<AlterInfo, AlterFeatureInfo>(GetAlterEntryData(), new_version);
}

string AlterFeatureInfo::ToString() const {
	throw NotImplementedException("NOT PARSABLE CURRENTLY");
}

} // namespace duckdb
