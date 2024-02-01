#include "duckdb/parser/parsed_data/extra_drop_info.hpp"

namespace duckdb {

ExtraDropSecretInfo::ExtraDropSecretInfo() : ExtraDropInfo(ExtraDropInfoType::SECRET_INFO) {
}

ExtraDropSecretInfo::ExtraDropSecretInfo(const ExtraDropSecretInfo &info)
    : ExtraDropInfo(ExtraDropInfoType::SECRET_INFO) {
}

unique_ptr<ExtraDropInfo> ExtraDropSecretInfo::Copy() const {
	return std::move(make_uniq<ExtraDropSecretInfo>(*this));
}

} // namespace duckdb
