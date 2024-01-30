#include "duckdb/parser/parsed_data/drop_secret_info.hpp"

namespace duckdb {

DropSecretInfo::DropSecretInfo() : DropInfo(), persist_mode(SecretPersistType::DEFAULT), secret_storage("") {
	type = CatalogType::SECRET_ENTRY;
}

DropSecretInfo::DropSecretInfo(const DropSecretInfo &info)
    : DropInfo(info), persist_mode(info.persist_mode), secret_storage(info.secret_storage) {
}

unique_ptr<DropInfo> DropSecretInfo::Copy() const {
	return unique_ptr_cast<DropInfo, DropSecretInfo>(make_uniq<DropSecretInfo>(*this));
}

} // namespace duckdb
