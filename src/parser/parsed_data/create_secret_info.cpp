#include "duckdb/parser/parsed_data/create_secret_info.hpp"

#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

CreateSecretInfo::CreateSecretInfo(OnCreateConflict on_conflict, SecretPersistType persist_type)
    : CreateInfo(CatalogType::SECRET_ENTRY), on_conflict(on_conflict), persist_type(persist_type), options() {
}

unique_ptr<CreateInfo> CreateSecretInfo::Copy() const {
	auto result = make_uniq<CreateSecretInfo>(on_conflict, persist_type);
	result->type = type;
	result->storage_type = storage_type;
	result->provider = provider;
	result->name = name;
	result->scope = scope;
	result->options = options;
	return std::move(result);
}

} // namespace duckdb
