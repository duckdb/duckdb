#include "duckdb/parser/parsed_data/create_secret_info.hpp"

#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

CreateSecretInfo::CreateSecretInfo(OnCreateConflict on_conflict, SecretPersistMode persist_mode)
    : CreateInfo(CatalogType::SECRET_ENTRY), on_conflict(on_conflict), persist_mode(persist_mode), options() {
}

unique_ptr<CreateInfo> CreateSecretInfo::Copy() const {
	auto result = make_uniq<CreateSecretInfo>(on_conflict, persist_mode);
	result->type = type;
	result->provider = provider;
	result->name = name;
	result->scope = scope;
	result->options = options;
	return result;
}

} // namespace duckdb
