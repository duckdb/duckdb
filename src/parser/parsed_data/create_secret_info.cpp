#include "duckdb/parser/parsed_data/create_secret_info.hpp"

#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

CreateSecretInfo::CreateSecretInfo(OnCreateConflict on_conflict, const string &storage_type)
    : CreateInfo(CatalogType::SECRET_ENTRY), on_conflict(on_conflict), storage_type(storage_type), options() {
}

unique_ptr<CreateInfo> CreateSecretInfo::Copy() const {
	auto result = make_uniq<CreateSecretInfo>(on_conflict, storage_type);
	result->type = type;
	result->provider = provider;
	result->name = name;
	result->scope = scope;
	result->options = options;
	return std::move(result);
}

} // namespace duckdb
