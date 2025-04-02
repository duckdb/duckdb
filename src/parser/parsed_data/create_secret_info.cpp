#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

CreateSecretInfo::CreateSecretInfo(OnCreateConflict on_conflict, SecretPersistType persist_type)
    : CreateInfo(CatalogType::SECRET_ENTRY), on_conflict(on_conflict), persist_type(persist_type), options() {
}

CreateSecretInfo::~CreateSecretInfo() {
}

unique_ptr<CreateInfo> CreateSecretInfo::Copy() const {
	auto result = make_uniq<CreateSecretInfo>(on_conflict, persist_type);

	result->storage_type = storage_type;
	result->name = name;

	if (type) {
		result->type = type->Copy();
	}
	if (provider) {
		result->provider = provider->Copy();
	}
	if (scope) {
		result->scope = scope->Copy();
	}

	for (const auto &option : options) {
		result->options.insert(make_pair(option.first, option.second->Copy()));
	}

	return std::move(result);
}

} // namespace duckdb
