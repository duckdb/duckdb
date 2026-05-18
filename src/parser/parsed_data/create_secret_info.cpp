#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

CreateSecretInfo::CreateSecretInfo(OnCreateConflict on_conflict_p, SecretPersistType persist_type)
    : CreateInfo(CatalogType::SECRET_ENTRY), persist_type(persist_type), options() {
	on_conflict = on_conflict_p;
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

string CreateSecretInfo::ToString() const {
	string result;
	string create_type;
	if (persist_type == SecretPersistType::PERSISTENT) {
		create_type = "PERSISTENT SECRET";
	} else {
		create_type = "SECRET";
	}
	result = GetCreatePrefix(create_type);
	if (!name.empty()) {
		result += " " + SQLIdentifier(name);
	}
	if (!storage_type.empty()) {
		result += " IN" + SQLIdentifier(storage_type);
	}
	string option_list;
	if (provider) {
		option_list += "PROVIDER " + provider->ToString();
	}
	if (type) {
		if (!option_list.empty()) {
			option_list += ", ";
		}
		option_list += "TYPE " + type->ToString();
	}
	if (scope) {
		if (!option_list.empty()) {
			option_list += ", ";
		}
		option_list += "SCOPE " + scope->ToString();
	}
	for (auto &opt : options) {
		if (!option_list.empty()) {
			option_list += ", ";
		}
		option_list += opt.first + " " + opt.second->ToString();
	}
	result += "(" + option_list + ")";
	return result;
}

} // namespace duckdb
