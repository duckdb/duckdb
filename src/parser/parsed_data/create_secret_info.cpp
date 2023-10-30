#include "duckdb/parser/parsed_data/create_secret_info.hpp"

#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

CreateSecretInfo::CreateSecretInfo(string type, OnCreateConflict on_conflict)
    : ParseInfo(ParseInfoType::CREATE_SECRET_INFO), on_conflict(on_conflict), type(type), named_parameters() {
}

unique_ptr<CreateSecretInfo> CreateSecretInfo::Copy() const {
	auto result = make_uniq<CreateSecretInfo>(type, on_conflict);
	result->named_parameters = named_parameters;
	result->mode = mode;
	return result;
}

} // namespace duckdb
