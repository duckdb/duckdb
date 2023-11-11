//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_secret_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/named_parameter_map.hpp"

namespace duckdb {

enum class SecretPersistMode : uint8_t {
	DEFAULT,
	TEMPORARY,
	PERMANENT
};

struct CreateSecretInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::CREATE_SECRET_INFO;

public:
	explicit CreateSecretInfo(OnCreateConflict on_conflict, SecretPersistMode persist_mode);
	//! How to handle conflict
	OnCreateConflict on_conflict;
	//! Should the secret be persisted
	SecretPersistMode persist_mode;
	//! The type of secret
	string type;
	//! (optionally) the provider of the secret credentials
	string provider;
	//! (optionally) the name of the secret
	string name;
	//! (optionally) the scope of the secret
	vector<string> scope;
	//! Named parameter list (if any)
	named_parameter_map_t named_parameters;

	unique_ptr<CreateSecretInfo> Copy() const;
};

} // namespace duckdb
