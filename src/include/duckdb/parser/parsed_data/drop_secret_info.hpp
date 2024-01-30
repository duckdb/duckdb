//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/drop_secret_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

struct DropSecretInfo : public DropInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::DROP_INFO;

	DropSecretInfo();
	DropSecretInfo(const DropSecretInfo &info);

	//! Secret Persistence
	SecretPersistType persist_mode;
	//! (optional) the name of the storage to drop from
	string secret_storage;

	unique_ptr<DropInfo> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<DropInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
