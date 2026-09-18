//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/alter_database_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/alter_info.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {

enum class AlterDatabaseType : uint8_t { RENAME_DATABASE = 0 };

struct AlterDatabaseInfo : public AlterInfo {
public:
	explicit AlterDatabaseInfo(AlterDatabaseType alter_database_type);
	AlterDatabaseInfo(AlterDatabaseType alter_database_type, Identifier catalog_p, OnEntryNotFound if_not_found);
	~AlterDatabaseInfo() override;

	AlterDatabaseType alter_database_type;

public:
	CatalogType GetCatalogType() const override;
	string ToString() const override = 0;

	static unique_ptr<AlterInfo> Deserialize(Deserializer &deserializer);

protected:
	void Serialize(Serializer &serializer) const override;
};

struct RenameDatabaseInfo : public AlterDatabaseInfo {
public:
	RenameDatabaseInfo();
	RenameDatabaseInfo(Identifier catalog_p, Identifier new_name_p, OnEntryNotFound if_not_found);

	Identifier new_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;

	static unique_ptr<AlterDatabaseInfo> Deserialize(Deserializer &deserializer);

protected:
	void Serialize(Serializer &serializer) const override;
};

} // namespace duckdb
