//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/alter_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

enum class AlterType : uint8_t {
	INVALID = 0,
	ALTER_TABLE = 1,
	ALTER_VIEW = 2,
	ALTER_SEQUENCE = 3,
	CHANGE_OWNERSHIP = 4,
	ALTER_FUNCTION = 5
};

struct AlterEntryData {
	AlterEntryData() {
	}
	AlterEntryData(string catalog_p, string schema_p, string name_p, bool if_exists)
	    : catalog(move(catalog_p)), schema(move(schema_p)), name(move(name_p)), if_exists(if_exists) {
	}

	string catalog;
	string schema;
	string name;
	bool if_exists;
};

struct AlterInfo : public ParseInfo {
	AlterInfo(AlterType type, string catalog, string schema, string name, bool if_exists);
	virtual ~AlterInfo() override;

	AlterType type;
	//! if exists
	bool if_exists;
	//! Catalog name to alter
	string catalog;
	//! Schema name to alter
	string schema;
	//! Entry name to alter
	string name;
	//! Allow altering internal entries
	bool allow_internal;

public:
	virtual CatalogType GetCatalogType() const = 0;
	virtual unique_ptr<AlterInfo> Copy() const = 0;
	void Serialize(Serializer &serializer) const;
	virtual void Serialize(FieldWriter &writer) const = 0;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source);

	AlterEntryData GetAlterEntryData() const;
};

} // namespace duckdb
