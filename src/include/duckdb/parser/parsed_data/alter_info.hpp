//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/alter_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"

namespace duckdb {

enum class AlterType : uint8_t {
	INVALID = 0,
	ALTER_TABLE = 1,
	ALTER_VIEW = 2,
	ALTER_SEQUENCE = 3,
	CHANGE_OWNERSHIP = 4,
	ALTER_SCALAR_FUNCTION = 5,
	ALTER_TABLE_FUNCTION = 6,
	SET_COMMENT = 7,
	SET_COLUMN_COMMENT = 8,
	ALTER_DATABASE = 9
};

struct AlterEntryData {
	AlterEntryData() {
	}
	AlterEntryData(string catalog_p, string schema_p, string name_p, OnEntryNotFound if_not_found)
	    : catalog(std::move(catalog_p)), schema(std::move(schema_p)), name(std::move(name_p)),
	      if_not_found(if_not_found) {
	}

	string catalog;
	string schema;
	string name;
	OnEntryNotFound if_not_found;
};

struct AlterInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::ALTER_INFO;

public:
	AlterInfo(AlterType type, string catalog, string schema, string name, OnEntryNotFound if_not_found);
	~AlterInfo() override;

	AlterType type;
	//! if exists
	OnEntryNotFound if_not_found;
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
	virtual string ToString() const = 0;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);

	virtual string GetColumnName() const {
		return "";
	};

	AlterEntryData GetAlterEntryData() const;
	bool IsAddPrimaryKey() const;

protected:
	explicit AlterInfo(AlterType type);
};

} // namespace duckdb
