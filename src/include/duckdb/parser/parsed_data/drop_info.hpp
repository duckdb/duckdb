//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/drop_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/parser/parsed_data/extra_drop_info.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {
struct ExtraDropInfo;

struct DropInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::DROP_INFO;

public:
	DropInfo();
	DropInfo(const DropInfo &info);

	//! The catalog type to drop
	CatalogType type;
	//! Catalog name to drop from, if any
	string catalog;
	//! Schema name to drop from, if any
	string schema;
	//! Element name to drop
	string name;
	//! Ignore if the entry does not exist instead of failing
	OnEntryNotFound if_not_found = OnEntryNotFound::THROW_EXCEPTION;
	//! Cascade drop (drop all dependents instead of throwing an error if there
	//! are any)
	bool cascade = false;
	//! Allow dropping of internal system entries
	bool allow_drop_internal = false;
	//! Extra info related to this drop
	unique_ptr<ExtraDropInfo> extra_drop_info;

public:
	//! NOTE(backport): see the note on CreateInfo::GetQualifiedName - assembled on demand, returned by value.
	QualifiedName GetQualifiedName() const {
		return QualifiedName(catalog, schema, name);
	}
	//! NOTE(backport): DuckDB 2.0 takes the `QualifiedName` by value and moves it into the stored member; here the
	//! separate string members are copied out of it, so a const reference avoids a pointless copy.
	void SetQualifiedName(const QualifiedName &qualified_name) {
		catalog = qualified_name.catalog;
		schema = qualified_name.schema;
		name = qualified_name.name;
	}
	void SetQualifiedName(string catalog_p, string schema_p, string name_p) {
		SetQualifiedName(QualifiedName(std::move(catalog_p), std::move(schema_p), std::move(name_p)));
	}

	virtual unique_ptr<DropInfo> Copy() const;
	string ToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
