//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/comment_on_column_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/catalog/catalog_entry_retriever.hpp"

namespace duckdb {
class ClientContext;
class CatalogEntry;

struct SetColumnCommentInfo : public AlterInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::COMMENT_ON_COLUMN_INFO;

public:
	SetColumnCommentInfo();
	SetColumnCommentInfo(string catalog, string schema, string name, string column_name, Value comment_value,
	                     OnEntryNotFound if_not_found);

	//! The resolved Catalog Type
	CatalogType catalog_entry_type;

	//! name of the column to comment on
	string column_name;
	//! The comment, can be NULL or a string
	Value comment_value;

public:
	optional_ptr<CatalogEntry> TryResolveCatalogEntry(CatalogEntryRetriever &retriever);
	unique_ptr<AlterInfo> Copy() const override;
	CatalogType GetCatalogType() const override;
	string ToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
