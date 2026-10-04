//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/set_tags_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"

namespace duckdb {
class CatalogEntry;
class CatalogEntryRetriever;

enum class TagAction : uint8_t { SET = 0, UNSET = 1 };

struct TagActionInfo {
	TagAction action = TagAction::SET;
	InsertionOrderPreservingMap<string> tags;
	vector<string> tag_names;
};

struct SetTagsInfo : public AlterInfo {
public:
	SetTagsInfo();
	SetTagsInfo(CatalogType entry_catalog_type, QualifiedName name, Identifier column_name, TagActionInfo action_info,
	            OnEntryNotFound if_not_found);

	//! The resolved catalog type. COLUMN targets are resolved to TABLE or VIEW during binding.
	CatalogType entry_catalog_type;
	//! Empty for object tags, otherwise the column name.
	Identifier column_name;
	TagAction action;
	InsertionOrderPreservingMap<string> tags;
	vector<string> tag_names;

public:
	bool IsColumn() const;
	optional_ptr<CatalogEntry> TryResolveCatalogEntry(CatalogEntryRetriever &retriever);
	void Apply(InsertionOrderPreservingMap<string> &target) const;

	unique_ptr<AlterInfo> Copy() const override;
	CatalogType GetCatalogType() const override;
	string ToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
