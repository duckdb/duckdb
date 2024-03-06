#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

SetColumnCommentInfo::SetColumnCommentInfo()
    : AlterInfo(AlterType::SET_COLUMN_COMMENT, INVALID_CATALOG, INVALID_SCHEMA, "", OnEntryNotFound::THROW_EXCEPTION),
      catalog_entry_type(CatalogType::INVALID), column_name(""), comment_value(Value()) {
}

SetColumnCommentInfo::SetColumnCommentInfo(string catalog, string schema, string name, string column_name,
                                           Value comment_value, OnEntryNotFound if_not_found)
    : AlterInfo(AlterType::SET_COLUMN_COMMENT, std::move(catalog), std::move(schema), std::move(name), if_not_found),
      catalog_entry_type(CatalogType::INVALID), column_name(std::move(column_name)),
      comment_value(std::move(comment_value)) {
}

unique_ptr<AlterInfo> SetColumnCommentInfo::Copy() const {
	auto result = make_uniq<SetColumnCommentInfo>(catalog, schema, name, column_name, comment_value, if_not_found);
	result->type = type;
	return std::move(result);
}

optional_ptr<CatalogEntry> SetColumnCommentInfo::TryResolveCatalogEntry(ClientContext &context) {
	auto entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, catalog, schema, name, if_not_found);

	if (entry) {
		catalog_entry_type = entry->type;
		return entry;
	}

	return nullptr;
}

// Note: this is a bit of a weird one: the exact type is not yet known: it can be either a view or a column this needs
//       to be resolved at bind time. If type is invalid here, the CommentOnColumnInfo was not properly resolved first
CatalogType SetColumnCommentInfo::GetCatalogType() const {
	if (catalog_entry_type == CatalogType::INVALID) {
		throw InternalException("Attempted to access unresolved ");
	}
	return catalog_entry_type;
}

} // namespace duckdb
