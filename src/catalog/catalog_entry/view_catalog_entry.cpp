#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "duckdb/common/limits.hpp"

#include <algorithm>

namespace duckdb {

void ViewCatalogEntry::Initialize(CreateViewInfo &info) {
	query = std::move(info.query);
	this->aliases = info.aliases;
	this->types = info.types;
	this->names = info.names;
	this->temporary = info.temporary;
	this->sql = info.sql;
	this->internal = info.internal;
	this->dependencies = info.dependencies;
	this->comment = info.comment;
	this->tags = info.tags;
	this->column_comments = info.column_comments;
}

ViewCatalogEntry::ViewCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info)
    : StandardEntry(CatalogType::VIEW_ENTRY, schema, catalog, info.view_name) {
	Initialize(info);
}

unique_ptr<CreateInfo> ViewCatalogEntry::GetInfo() const {
	auto result = make_uniq<CreateViewInfo>();
	result->schema = schema.name;
	result->view_name = name;
	result->sql = sql;
	result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	result->aliases = aliases;
	result->names = names;
	result->types = types;
	result->temporary = temporary;
	result->dependencies = dependencies;
	result->comment = comment;
	result->tags = tags;
	result->column_comments = column_comments;
	return std::move(result);
}

unique_ptr<CatalogEntry> ViewCatalogEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
	D_ASSERT(!internal);

	// Column comments have a special alter type
	if (info.type == AlterType::SET_COLUMN_COMMENT) {
		auto &comment_on_column_info = info.Cast<SetColumnCommentInfo>();
		auto copied_view = Copy(context);

		for (idx_t i = 0; i < names.size(); i++) {
			const auto &col_name = names[i];
			if (col_name == comment_on_column_info.column_name) {
				auto &copied_view_entry = copied_view->Cast<ViewCatalogEntry>();

				// If vector is empty, we need to initialize it on setting here
				if (copied_view_entry.column_comments.empty()) {
					copied_view_entry.column_comments = vector<Value>(copied_view_entry.types.size());
				}

				copied_view_entry.column_comments[i] = comment_on_column_info.comment_value;
				return copied_view;
			}
		}
		throw BinderException("View \"%s\" does not have a column with name \"%s\"", name,
		                      comment_on_column_info.column_name);
	}

	if (info.type != AlterType::ALTER_VIEW) {
		throw CatalogException("Can only modify view with ALTER VIEW statement");
	}
	auto &view_info = info.Cast<AlterViewInfo>();
	switch (view_info.alter_view_type) {
	case AlterViewType::RENAME_VIEW: {
		auto &rename_info = view_info.Cast<RenameViewInfo>();
		auto copied_view = Copy(context);
		copied_view->name = rename_info.new_view_name;
		return copied_view;
	}
	default:
		throw InternalException("Unrecognized alter view type!");
	}
}

string ViewCatalogEntry::ToSQL() const {
	if (sql.empty()) {
		//! Return empty sql with view name so pragma view_tables don't complain
		return sql;
	}
	auto info = GetInfo();
	auto result = info->ToString();
	return result;
}

unique_ptr<CatalogEntry> ViewCatalogEntry::Copy(ClientContext &context) const {
	D_ASSERT(!internal);
	auto create_info = GetInfo();

	return make_uniq<ViewCatalogEntry>(catalog, schema, create_info->Cast<CreateViewInfo>());
}

} // namespace duckdb
