#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include <algorithm>
#include <sstream>

namespace duckdb {

constexpr const char *TypeCatalogEntry::Name;

TypeCatalogEntry::TypeCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTypeInfo &info)
    : StandardEntry(CatalogType::TYPE_ENTRY, schema, catalog, info.name), user_type(info.type),
      bind_function(info.bind_function), field_comments(info.field_comments_map) {
	this->temporary = info.temporary;
	this->internal = info.internal;
	this->extension_name = info.extension_name;
	this->dependencies = info.dependencies;
	this->comment = info.comment;
	this->tags = info.tags;
}

unique_ptr<CatalogEntry> TypeCatalogEntry::Copy(ClientContext &context) const {
	auto info_copy = GetInfo();
	auto &cast_info = info_copy->Cast<CreateTypeInfo>();
	auto result = make_uniq<TypeCatalogEntry>(catalog, schema, cast_info);
	return std::move(result);
}

unique_ptr<CreateInfo> TypeCatalogEntry::GetInfo() const {
	auto result = make_uniq<CreateTypeInfo>();
	result->catalog = catalog.GetName();
	result->schema = schema.name;
	result->name = name;
	result->type = user_type;
	result->extension_name = extension_name;
	result->dependencies = dependencies;
	result->comment = comment;
	result->tags = tags;
	result->bind_function = bind_function;
	result->field_comments_map = field_comments;
	return std::move(result);
}

unique_ptr<CatalogEntry> TypeCatalogEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
	D_ASSERT(!internal);

	if (info.type == AlterType::SET_COLUMN_COMMENT) {
		auto &comment_on_column_info = info.Cast<SetColumnCommentInfo>();
		auto copied_type = Copy(context);

		// Verify that the type is a STRUCT and the field exists
		if (user_type.id() != LogicalTypeId::STRUCT) {
			throw CatalogException("Type \"%s\" is not a STRUCT type, cannot comment on its fields", name);
		}
		auto &child_types = StructType::GetChildTypes(user_type);
		bool found = false;
		for (auto &child : child_types) {
			if (StringUtil::CIEquals(child.first, comment_on_column_info.column_name)) {
				found = true;
				break;
			}
		}
		if (!found) {
			throw CatalogException("Type \"%s\" does not have a field with name \"%s\"", name,
			                       comment_on_column_info.column_name);
		}

		auto &copied_type_entry = copied_type->Cast<TypeCatalogEntry>();
		copied_type_entry.field_comments[comment_on_column_info.column_name] = comment_on_column_info.comment_value;
		return copied_type;
	}

	throw CatalogException("Unsupported alter type for type entry");
}

Value TypeCatalogEntry::GetFieldComment(const string &field_name) {
	auto entry = field_comments.find(field_name);
	if (entry != field_comments.end()) {
		return entry->second;
	}
	return Value();
}

string TypeCatalogEntry::ToSQL() const {
	duckdb::stringstream ss;
	ss << "CREATE TYPE ";
	ss << KeywordHelper::WriteOptionallyQuoted(name);
	ss << " AS ";

	auto user_type_copy = user_type;

	// Strip off the potential alias so ToString doesn't just output the alias
	user_type_copy.SetAlias("");
	D_ASSERT(user_type_copy.GetAlias().empty());

	ss << user_type_copy.ToString();
	ss << ";";
	return ss.str();
}

} // namespace duckdb
