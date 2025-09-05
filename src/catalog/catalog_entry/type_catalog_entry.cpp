#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include <algorithm>
#include <sstream>

namespace duckdb {

constexpr const char *TypeCatalogEntry::Name;

TypeCatalogEntry::TypeCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTypeInfo &info)
    : StandardEntry(CatalogType::TYPE_ENTRY, schema, catalog, info.name), user_type(info.type),
      bind_function(info.bind_function) {
	this->temporary = info.temporary;
	this->internal = info.internal;
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
	result->dependencies = dependencies;
	result->comment = comment;
	result->tags = tags;
	result->bind_function = bind_function;
	return std::move(result);
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
