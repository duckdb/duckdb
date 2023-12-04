#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include <algorithm>
#include <sstream>

namespace duckdb {

TypeCatalogEntry::TypeCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTypeInfo &info)
    : StandardEntry(CatalogType::TYPE_ENTRY, schema, catalog, info.name), user_type(info.type) {
	this->temporary = info.temporary;
	this->internal = info.internal;
}

unique_ptr<CreateInfo> TypeCatalogEntry::GetInfo() const {
	auto result = make_uniq<CreateTypeInfo>();
	result->catalog = catalog.GetName();
	result->schema = schema.name;
	result->name = name;
	result->type = user_type;
	return std::move(result);
}

string TypeCatalogEntry::ToSQL() const {
	std::stringstream ss;
	switch (user_type.id()) {
	case (LogicalTypeId::ENUM): {
		auto &values_insert_order = EnumType::GetValuesInsertOrder(user_type);
		idx_t size = EnumType::GetSize(user_type);
		ss << "CREATE TYPE ";
		ss << KeywordHelper::WriteOptionallyQuoted(name);
		ss << " AS ENUM ( ";

		for (idx_t i = 0; i < size; i++) {
			ss << "'" << values_insert_order.GetValue(i).ToString() << "'";
			if (i != size - 1) {
				ss << ", ";
			}
		}
		ss << ");";
		break;
	}
	default:
		throw InternalException("Logical Type can't be used as a User Defined Type");
	}

	return ss.str();
}

} // namespace duckdb
