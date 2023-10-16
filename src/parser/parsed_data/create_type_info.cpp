#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CreateTypeInfo::CreateTypeInfo() : CreateInfo(CatalogType::TYPE_ENTRY) {
}
CreateTypeInfo::CreateTypeInfo(string name_p, LogicalType type_p)
    : CreateInfo(CatalogType::TYPE_ENTRY), name(std::move(name_p)), type(std::move(type_p)) {
}

unique_ptr<CreateInfo> CreateTypeInfo::Copy() const {
	auto result = make_uniq<CreateTypeInfo>();
	CopyProperties(*result);
	result->name = name;
	result->type = type;
	if (query) {
		result->query = query->Copy();
	}
	return std::move(result);
}

string CreateTypeInfo::ToString() const {
	string result = "";
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto &values_insert_order = EnumType::GetValuesInsertOrder(type);
	idx_t size = EnumType::GetSize(type);
	result += "CREATE TYPE ";
	result += KeywordHelper::WriteOptionallyQuoted(name);
	result += " AS ENUM ( ";

	for (idx_t i = 0; i < size; i++) {
		result += "'" + values_insert_order.GetValue(i).ToString() + "'";
		if (i != size - 1) {
			result += ", ";
		}
	}
	result += " );";
	return result;
}

} // namespace duckdb
