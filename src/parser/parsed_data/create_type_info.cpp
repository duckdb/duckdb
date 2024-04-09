#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/extra_type_info.hpp"

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

bool CreateTypeInfo::HasToString() const {
	switch (type.id()) {
	case LogicalTypeId::ENUM:
		return true;
	case LogicalTypeId::USER:
		return true;
	default:
		return false;
	}
}

string CreateTypeInfo::ToString() const {
	string result = "";
	result += "CREATE TYPE ";
	if (!catalog.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(catalog);
		result += ".";
	}
	if (!schema.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(schema);
		result += ".";
	}
	result += KeywordHelper::WriteOptionallyQuoted(name);
	if (type.id() == LogicalTypeId::ENUM) {
		auto &values_insert_order = EnumType::GetValuesInsertOrder(type);
		idx_t size = EnumType::GetSize(type);

		result += " AS ENUM ( ";
		for (idx_t i = 0; i < size; i++) {
			result += "'" + values_insert_order.GetValue(i).ToString() + "'";
			if (i != size - 1) {
				result += ", ";
			}
		}
		result += " );";
	} else if (type.id() == LogicalTypeId::USER) {
		result += " AS ";
		auto extra_info = type.AuxInfo();
		D_ASSERT(extra_info);
		D_ASSERT(extra_info->type == ExtraTypeInfoType::USER_TYPE_INFO);
		auto &user_info = extra_info->Cast<UserTypeInfo>();
		// FIXME: catalog, schema ??
		result += user_info.user_type_name;
	} else {
		throw InternalException("CreateTypeInfo::ToString() not implemented for %s", LogicalTypeIdToString(type.id()));
	}
	return result;
}

} // namespace duckdb
