#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CreateTableInfo::CreateTableInfo() : CreateInfo(CatalogType::TABLE_ENTRY, INVALID_SCHEMA) {
}

CreateTableInfo::CreateTableInfo(string catalog_p, string schema_p, string name_p)
    : CreateInfo(CatalogType::TABLE_ENTRY, std::move(schema_p), std::move(catalog_p)), table(std::move(name_p)) {
}

CreateTableInfo::CreateTableInfo(SchemaCatalogEntry &schema, string name_p)
    : CreateTableInfo(schema.catalog.GetName(), schema.name, std::move(name_p)) {
}

unique_ptr<CreateInfo> CreateTableInfo::Copy() const {
	auto result = make_uniq<CreateTableInfo>(catalog, schema, table);
	CopyProperties(*result);
	result->columns = columns.Copy();
	for (auto &constraint : constraints) {
		result->constraints.push_back(constraint->Copy());
	}
	if (query) {
		result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	}
	return std::move(result);
}

string CreateTableInfo::ToString() const {
	string ret = "";

	string table_name = KeywordHelper::WriteOptionallyQuoted(table);
	if (schema != DEFAULT_SCHEMA) {
		table_name = KeywordHelper::WriteOptionallyQuoted(schema) + "." + table_name;
	}

	ret += "CREATE TABLE " + table_name;
	if (query != nullptr) {
		ret += " AS " + query->ToString();
	} else {
		ret += " (" + ColumnsTo
		auto column_names = columns.GetColumnNames();
		auto column_types = columns.GetColumnTypes();
		D_ASSERT(column_names.size() == column_types.size());
		for (idx_t i = 0; i < column_names.size(); i++) {
			ret += column_names.at(i) + " " + LogicalTypeIdToString(column_types.at(i).id());
			if (i < column_names.size() - 1) {
				ret += ", ";
			}
		}
		ret += ")";
	}
	return ret;
}

} // namespace duckdb
