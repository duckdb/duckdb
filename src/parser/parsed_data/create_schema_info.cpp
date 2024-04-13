#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

CreateSchemaInfo::CreateSchemaInfo() : CreateInfo(CatalogType::SCHEMA_ENTRY) {
}

unique_ptr<CreateInfo> CreateSchemaInfo::Copy() const {
	auto result = make_uniq<CreateSchemaInfo>();
	CopyProperties(*result);
	return std::move(result);
}

string CreateSchemaInfo::CreateQualifiedName() const {
	string result;
	auto has_catalog = !catalog.empty();
	if (has_catalog) {
		if (temporary && catalog == TEMP_CATALOG) {
			has_catalog = false;
		}
	}
	if (has_catalog) {
		result += KeywordHelper::WriteOptionallyQuoted(catalog) + ".";
	}
	result += KeywordHelper::WriteOptionallyQuoted(schema);
	return result;
}

string CreateSchemaInfo::ToString() const {
	string ret = "";
	string qualified = CreateQualifiedName();

	switch (on_conflict) {
	case OnCreateConflict::ALTER_ON_CONFLICT: {
		ret += "CREATE SCHEMA " + qualified + " ON CONFLICT INSERT OR REPLACE;";
		break;
	}
	case OnCreateConflict::IGNORE_ON_CONFLICT: {
		ret += "CREATE SCHEMA " + qualified + " IF NOT EXISTS;";
		break;
	}
	case OnCreateConflict::REPLACE_ON_CONFLICT: {
		ret += "CREATE OR REPLACE SCHEMA " + qualified + ";";
		break;
	}
	case OnCreateConflict::ERROR_ON_CONFLICT: {
		ret += "CREATE SCHEMA " + qualified + ";";
		break;
	}
	}
	return ret;
}

} // namespace duckdb
