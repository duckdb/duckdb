#include "duckdb/parser/parsed_data/create_info.hpp"

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

void CreateInfo::CopyProperties(CreateInfo &other) const {
	other.type = type;
	other.catalog = catalog;
	other.schema = schema;
	other.on_conflict = on_conflict;
	other.temporary = temporary;
	other.internal = internal;
	other.sql = sql;
	other.comment = comment;
}

string CreateInfo::QualifierToString(const string &name) const {
	string result;
	auto has_catalog = !catalog.empty();
	if (has_catalog) {
		if (temporary && catalog == TEMP_CATALOG) {
			has_catalog = false;
		}
		if (catalog == "memory") {
			has_catalog = false;
		}
	}
	if (has_catalog) {
		result += KeywordHelper::WriteOptionallyQuoted(catalog) + ".";
		if (!schema.empty()) {
			result += KeywordHelper::WriteOptionallyQuoted(schema) + ".";
		}
	} else if (schema != DEFAULT_SCHEMA && !schema.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(schema) + ".";
	}
	result += KeywordHelper::WriteOptionallyQuoted(name);
	return result;
}

unique_ptr<AlterInfo> CreateInfo::GetAlterInfo() const {
	throw NotImplementedException("GetAlterInfo not implemented for this type");
}

} // namespace duckdb
