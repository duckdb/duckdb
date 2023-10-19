#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/alter_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_function_info.hpp"

namespace duckdb {

AlterInfo::AlterInfo(AlterType type, string catalog_p, string schema_p, string name_p, OnEntryNotFound if_not_found)
    : ParseInfo(TYPE), type(type), if_not_found(if_not_found), catalog(std::move(catalog_p)),
      schema(std::move(schema_p)), name(std::move(name_p)), allow_internal(false) {
}

AlterInfo::AlterInfo(AlterType type) : ParseInfo(TYPE), type(type) {
}

AlterInfo::~AlterInfo() {
}

AlterEntryData AlterInfo::GetAlterEntryData() const {
	AlterEntryData data;
	data.catalog = catalog;
	data.schema = schema;
	data.name = name;
	data.if_not_found = if_not_found;
	return data;
}

} // namespace duckdb
