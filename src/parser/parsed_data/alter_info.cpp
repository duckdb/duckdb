#include "duckdb/parser/parsed_data/alter_info.hpp"

#include "duckdb/parser/parsed_data/alter_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

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

bool AlterInfo::IsAddPrimaryKey() const {
	if (type != AlterType::ALTER_TABLE) {
		return false;
	}

	auto &table_info = Cast<AlterTableInfo>();
	if (table_info.alter_table_type != AlterTableType::ADD_CONSTRAINT) {
		return false;
	}

	auto &constraint_info = table_info.Cast<AddConstraintInfo>();
	if (constraint_info.constraint->type != ConstraintType::UNIQUE) {
		return false;
	}

	auto &unique_info = constraint_info.constraint->Cast<UniqueConstraint>();
	if (!unique_info.IsPrimaryKey()) {
		return false;
	}

	return true;
}

} // namespace duckdb
