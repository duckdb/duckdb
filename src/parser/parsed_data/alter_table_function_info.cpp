#include "duckdb/parser/parsed_data/alter_table_function_info.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// AlterTableFunctionInfo
//===--------------------------------------------------------------------===//
AlterTableFunctionInfo::AlterTableFunctionInfo(AlterTableFunctionType type, AlterEntryData data)
    : AlterInfo(AlterType::ALTER_TABLE_FUNCTION, std::move(data.catalog), std::move(data.schema), std::move(data.name),
                data.if_exists),
      alter_table_function_type(type) {
}
AlterTableFunctionInfo::~AlterTableFunctionInfo() {
}

CatalogType AlterTableFunctionInfo::GetCatalogType() const {
	return CatalogType::TABLE_FUNCTION_ENTRY;
}

void AlterTableFunctionInfo::Serialize(FieldWriter &writer) const {
	writer.WriteField<AlterTableFunctionType>(alter_table_function_type);
	writer.WriteString(catalog);
	writer.WriteString(schema);
	writer.WriteString(name);
	writer.WriteField(if_exists);
}

unique_ptr<AlterInfo> AlterTableFunctionInfo::Deserialize(FieldReader &reader) {
	throw NotImplementedException("AlterTableFunctionInfo cannot be deserialized");
}

//===--------------------------------------------------------------------===//
// AddTableFunctionOverloadInfo
//===--------------------------------------------------------------------===//
AddTableFunctionOverloadInfo::AddTableFunctionOverloadInfo(AlterEntryData data, TableFunctionSet new_overloads_p)
    : AlterTableFunctionInfo(AlterTableFunctionType::ADD_FUNCTION_OVERLOADS, std::move(data)),
      new_overloads(std::move(new_overloads_p)) {
	this->allow_internal = true;
}

AddTableFunctionOverloadInfo::~AddTableFunctionOverloadInfo() {
}

unique_ptr<AlterInfo> AddTableFunctionOverloadInfo::Copy() const {
	return make_unique_base<AlterInfo, AddTableFunctionOverloadInfo>(GetAlterEntryData(), new_overloads);
}

} // namespace duckdb
