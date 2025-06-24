#include "duckdb/parser/parsed_data/alter_table_function_info.hpp"

#include "duckdb/parser/constraint.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// AlterTableFunctionInfo
//===--------------------------------------------------------------------===//
AlterTableFunctionInfo::AlterTableFunctionInfo(AlterTableFunctionType type, AlterEntryData data)
    : AlterInfo(AlterType::ALTER_TABLE_FUNCTION, std::move(data.catalog), std::move(data.schema), std::move(data.name),
                data.if_not_found),
      alter_table_function_type(type) {
}
AlterTableFunctionInfo::~AlterTableFunctionInfo() {
}

CatalogType AlterTableFunctionInfo::GetCatalogType() const {
	return CatalogType::TABLE_FUNCTION_ENTRY;
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
	return make_uniq_base<AlterInfo, AddTableFunctionOverloadInfo>(GetAlterEntryData(), new_overloads);
}

string AddTableFunctionOverloadInfo::ToString() const {
	throw NotImplementedException("NOT PARSABLE");
}

} // namespace duckdb
