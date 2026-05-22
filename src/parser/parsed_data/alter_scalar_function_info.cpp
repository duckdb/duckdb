#include "duckdb/parser/parsed_data/alter_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// AlterScalarFunctionInfo
//===--------------------------------------------------------------------===//
AlterScalarFunctionInfo::AlterScalarFunctionInfo(AlterScalarFunctionType type, AlterEntryData data)
    : AlterInfo(AlterType::ALTER_SCALAR_FUNCTION, std::move(data.catalog), std::move(data.schema), std::move(data.name),
                data.if_not_found),
      alter_scalar_function_type(type) {
}
AlterScalarFunctionInfo::~AlterScalarFunctionInfo() {
}

CatalogType AlterScalarFunctionInfo::GetCatalogType() const {
	return CatalogType::SCALAR_FUNCTION_ENTRY;
}

//===--------------------------------------------------------------------===//
// AddScalarFunctionOverloadInfo
//===--------------------------------------------------------------------===//
AddScalarFunctionOverloadInfo::AddScalarFunctionOverloadInfo(AlterEntryData data,
                                                             unique_ptr<CreateScalarFunctionInfo> new_overloads_p)
    : AlterScalarFunctionInfo(AlterScalarFunctionType::ADD_FUNCTION_OVERLOADS, std::move(data)),
      new_overloads(std::move(new_overloads_p)) {
	this->allow_internal = true;
}

AddScalarFunctionOverloadInfo::~AddScalarFunctionOverloadInfo() {
}

unique_ptr<AlterInfo> AddScalarFunctionOverloadInfo::Copy() const {
	return make_uniq_base<AlterInfo, AddScalarFunctionOverloadInfo>(
	    GetAlterEntryData(), unique_ptr_cast<CreateInfo, CreateScalarFunctionInfo>(new_overloads->Copy()));
}

string AddScalarFunctionOverloadInfo::ToString() const {
	throw NotImplementedException("NOT PARSABLE CURRENTLY");
}

//===--------------------------------------------------------------------===//
// RenameScalarFunctionInfo
//===--------------------------------------------------------------------===//
RenameScalarFunctionInfo::RenameScalarFunctionInfo()
    : AlterScalarFunctionInfo(AlterScalarFunctionType::RENAME_SCALAR_FUNCTION, AlterEntryData()) {
}

RenameScalarFunctionInfo::RenameScalarFunctionInfo(AlterEntryData data, string new_name_p)
    : AlterScalarFunctionInfo(AlterScalarFunctionType::RENAME_SCALAR_FUNCTION, std::move(data)),
      new_name(std::move(new_name_p)) {
}

RenameScalarFunctionInfo::~RenameScalarFunctionInfo() {
}

unique_ptr<AlterInfo> RenameScalarFunctionInfo::Copy() const {
	return make_uniq_base<AlterInfo, RenameScalarFunctionInfo>(GetAlterEntryData(), new_name);
}

string RenameScalarFunctionInfo::ToString() const {
	string result = "ALTER FUNCTION ";
	if (!schema.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(schema) + ".";
	}
	result += KeywordHelper::WriteOptionallyQuoted(name);
	result += " RENAME TO ";
	result += KeywordHelper::WriteOptionallyQuoted(new_name);
	result += ";";
	return result;
}

} // namespace duckdb
