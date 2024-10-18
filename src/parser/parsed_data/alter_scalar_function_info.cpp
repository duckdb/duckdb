#include "duckdb/parser/parsed_data/alter_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/constraint.hpp"

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

} // namespace duckdb
