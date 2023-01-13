#include "duckdb/parser/parsed_data/alter_function_info.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// AlterFunctionInfo
//===--------------------------------------------------------------------===//
AlterFunctionInfo::AlterFunctionInfo(AlterFunctionType type, AlterEntryData data)
    : AlterInfo(AlterType::ALTER_FUNCTION, std::move(data.catalog), std::move(data.schema), std::move(data.name),
                data.if_exists),
      alter_function_type(type) {
}
AlterFunctionInfo::~AlterFunctionInfo() {
}

CatalogType AlterFunctionInfo::GetCatalogType() const {
	return CatalogType::SCALAR_FUNCTION_ENTRY;
}

void AlterFunctionInfo::Serialize(FieldWriter &writer) const {
	writer.WriteField<AlterFunctionType>(alter_function_type);
	writer.WriteString(catalog);
	writer.WriteString(schema);
	writer.WriteString(name);
	writer.WriteField(if_exists);
}

unique_ptr<AlterInfo> AlterFunctionInfo::Deserialize(FieldReader &reader) {
	//	auto type = reader.ReadRequired<AlterFunctionType>();
	//	auto schema = reader.ReadRequired<string>();
	//	auto table = reader.ReadRequired<string>();
	//	auto if_exists = reader.ReadRequired<bool>();

	throw NotImplementedException("AlterFunctionInfo cannot be deserialized");
}

//===--------------------------------------------------------------------===//
// AddFunctionOverloadInfo
//===--------------------------------------------------------------------===//
AddFunctionOverloadInfo::AddFunctionOverloadInfo(AlterEntryData data, ScalarFunctionSet new_overloads_p)
    : AlterFunctionInfo(AlterFunctionType::ADD_FUNCTION_OVERLOADS, std::move(data)),
      new_overloads(std::move(new_overloads_p)) {
	this->allow_internal = true;
}
AddFunctionOverloadInfo::~AddFunctionOverloadInfo() {
}

unique_ptr<AlterInfo> AddFunctionOverloadInfo::Copy() const {
	return make_unique_base<AlterInfo, AddFunctionOverloadInfo>(GetAlterEntryData(), new_overloads);
}

} // namespace duckdb
