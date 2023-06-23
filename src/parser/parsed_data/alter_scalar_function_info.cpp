#include "duckdb/parser/parsed_data/alter_scalar_function_info.hpp"

#include "duckdb/common/field_writer.hpp"
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

void AlterScalarFunctionInfo::Serialize(FieldWriter &writer) const {
	writer.WriteField<AlterScalarFunctionType>(alter_scalar_function_type);
	writer.WriteString(catalog);
	writer.WriteString(schema);
	writer.WriteString(name);
	writer.WriteField(if_not_found);
}

unique_ptr<AlterInfo> AlterScalarFunctionInfo::Deserialize(FieldReader &reader) {
	//	auto type = reader.ReadRequired<AlterScalarFunctionType>();
	//	auto schema = reader.ReadRequired<string>();
	//	auto table = reader.ReadRequired<string>();
	//	auto if_exists = reader.ReadRequired<bool>();

	throw NotImplementedException("AlterScalarFunctionInfo cannot be deserialized");
}

//===--------------------------------------------------------------------===//
// AddScalarFunctionOverloadInfo
//===--------------------------------------------------------------------===//
AddScalarFunctionOverloadInfo::AddScalarFunctionOverloadInfo(AlterEntryData data, ScalarFunctionSet new_overloads_p)
    : AlterScalarFunctionInfo(AlterScalarFunctionType::ADD_FUNCTION_OVERLOADS, std::move(data)),
      new_overloads(std::move(new_overloads_p)) {
	this->allow_internal = true;
}

AddScalarFunctionOverloadInfo::~AddScalarFunctionOverloadInfo() {
}

unique_ptr<AlterInfo> AddScalarFunctionOverloadInfo::Copy() const {
	return make_uniq_base<AlterInfo, AddScalarFunctionOverloadInfo>(GetAlterEntryData(), new_overloads);
}

} // namespace duckdb
