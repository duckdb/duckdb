#include "duckdb/parser/parsed_data/alter_function_info.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// AlterFunctionInfo
//===--------------------------------------------------------------------===//
AlterFunctionInfo::AlterFunctionInfo(AlterFunctionType type, string schema_p, string table_p, bool if_exists)
    : AlterInfo(AlterType::ALTER_FUNCTION, move(move(schema_p)), move(table_p), if_exists), alter_function_type(type) {
}
AlterFunctionInfo::~AlterFunctionInfo() {
}

CatalogType AlterFunctionInfo::GetCatalogType() const {
	return CatalogType::SCALAR_FUNCTION_ENTRY;
}

void AlterFunctionInfo::Serialize(FieldWriter &writer) const {
	writer.WriteField<AlterFunctionType>(alter_function_type);
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
AddFunctionOverloadInfo::AddFunctionOverloadInfo(string schema_p, string name_p, bool if_exists_p,
                                                 ScalarFunctionSet new_overloads_p)
    : AlterFunctionInfo(AlterFunctionType::ADD_FUNCTION_OVERLOADS, move(schema_p), move(name_p), if_exists_p),
      new_overloads(move(new_overloads_p)) {
}
AddFunctionOverloadInfo::~AddFunctionOverloadInfo() {
}

unique_ptr<AlterInfo> AddFunctionOverloadInfo::Copy() const {
	return make_unique_base<AlterInfo, AddFunctionOverloadInfo>(schema, name, if_exists, new_overloads);
}

} // namespace duckdb
