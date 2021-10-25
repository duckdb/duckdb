#include "duckdb/parser/parsed_data/alter_sequence_info.hpp"
#include "duckdb/common/serializer.hpp"

namespace duckdb {
//===--------------------------------------------------------------------===//
// AlterSequenceInfo
//===--------------------------------------------------------------------===//

void AlterSequenceInfo::Serialize(Serializer &serializer) {
	AlterInfo::Serialize(serializer);
	serializer.Write<AlterSequenceType>(alter_sequence_type);
	serializer.WriteString(schema);
	serializer.WriteString(name);
}
unique_ptr<AlterInfo> AlterSequenceInfo::Deserialize(Deserializer &source) {
	auto type = source.Read<AlterSequenceType>();
	auto schema = source.Read<string>();
	auto name = source.Read<string>();
	switch (type) {
	case AlterSequenceType::CHANGE_OWNERSHIP:
		return ChangeOwnershipInfo::Deserialize(source, schema, name);
	default:
		throw SerializationException("Unknown alter sequence type for deserialization!");
	}
}
unique_ptr<AlterInfo> AlterSequenceInfo::Copy() const {
	return make_unique_base<AlterInfo, AlterSequenceInfo>(schema, name);
}

//===--------------------------------------------------------------------===//
// ChangeOwnershipInfo
//===--------------------------------------------------------------------===//

unique_ptr<AlterInfo> ChangeOwnershipInfo::Deserialize(Deserializer &source, string schema, string name) {
	auto table_schema = source.Read<string>();
	auto table_name = source.Read<string>();
	return make_unique<ChangeOwnershipInfo>(schema, name, table_schema, table_name);
}
void ChangeOwnershipInfo::Serialize(Serializer &serializer) {
	AlterInfo::Serialize(serializer);
	serializer.Write<AlterSequenceType>(alter_sequence_type);
	serializer.WriteString(schema);
	serializer.WriteString(name);
	serializer.WriteString(table_schema);
	serializer.WriteString(table_name);
}
unique_ptr<AlterInfo> ChangeOwnershipInfo::Copy() const {
	return make_unique_base<AlterInfo, ChangeOwnershipInfo>(schema, name, table_schema, table_name);
}
} // namespace duckdb
