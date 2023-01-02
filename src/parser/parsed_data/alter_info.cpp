#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/alter_function_info.hpp"

#include "duckdb/common/field_writer.hpp"

namespace duckdb {

AlterInfo::AlterInfo(AlterType type, string catalog_p, string schema_p, string name_p, bool if_exists)
    : type(type), if_exists(if_exists), catalog(std::move(catalog_p)), schema(std::move(schema_p)),
      name(std::move(name_p)), allow_internal(false) {
}

AlterInfo::~AlterInfo() {
}

void AlterInfo::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<AlterType>(type);
	Serialize(writer);
	writer.Finalize();
}

unique_ptr<AlterInfo> AlterInfo::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto type = reader.ReadRequired<AlterType>();

	unique_ptr<AlterInfo> result;
	switch (type) {
	case AlterType::ALTER_TABLE:
		result = AlterTableInfo::Deserialize(reader);
		break;
	case AlterType::ALTER_VIEW:
		result = AlterViewInfo::Deserialize(reader);
		break;
	case AlterType::ALTER_FUNCTION:
		result = AlterFunctionInfo::Deserialize(reader);
		break;
	default:
		throw SerializationException("Unknown alter type for deserialization!");
	}
	reader.Finalize();

	return result;
}

AlterEntryData AlterInfo::GetAlterEntryData() const {
	AlterEntryData data;
	data.catalog = catalog;
	data.schema = schema;
	data.name = name;
	data.if_exists = if_exists;
	return data;
}

} // namespace duckdb
