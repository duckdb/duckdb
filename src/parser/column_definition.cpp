#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

ColumnDefinition::ColumnDefinition(string name_p, LogicalType type_p) : name(move(name_p)), type(move(type_p)) {
}

ColumnDefinition::ColumnDefinition(string name, LogicalType type, unique_ptr<ParsedExpression> default_value,
                                   CompressionType compression_type)
    : name(move(name)), type(move(type)), default_value(move(default_value)), compression_type(compression_type) {
}

ColumnDefinition ColumnDefinition::Copy() const {
	ColumnDefinition copy(name, type);
	copy.oid = oid;
	copy.default_value = default_value ? default_value->Copy() : nullptr;
	copy.compression_type = compression_type;
	return copy;
}

void ColumnDefinition::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(name);
	writer.WriteSerializable(type);
	writer.WriteOptional(default_value);
	writer.WriteField(compression_type);
	writer.Finalize();
}

ColumnDefinition ColumnDefinition::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto column_name = reader.ReadRequired<string>();
	auto column_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto default_value = reader.ReadOptional<ParsedExpression>(nullptr);
	auto compression_type = reader.ReadField(CompressionType::COMPRESSION_AUTO);
	reader.Finalize();


	return ColumnDefinition(column_name, column_type, move(default_value), compression_type);
}

} // namespace duckdb
