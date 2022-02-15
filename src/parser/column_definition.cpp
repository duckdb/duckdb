#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

ColumnDefinition::ColumnDefinition(string name_p, LogicalType type_p) : name(move(name_p)), type(move(type_p)) {
}

ColumnDefinition::ColumnDefinition(string name_p, LogicalType type_p, unique_ptr<ParsedExpression> default_value)
    : name(move(name_p)), type(move(type_p)), default_value(move(default_value)) {
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
	writer.Finalize();
}

ColumnDefinition ColumnDefinition::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto column_name = reader.ReadRequired<string>();
	auto column_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto default_value = reader.ReadOptional<ParsedExpression>(nullptr);
	reader.Finalize();

	return ColumnDefinition(column_name, column_type, move(default_value));
}

} // namespace duckdb
