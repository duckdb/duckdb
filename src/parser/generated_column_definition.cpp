#include "duckdb/parser/generated_column_definition.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

GeneratedColumnDefinition::GeneratedColumnDefinition(string name_p, LogicalType type_p,
                                                     unique_ptr<ParsedExpression> expression)
    : name(move(name_p)), type(move(type_p)), expression(move(expression)) {
}

GeneratedColumnDefinition GeneratedColumnDefinition::Copy() const {
	GeneratedColumnDefinition copy(name, type, expression->Copy());
	copy.oid = oid;
	copy.compression_type = compression_type;
	return copy;
}

void GeneratedColumnDefinition::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(name);
	writer.WriteSerializable(type);
	writer.WriteSerializable(*expression);
	writer.Finalize();
}

GeneratedColumnDefinition GeneratedColumnDefinition::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto column_name = reader.ReadRequired<string>();
	auto column_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto expression = ParsedExpression::Deserialize(source);
	reader.Finalize();

	return GeneratedColumnDefinition(column_name, column_type, move(expression));
}

} // namespace duckdb
