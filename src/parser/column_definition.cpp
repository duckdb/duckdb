#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

ColumnDefinition::ColumnDefinition(string name_p, LogicalType type_p) : name(move(name_p)), type(move(type_p)) {
}

ColumnDefinition::ColumnDefinition(string name_p, LogicalType type_p, ColumnExpression expression)
    : name(move(name_p)), type(move(type_p)) {
	switch (expression.type) {
	case ColumnExpressionType::DEFAULT: {
		default_value = move(expression.expression);
		break;
	}
	case ColumnExpressionType::GENERATED: {
		generated_expression = move(expression.expression);
		category = TableColumnType::GENERATED;
		break;
	}
	default: {
		throw InternalException("Type not implemented for ColumnExpressionType");
	}
	}
}

void ColumnDefinition::SetGeneratedExpression(unique_ptr<ParsedExpression> expression) {
	if (default_value) {
		throw InvalidInputException("DEFAULT constraint on GENERATED column \"%s\" is not allowed", name);
	}
	category = TableColumnType::GENERATED;
	generated_expression = move(expression);
}

ParsedExpression &ColumnDefinition::GeneratedExpression() {
	D_ASSERT(category == TableColumnType::GENERATED);
	return *generated_expression;
}

ColumnDefinition ColumnDefinition::Copy() const {
	ColumnDefinition copy(name, type);
	copy.oid = oid;
	copy.default_value = default_value ? default_value->Copy() : nullptr;
	copy.generated_expression = generated_expression ? generated_expression->Copy() : nullptr;
	copy.compression_type = compression_type;
	return copy;
}

void ColumnDefinition::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(name);
	writer.WriteSerializable(type);
	writer.WriteOptional(default_value);
	writer.WriteOptional(generated_expression);
	writer.Finalize();
}

ColumnDefinition ColumnDefinition::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto column_name = reader.ReadRequired<string>();
	auto column_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto default_value = reader.ReadOptional<ParsedExpression>(nullptr);
	auto generated_expression = reader.ReadOptional<ParsedExpression>(nullptr);
	reader.Finalize();

	auto category = TableColumnType::STANDARD;
	if (generated_expression) {
		category = TableColumnType::GENERATED;
	}

	switch (category) {
	case TableColumnType::STANDARD:
		return ColumnDefinition(column_name, column_type, ColumnExpression(move(default_value)));
	case TableColumnType::GENERATED:
		return ColumnDefinition(column_name, column_type,
		                        ColumnExpression(move(generated_expression), ColumnExpressionType::GENERATED));
	default:
		throw NotImplementedException("Type not implemented for TableColumnType");
	}
}

} // namespace duckdb
