#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/serializer.hpp"

namespace duckdb {

ColumnDefinition ColumnDefinition::Copy() const {
	ColumnDefinition copy(name, type);
	copy.oid = oid;
	copy.default_value = default_value ? default_value->Copy() : nullptr;
	copy.compression_type = compression_type;
	return copy;
}

void ColumnDefinition::Serialize(Serializer &serializer) const {
	serializer.WriteString(name);
	type.Serialize(serializer);
	serializer.WriteOptional(default_value);
}

ColumnDefinition ColumnDefinition::Deserialize(Deserializer &source) {
	auto column_name = source.Read<string>();
	auto column_type = LogicalType::Deserialize(source);
	auto default_value = source.ReadOptional<ParsedExpression>();
	return ColumnDefinition(column_name, column_type, move(default_value));
}

} // namespace duckdb
