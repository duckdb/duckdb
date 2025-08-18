//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/measure_definition_expression.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/parser/expression/measure_definition_expression.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

void MeasureDefinitionExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(200, "measure_expression", measure_expression);
}

unique_ptr<ParsedExpression> MeasureDefinitionExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<MeasureDefinitionExpression>(new MeasureDefinitionExpression());
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(200, "measure_expression",
	                                                                   result->measure_expression);
	return std::move(result);
}

} // namespace duckdb
