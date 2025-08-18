//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/measure_definition_expression.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

class MeasureDefinitionExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::MEASURE_DEFINITION;

public:
	DUCKDB_API MeasureDefinitionExpression(unique_ptr<ParsedExpression> measure_expression = nullptr)
	    : ParsedExpression(ExpressionType::VALUE_MEASURE_DEFINITION, TYPE),
	      measure_expression(std::move(measure_expression)) {
	}

	//! The underlying expression defining the measure
	unique_ptr<ParsedExpression> measure_expression;

public:
	string ToString() const override {
		if (measure_expression) {
			return "MEASURE " + measure_expression->alias + ": " + measure_expression->ToString();
		}
		return "MEASURE: UNKNOWN";
	}

	unique_ptr<ParsedExpression> Copy() const override {
		auto copy = make_uniq<MeasureDefinitionExpression>();
		if (measure_expression) {
			copy->measure_expression = measure_expression->Copy();
		}
		copy->CopyProperties(*this);
		return std::move(copy);
	}

	// Serialization
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
