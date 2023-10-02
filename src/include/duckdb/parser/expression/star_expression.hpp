//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/star_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

//! Represents a * expression in the SELECT clause
class StarExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::STAR;

public:
	StarExpression(string relation_name = string());

	//! The relation name in case of tbl.*, or empty if this is a normal *
	string relation_name;
	//! List of columns to exclude from the STAR expression
	case_insensitive_set_t exclude_list;
	//! List of columns to replace with another expression
	case_insensitive_map_t<unique_ptr<ParsedExpression>> replace_list;
	//! The expression to select the columns (regular expression or list)
	unique_ptr<ParsedExpression> expr;
	//! Whether or not this is a COLUMNS expression
	bool columns = false;

public:
	string ToString() const override;

	static bool Equal(const StarExpression &a, const StarExpression &b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
