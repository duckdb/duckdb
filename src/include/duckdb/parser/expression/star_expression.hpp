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
	StarExpression(string relation_name = string());

	//! The relation name in case of tbl.*, or empty if this is a normal *
	string relation_name;
	//! List of columns to exclude from the STAR expression
	case_insensitive_set_t exclude_list;
	//! List of columns to replace with another expression
	case_insensitive_map_t<unique_ptr<ParsedExpression>> replace_list;
	//! Regular expression to select columns (if any)
	string regex;
	//! Whether or not this is a COLUMNS expression
	bool columns = false;

public:
	string ToString() const override;

	static bool Equal(const StarExpression *a, const StarExpression *b);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);
};
} // namespace duckdb
