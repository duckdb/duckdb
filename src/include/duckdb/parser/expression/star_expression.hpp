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
#include "duckdb/parser/qualified_name_set.hpp"

namespace duckdb {

//! Represents a * expression in the SELECT clause
class StarExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::STAR;

public:
	explicit StarExpression(string relation_name = string());

	//! The relation name in case of tbl.*, or empty if this is a normal *
	string relation_name;
	//! List of columns to exclude from the STAR expression
	qualified_column_set_t exclude_list;
	//! List of columns to replace with another expression
	case_insensitive_map_t<unique_ptr<ParsedExpression>> replace_list;
	//! List of columns to rename
	qualified_column_map_t<string> rename_list;
	//! The expression to select the columns (regular expression or list)
	unique_ptr<ParsedExpression> expr;
	//! Whether or not this is a COLUMNS expression
	bool columns = false;
	//! Whether the columns are unpacked
	bool unpacked = false;

public:
	string ToString() const override;

	static bool Equal(const StarExpression &a, const StarExpression &b);
	static bool IsStar(const ParsedExpression &a);
	static bool IsColumns(const ParsedExpression &a);
	static bool IsColumnsUnpacked(const ParsedExpression &a);

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

public:
	// these methods exist for backwards compatibility of (de)serialization
	StarExpression(const case_insensitive_set_t &exclude_list, qualified_column_set_t qualified_set);

	case_insensitive_set_t SerializedExcludeList() const;
	qualified_column_set_t SerializedQualifiedExcludeList() const;
};
} // namespace duckdb
