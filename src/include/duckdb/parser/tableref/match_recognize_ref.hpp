//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/match_recognize_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

enum class MatchRecognizeRows : uint8_t {
	MATCH_RECOGNIZE_ROWS_DEFAULT = 1, /* no option specified */
	MATCH_RECOGNIZE_ROWS_ONE = 2,     /* ONE ROW PER MATCH */
	MATCH_RECOGNIZE_ROWS_ALL = 3      /* ALL ROWS PER MATCH */
};

enum class MatchRecognizeAfterMatch : uint8_t {
	MATCH_RECOGNIZE_AFTER_MATCH_DEFAULT = 1,   /* no option specified */
	MATCH_RECOGNIZE_AFTER_MATCH_NEXT_ROW = 2,  /* AFTER MATCH SKIP TO NEXT ROW */
	MATCH_RECOGNIZE_AFTER_MATCH_LAST_ROW = 3,  /* AFTER MATCH SKIP PAST LAST ROW */
	MATCH_RECOGNIZE_AFTER_MATCH_FIRST_VAR = 4, /* AFTER MATCH SKIP TO FIRST var */
	MATCH_RECOGNIZE_AFTER_MATCH_LAST_VAR = 5   /* AFTER MATCH SKIP TO LAST var */
};

//! A PATTERN quantifier - an unset bound means "unbounded" in that direction
struct MatchRecognizeQuantifier {
	optional_idx min_count;
	optional_idx max_count;
};

//! An AFTER MATCH SKIP clause, with the target variable for the TO FIRST/LAST forms
struct MatchRecognizeAfterMatchClause {
	MatchRecognizeAfterMatch after_match = MatchRecognizeAfterMatch::MATCH_RECOGNIZE_AFTER_MATCH_DEFAULT;
	string variable;
};

//! A SUBSET names a union of pattern variables
struct MatchRecognizeSubset {
	string name;
	vector<string> members;
};

//! One clause of a MATCH_RECOGNIZE body. The clauses may be written in any order, so the parser
//! collects them and the transformer sorts out which is which.
//! Which clause of a MATCH_RECOGNIZE body this is. Named rather than nested, because the enum
//! utilities generate from the name alone and a nested one reads as a type of its own.
enum class MatchRecognizeClauseKind : uint8_t { PARTITION, ORDER_BY, MEASURES, ROWS, SKIP, PATTERN, SUBSET, DEFINE };

struct MatchRecognizeClause {
	MatchRecognizeClauseKind kind = MatchRecognizeClauseKind::PATTERN;
	//! PARTITION, MEASURES and DEFINE
	vector<unique_ptr<ParsedExpression>> expressions;
	vector<OrderByNode> order_by;
	vector<MatchRecognizeSubset> subsets;
	MatchRecognizeRows rows = MatchRecognizeRows::MATCH_RECOGNIZE_ROWS_DEFAULT;
	MatchRecognizeAfterMatchClause skip;
	unique_ptr<ParsedExpression> pattern;
	//! DEFINE AUTO: take each variable's condition from the column of the same name
	bool define_auto = false;
};

struct MatchRecognizeConfig {
	vector<unique_ptr<ParsedExpression>> partition_expressions;
	vector<OrderByNode> order_by_expressions;
	vector<unique_ptr<ParsedExpression>> measures_expression_list;
	vector<unique_ptr<ParsedExpression>> defines_expression_list;
	MatchRecognizeRows rows_per_match;
	MatchRecognizeAfterMatch after_match;
	unique_ptr<ConstantExpression> after_match_variable;
	unique_ptr<ParsedExpression> pattern;
	vector<MatchRecognizeSubset> subsets;
	//! DEFINE AUTO: take each variable's condition from the column of the same name
	bool define_auto = false;
};

//! Represents a SHOW/DESCRIBE/SUMMARIZE statement
class MatchRecognizeRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::MATCH_RECOGNIZE;

	unique_ptr<TableRef> input;
	unique_ptr<MatchRecognizeConfig> config;

private:
	MatchRecognizeRef();

public:
	MatchRecognizeRef(unique_ptr<TableRef> input_p, unique_ptr<MatchRecognizeConfig> config_p)
	    : TableRef(TableReferenceType::MATCH_RECOGNIZE), input(std::move(input_p)), config(std::move(config_p)) {
	}

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a MatchRecognizeRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};

} // namespace duckdb
