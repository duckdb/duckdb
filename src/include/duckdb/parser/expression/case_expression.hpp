//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/case_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct CaseCheck {
	unique_ptr<ParsedExpression> when_expr;
	unique_ptr<ParsedExpression> then_expr;

	void Serialize(Serializer &serializer) const;
	static CaseCheck Deserialize(Deserializer &deserializer);
};

//! The CaseExpression represents a CASE expression in the query
class CaseExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::CASE;

public:
	DUCKDB_API CaseExpression();

public:
	const vector<CaseCheck> &CaseChecks() const {
		return case_checks;
	}
	const unique_ptr<ParsedExpression> &CaseExpr() const {
		return case_expr;
	}
	const ParsedExpression &Else() const {
		return *else_expr;
	}
	vector<CaseCheck> &CaseChecksMutable() {
		return case_checks;
	}
	unique_ptr<ParsedExpression> &CaseExprMutable() {
		return case_expr;
	}
	unique_ptr<ParsedExpression> &ElseMutable() {
		return else_expr;
	}

public:
	string ToString() const override;

	bool Equals(const ParsedExpression &other) const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

public:
	template <class T, class BASE>
	static string ToString(const T &entry) {
		string case_str = "CASE ";
		if (entry.CaseExpr()) {
			case_str += "(" + entry.CaseExpr()->ToString() + ")";
		}
		for (auto &check : entry.CaseChecks()) {
			case_str += " WHEN (" + check.when_expr->ToString() + ")";
			case_str += " THEN (" + check.then_expr->ToString() + ")";
		}
		case_str += " ELSE " + entry.Else().ToString();
		case_str += " END";
		return case_str;
	}

private:
	unique_ptr<ParsedExpression> case_expr;
	vector<CaseCheck> case_checks;
	unique_ptr<ParsedExpression> else_expr;
};
} // namespace duckdb
