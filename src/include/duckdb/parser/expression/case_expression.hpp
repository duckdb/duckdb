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

struct ComparisonExpressionTail {
	ExpressionType comparison_type;
	unique_ptr<ParsedExpression> expression;

	void Serialize(Serializer &serializer) const;
	static ComparisonExpressionTail Deserialize(Deserializer &deserializer);
};

struct SimpleCaseCheck {
	vector<ComparisonExpressionTail> when;
	unique_ptr<ParsedExpression> then_expr;

	SimpleCaseCheck Copy() const;
	bool Equals(const SimpleCaseCheck &other) const;

	void Serialize(Serializer &serializer) const;
	static SimpleCaseCheck Deserialize(Deserializer &deserializer);
};

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
	//! Expand the simple operand into each comparison for pre-v2.1 storage.
	void ConvertToSearched();
	bool UseLegacySerialization() const {
		return case_operand != nullptr;
	}
	void LegacySerialize(Serializer &serializer) const;
	const unique_ptr<ParsedExpression> &CaseOperand() const {
		return case_operand;
	}
	unique_ptr<ParsedExpression> &CaseOperandMutable() {
		return case_operand;
	}
	const vector<SimpleCaseCheck> &SimpleChecks() const {
		return simple_checks;
	}
	vector<SimpleCaseCheck> &SimpleChecksMutable() {
		return simple_checks;
	}
	void SetSingleComparison() {
		single_comparison = true;
	}
	void VerifyShape() const;
	const ParsedExpression &Else() const {
		return *else_expr;
	}
	vector<CaseCheck> &CaseChecksMutable() {
		return case_checks;
	}
	unique_ptr<ParsedExpression> &ElseMutable() {
		return else_expr;
	}

public:
	string ToString() const override;

	bool Equals(const ParsedExpression &other) const override;
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

public:
	template <class T, class BASE>
	static string ToString(const T &entry) {
		string case_str = "CASE ";
		for (auto &check : entry.CaseChecks()) {
			case_str += " WHEN (" + check.when_expr->ToString() + ")";
			case_str += " THEN (" + check.then_expr->ToString() + ")";
		}
		case_str += " ELSE " + entry.Else().ToString();
		case_str += " END";
		return case_str;
	}

private:
	//! A single simple comparison shares the searched tree for grouping and old storage formats.
	bool single_comparison = false;
	unique_ptr<ParsedExpression> case_operand;
	vector<SimpleCaseCheck> simple_checks;
	vector<CaseCheck> case_checks;
	unique_ptr<ParsedExpression> else_expr;
};
} // namespace duckdb
