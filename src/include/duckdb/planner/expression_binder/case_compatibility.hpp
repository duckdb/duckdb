//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/case_compatibility.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/expression/case_expression.hpp"

namespace duckdb {
class Binder;
class Expression;

//! Binding-time facts about CASE operands, shared across query scopes.
class CaseCompatibilityContext : public enable_shared_from_this<CaseCompatibilityContext> {
public:
	void Prepare(ParsedExpression &expr);
	static bool CanRepeatOperand(const Expression &expr, const Binder &binder);
	static bool CanRepeatComparisonOperand(const Expression &expr, const Binder &binder);
	static void Record(CaseExpression &expr, bool safe);
};

//! A normalized lookup key with immutable facts for each CASE occurrence.
class CaseCompatibilityKey {
public:
	explicit CaseCompatibilityKey(const ParsedExpression &expr);
	const ParsedExpression &Expression() const {
		return *expression;
	}
	bool Matches(const CaseCompatibilityKey &other) const;
	void TransferTo(const ParsedExpression &expr) const;

private:
	struct CaseSite {
		bool simple;
		CaseBindingSafety safety;
	};
	static void Normalize(unique_ptr<ParsedExpression> &expr, vector<CaseSite> &sites,
	                      optional_ptr<vector<shared_ptr<CaseBindingInfo>>> bindings = nullptr);

private:
	unique_ptr<ParsedExpression> expression;
	vector<CaseSite> sites;
};
} // namespace duckdb
