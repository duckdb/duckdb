//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/match_recognize.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/parser/tableref/match_recognize_ref.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! Pattern symbols share a namespace with the input columns, so they are qualified with this prefix in
//! the plan to keep a DEFINE from resolving to a base table column of the same name.
constexpr const char *MATCH_RECOGNIZE_DEFINE_PREFIX = "__mr_define_";

//! The column MATCH_NUMBER() reads in a DEFINE condition. The matcher overwrites it for every match
//! it attempts, which is what lets a DEFINE depend on the match being assembled.
constexpr const char *MATCH_RECOGNIZE_MATCH_NUMBER_COLUMN = "__mr_match_number";

//! RUNNING and FINAL are carried from the parser to the binder as these markers, which wrap the
//! measure they applied to and are unwrapped once the frame has been decided.
constexpr const char *MATCH_RECOGNIZE_RUNNING_MARKER = "__mr_running";
constexpr const char *MATCH_RECOGNIZE_FINAL_MARKER = "__mr_final";

//! The user facing pattern variable for a prefixed plan column
inline string MatchRecognizeSymbolName(const string &column_name) {
	const auto prefix_size = strlen(MATCH_RECOGNIZE_DEFINE_PREFIX);
	if (StringUtil::StartsWith(column_name, MATCH_RECOGNIZE_DEFINE_PREFIX)) {
		return column_name.substr(prefix_size);
	}
	return column_name;
}

class BoundAlternationExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::PATTERN;

	BoundAlternationExpression(unique_ptr<Expression> child_left_p, unique_ptr<Expression> child_right_p)
	    : Expression(ExpressionType::ALTERNATION, ExpressionClass::PATTERN, LogicalType::BOOLEAN),
	      child_left(std::move(child_left_p)), child_right(std::move(child_right_p)) {
	}

	unique_ptr<Expression> child_left;
	unique_ptr<Expression> child_right;

	string ToString() const override {
		return StringUtil::Format("(%s|%s)", child_left->ToString(), child_right->ToString());
	}

	unique_ptr<Expression> Copy() const override {
		auto child_left_copy = child_left->Copy();
		auto child_right_copy = child_right->Copy();
		return make_uniq<BoundAlternationExpression>(std::move(child_left_copy), std::move(child_right_copy));
	}
};

class BoundConcatenationExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::PATTERN;

	BoundConcatenationExpression(vector<unique_ptr<Expression>> children_p)
	    : Expression(ExpressionType::CONCATENATION, ExpressionClass::PATTERN, LogicalType::BOOLEAN),
	      children(std::move(children_p)) {
	}

	vector<unique_ptr<Expression>> children;

	string ToString() const override {
		return StringUtil::Join(children, children.size(), ", ",
		                        [](const unique_ptr<Expression> &expr) { return expr->ToString(); });
	}

	unique_ptr<Expression> Copy() const override {
		vector<unique_ptr<Expression>> children_copy;
		for (auto &child : children) {
			children_copy.push_back(child->Copy());
		}
		return make_uniq<BoundConcatenationExpression>(std::move(children_copy));
	}
};

class BoundQuantifierExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::PATTERN;

	BoundQuantifierExpression(unique_ptr<Expression> child_p, optional_idx min_count_p, optional_idx max_count_p,
	                          bool excluded_p = false)
	    : Expression(ExpressionType::QUANTIFIER, ExpressionClass::PATTERN, LogicalType::BOOLEAN),
	      child(std::move(child_p)), min_count(min_count_p), max_count(max_count_p), excluded(excluded_p) {
	}

	unique_ptr<Expression> child;

	optional_idx min_count;
	optional_idx max_count;
	//! {- ... -}: the rows this matches take part in the match but are left out of the output
	bool excluded;

	static string QuantifierToString(optional_idx min_count, optional_idx max_count) {
		return StringUtil::Format("{%s,%s}", min_count.IsValid() ? to_string(min_count.GetIndex()) : "",
		                          max_count.IsValid() ? to_string(max_count.GetIndex()) : "");
	}

	string ToString() const override {
		return child->ToString() + QuantifierToString(min_count, max_count);
	}

	unique_ptr<Expression> Copy() const override {
		auto child_copy = child->Copy();
		return make_uniq<BoundQuantifierExpression>(std::move(child_copy), min_count, max_count, excluded);
	}
};

struct MatchRecognizeFunctionData : FunctionData {
	unique_ptr<Expression> pattern;
	//! One condition per pattern symbol, evaluated by the matcher rather than precomputed. Column
	//! references are BoundReferenceExpressions into the window's argument list.
	vector<unique_ptr<Expression>> conditions;
	//! The symbol each condition defines, in the same order
	vector<string> symbols;
	//! Whether any condition reads MATCH_NUMBER(), which is what forces re-evaluation per match
	bool depends_on_match_number = false;
	//! FIRST()/LAST() calls, resolved against the match being assembled
	struct Navigation {
		bool last;
		string symbol;
		idx_t field;
		idx_t offset;
	};
	vector<Navigation> navigations;
	//! Conditions that read a navigation field, and so have to be evaluated row by row
	vector<bool> row_scoped;
	//! How to resume scanning after a match has been found
	MatchRecognizeAfterMatch after_match = MatchRecognizeAfterMatch::MATCH_RECOGNIZE_AFTER_MATCH_DEFAULT;
	//! The target pattern variable for the SKIP TO FIRST/LAST forms
	string after_match_variable;

	unique_ptr<FunctionData> Copy() const override {
		auto res = make_uniq<MatchRecognizeFunctionData>();

		res->pattern = pattern->Copy();
		for (auto &condition : conditions) {
			res->conditions.push_back(condition->Copy());
		}
		res->symbols = symbols;
		res->depends_on_match_number = depends_on_match_number;
		res->navigations = navigations;
		res->row_scoped = row_scoped;
		res->after_match = after_match;
		res->after_match_variable = after_match_variable;
		return res;
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<MatchRecognizeFunctionData>();
		return other.pattern->Equals(*pattern) && other.symbols == symbols && other.after_match == after_match &&
		       other.after_match_variable == after_match_variable;
	}
};

} // namespace duckdb
