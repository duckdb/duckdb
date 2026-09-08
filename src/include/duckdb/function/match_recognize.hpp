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

//! Where that column sits among the ones the matcher is handed per row: first, so that the matcher
//! can rewrite it without looking it up
constexpr const idx_t MATCH_RECOGNIZE_MATCH_NUMBER_FIELD = 0;

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

	bool Equals(const BaseExpression &other_p) const override {
		if (!Expression::Equals(other_p)) {
			return false;
		}
		auto &other = other_p.Cast<BoundAlternationExpression>();
		return child_left->Equals(*other.child_left) && child_right->Equals(*other.child_right);
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

	bool Equals(const BaseExpression &other_p) const override {
		if (!Expression::Equals(other_p)) {
			return false;
		}
		return Expression::ListEquals(children, other_p.Cast<BoundConcatenationExpression>().children);
	}
};

class BoundQuantifierExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::PATTERN;

	BoundQuantifierExpression(unique_ptr<Expression> child_p, optional_idx min_count_p, optional_idx max_count_p,
	                          bool excluded_p = false, bool reluctant_p = false)
	    : Expression(ExpressionType::QUANTIFIER, ExpressionClass::PATTERN, LogicalType::BOOLEAN),
	      child(std::move(child_p)), min_count(min_count_p), max_count(max_count_p), excluded(excluded_p),
	      reluctant(reluctant_p) {
	}

	unique_ptr<Expression> child;

	optional_idx min_count;
	optional_idx max_count;
	//! {- ... -}: the rows this matches take part in the match but are left out of the output
	bool excluded;
	//! A trailing ?: prefer the fewest repetitions rather than the most
	bool reluctant;

	static string QuantifierToString(optional_idx min_count, optional_idx max_count) {
		return StringUtil::Format("{%s,%s}", min_count.IsValid() ? to_string(min_count.GetIndex()) : "",
		                          max_count.IsValid() ? to_string(max_count.GetIndex()) : "");
	}

	string ToString() const override {
		return child->ToString() + QuantifierToString(min_count, max_count);
	}

	unique_ptr<Expression> Copy() const override {
		auto child_copy = child->Copy();
		return make_uniq<BoundQuantifierExpression>(std::move(child_copy), min_count, max_count, excluded, reluctant);
	}

	bool Equals(const BaseExpression &other_p) const override {
		if (!Expression::Equals(other_p)) {
			return false;
		}
		auto &other = other_p.Cast<BoundQuantifierExpression>();
		return min_count == other.min_count && max_count == other.max_count && excluded == other.excluded &&
		       reluctant == other.reluctant && child->Equals(*other.child);
	}

	hash_t Hash() const override {
		// the child is hashed by the base, which walks it like any other expression tree. Combining is
		// an XOR, so the second count is offset before it is mixed in - {1,1} would cancel itself out.
		const auto min_value = min_count.IsValid() ? min_count.GetIndex() : DConstants::INVALID_INDEX;
		const auto max_value = max_count.IsValid() ? max_count.GetIndex() : DConstants::INVALID_INDEX;
		auto result = CombineHash(Expression::Hash(), duckdb::Hash(min_value));
		result = CombineHash(result, duckdb::Hash(max_value ^ 0x9E3779B97F4A7C15ULL));
		return CombineHash(result, duckdb::Hash(static_cast<uint8_t>((excluded ? 1 : 0) | (reluctant ? 2 : 0))));
	}
};

class BoundAnchorExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::PATTERN;

	explicit BoundAnchorExpression(bool at_end_p)
	    : Expression(ExpressionType::ANCHOR, ExpressionClass::PATTERN, LogicalType::BOOLEAN), at_end(at_end_p) {
	}

	//! ^ holds only at the first row of the partition, $ only past its last
	bool at_end;

	string ToString() const override {
		return at_end ? "$" : "^";
	}

	unique_ptr<Expression> Copy() const override {
		return make_uniq<BoundAnchorExpression>(at_end);
	}

	bool Equals(const BaseExpression &other_p) const override {
		return Expression::Equals(other_p) && at_end == other_p.Cast<BoundAnchorExpression>().at_end;
	}

	hash_t Hash() const override {
		return CombineHash(Expression::Hash(), duckdb::Hash(at_end));
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

		bool Equals(const Navigation &other) const {
			return last == other.last && symbol == other.symbol && field == other.field && offset == other.offset;
		}
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
		if (navigations.size() != other.navigations.size()) {
			return false;
		}
		for (idx_t i = 0; i < navigations.size(); i++) {
			if (!navigations[i].Equals(other.navigations[i])) {
				return false;
			}
		}
		return other.pattern->Equals(*pattern) && Expression::ListEquals(conditions, other.conditions) &&
		       other.symbols == symbols && other.depends_on_match_number == depends_on_match_number &&
		       other.row_scoped == row_scoped && other.after_match == after_match &&
		       other.after_match_variable == after_match_variable;
	}
};

} // namespace duckdb
