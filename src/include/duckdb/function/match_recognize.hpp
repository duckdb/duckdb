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

//! What a node of a pattern is. The four of them are the pattern algebra: a variable to match, an
//! anchor that matches a place rather than a row, and the ways of putting parts together.
enum class MatchRecognizePatternType : uint8_t { SYMBOL, ANCHOR, ALTERNATION, CONCATENATION, QUANTIFIER };

//! The pattern the matcher walks. It is a tree of its own, owned by the bind data below and compiled
//! into the matcher's program: nothing about it is an SQL expression, and it is never evaluated as one.
struct MatchRecognizePattern {
	explicit MatchRecognizePattern(MatchRecognizePatternType type_p) : type(type_p) {
	}

	MatchRecognizePatternType type;
	//! SYMBOL: the pattern variable, as an index into the bind data's symbols
	idx_t symbol = 0;
	//! ANCHOR: whether it holds past the partition's last row rather than at its first
	bool at_end = false;
	//! QUANTIFIER: how many repetitions it allows, unset for unbounded in that direction
	optional_idx min_count;
	optional_idx max_count;
	//! QUANTIFIER: {- -}, whose rows take part in the match but are left out of the output
	bool excluded = false;
	//! QUANTIFIER: a trailing ?, preferring the fewest repetitions rather than the most
	bool reluctant = false;
	//! ALTERNATION has two, CONCATENATION as many as it concatenates, QUANTIFIER one, the rest none
	vector<unique_ptr<MatchRecognizePattern>> children;

	unique_ptr<MatchRecognizePattern> Copy() const {
		auto result = make_uniq<MatchRecognizePattern>(type);
		result->symbol = symbol;
		result->at_end = at_end;
		result->min_count = min_count;
		result->max_count = max_count;
		result->excluded = excluded;
		result->reluctant = reluctant;
		for (auto &child : children) {
			result->children.push_back(child->Copy());
		}
		return result;
	}

	bool Equals(const MatchRecognizePattern &other) const {
		if (type != other.type || symbol != other.symbol || at_end != other.at_end || min_count != other.min_count ||
		    max_count != other.max_count || excluded != other.excluded || reluctant != other.reluctant ||
		    children.size() != other.children.size()) {
			return false;
		}
		for (idx_t i = 0; i < children.size(); i++) {
			if (!children[i]->Equals(*other.children[i])) {
				return false;
			}
		}
		return true;
	}

	void Serialize(Serializer &serializer) const;
	static unique_ptr<MatchRecognizePattern> Deserialize(Deserializer &deserializer);
};

struct MatchRecognizeFunctionData : FunctionData {
	unique_ptr<MatchRecognizePattern> pattern;
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
