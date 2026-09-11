//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/match_recognize_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/function/match_recognize.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"

namespace duckdb {

//! Names for the columns a MATCH_RECOGNIZE clause generates. The input is bound first, so a generated
//! name is one the input demonstrably does not have, and no two of them are the same.
struct GeneratedNames {
	explicit GeneratedNames(case_insensitive_set_t taken_p) : taken(std::move(taken_p)) {
	}

	string Reserve(const string &base) {
		auto name = base;
		for (idx_t suffix = 0; taken.count(name); suffix++) {
			name = base + "_" + to_string(suffix);
		}
		taken.insert(name);
		return name;
	}

	case_insensitive_set_t taken;
};

//! A FIRST()/LAST() call in a DEFINE condition, which the matcher resolves per row
struct MatchRecognizeNavigation {
	bool last;
	//! The pattern variable navigated, empty for the match as a whole
	string symbol;
	//! Where the navigated expression sits in the projection below the matcher
	idx_t column;
	idx_t offset;
};

//! What a DEFINE condition needs from the plan below the matcher: one column per value the matcher
//! cannot compute itself, plus the descriptors telling it what to do with them
struct MatchRecognizeConditionInputs {
	//! The projection the matcher reads from, built as the conditions are bound
	TableIndex projection_index;
	//! The table a matcher-supplied field is a column of, which no operator produces
	TableIndex match_number_index;
	vector<unique_ptr<Expression>> &select_list;
	vector<Identifier> &names;
	vector<LogicalType> &types;
	//! Names the output does not report, because they only exist for the matcher
	vector<string> &hidden;
	GeneratedNames &generated;
	vector<MatchRecognizeNavigation> &navigations;

	//! Compute this below the matcher and read it back as a column of its own
	unique_ptr<Expression> Project(unique_ptr<Expression> value, const string &base);
	unique_ptr<Expression> ProjectAs(unique_ptr<Expression> value, const string &name);
};

//! Where the value being bound is evaluated, which is what decides whether it may read the state the
//! matcher holds while it assembles a match.
enum class MatchRecognizeScope : uint8_t {
	//! The row the matcher is testing
	CANDIDATE_ROW,
	//! A window over the ordered partition
	WINDOW,
	//! The expression a navigation reads off a row of the match
	NAVIGATED,
	//! The partitioning the matcher walks
	PARTITION_BY,
	//! The ordering the matcher walks
	ORDER_BY
};

//! Binds a DEFINE condition. The matcher settles a condition one candidate row at a time, so
//! anything a condition reads that is not the row being tested - a navigation over the match, a
//! neighbour in the ordered partition - becomes a column of the projection below it.
class MatchRecognizeDefineBinder : public SelectBinder {
public:
	MatchRecognizeDefineBinder(Binder &binder, ClientContext &context, BoundSelectNode &node,
	                           MatchRecognizeConditionInputs &inputs, const WindowExpression &window_template,
	                           const case_insensitive_set_t &symbols, const unique_ptr<Expression> &match_number);

	//! Bind the condition of this variable, which is decided on the row the matcher is testing
	void BeginDefine(const string &name) {
		define_name = name;
		scope = MatchRecognizeScope::CANDIDATE_ROW;
	}
	//! Bind the partitioning or the ordering, which the matcher walks rather than produces
	void BeginFrame(MatchRecognizeScope frame) {
		define_name.clear();
		scope = frame;
	}

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) override;
	BindResult BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function, idx_t depth) override;
	string UnsupportedAggregateMessage() override;

private:
	//! PREV()/NEXT() walk the ordered partition rather than the match, so they are ordinary windows
	BindResult BindNeighbour(FunctionExpression &function, const string &function_name,
	                         unique_ptr<ParsedExpression> &expr_ptr, idx_t depth);
	//! FIRST()/LAST() read a row of the match being assembled, so the plan supplies the expression and
	//! the matcher reads it off the row it navigated to
	BindResult BindNavigation(FunctionExpression &function, const string &function_name, idx_t depth);
	BindResult BindNavigated(unique_ptr<ParsedExpression> inner, string symbol, bool last, idx_t offset, idx_t depth);
	//! Reject something that only means anything while the matcher is assembling a match
	void OutsideMatch(const string &what) const;

	MatchRecognizeConditionInputs &inputs;
	const WindowExpression &window_template;
	//! Every pattern variable the clause declares
	const case_insensitive_set_t &symbols;
	//! What the matcher's own match number reads as until the plan is built
	const unique_ptr<Expression> &match_number;
	//! The variable whose condition is being bound
	string define_name;
	MatchRecognizeScope scope = MatchRecognizeScope::CANDIDATE_ROW;
};

//! Binds the MEASURES clause: which rows of the match a value is read from, what a pattern variable in
//! front of a column means, and how much of the match RUNNING and FINAL let it see. Everything else is
//! ordinary binding, so an aggregate reaches the hook below only once the macros are expanded and the
//! overload chosen.
class MatchRecognizeMeasureBinder : public SelectBinder {
public:
	MatchRecognizeMeasureBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, string state,
	                            const MatchRecognizeConfig &config,
	                            const case_insensitive_map_t<vector<string>> &symbols, bool all_rows);

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) override;
	BindResult BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function, idx_t depth) override;

private:
	//! An aggregate in MEASURES aggregates the rows of the match, which is a window over them
	BindResult BindOverMatch(FunctionExpression &expr, idx_t depth);
	//! One field of the matcher's state, as it stands where the measures are projected
	unique_ptr<ParsedExpression> StateField(const string &field);
	//! Bind an expression this binder built, whose own parts are not the clause's to interpret again
	BindResult BindGenerated(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression);
	BindResult BindNavigation(FunctionExpression &function, const string &function_name,
	                          unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression);

	//! The column the matcher's state travels in
	string state;
	const MatchRecognizeConfig &config;
	//! Every pattern variable and SUBSET, mapped to the symbols it stands for
	const case_insensitive_map_t<vector<string>> &symbols;
	//! ONE ROW PER MATCH reports a finished match, so RUNNING and FINAL are the same thing there
	bool one_row;
	//! Whether the expression being bound sees the match up to the current row, or all of it
	bool running;
	//! Whether a value here is already read through something that scopes it to the match
	bool scoped = false;
};

//! Everything a reference names behind its first name is navigation into the column, so dropping a
//! qualifier drops that one name and keeps the rest: X.c.f is field f of column c, not column f.
unique_ptr<ParsedExpression> MatchRecognizeWithoutQualifier(const ColumnRefExpression &colref);

//! FIRST() and LAST() count from the end of the match they read from, by a constant the two clauses
//! spell the same way.
idx_t MatchRecognizeNavigationOffset(const string &function_name, const ParsedExpression &offset_expr);

//! One field of the matcher's state, read from the column it travels in
unique_ptr<ParsedExpression> MatchRecognizeStateField(const string &state, const string &field);

//! The names a lambda binds for its body, or false when the expression is only spelled like one: the
//! arrow form is also the JSON operator, and which it is only settles when the function is bound.
bool MatchRecognizeLambdaParameters(const ParsedExpression &expr, identifier_set_t &parameters);

} // namespace duckdb
