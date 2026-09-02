//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/pattern_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

// TODO move stuff to implementation file

class PatternExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::PATTERN;
	PatternExpression(ExpressionType type) : ParsedExpression(type, ExpressionClass::PATTERN) {};
};

class ConcatenationExpression : public PatternExpression {
public:
	ConcatenationExpression(vector<unique_ptr<ParsedExpression>> children_p)
	    : PatternExpression(ExpressionType::CONCATENATION), children(std::move(children_p)) {
	}

	string ToString() const override {
		return "(" +
		       StringUtil::Join(children, children.size(), " ",
		                        [](const unique_ptr<ParsedExpression> &expr) { return expr->ToString(); }) +
		       ")";
	}

	unique_ptr<ParsedExpression> Copy() const override {
		vector<unique_ptr<ParsedExpression>> new_children;
		for (auto &child : children) {
			new_children.push_back(child->Copy());
		}
		return make_uniq<ConcatenationExpression>(std::move(new_children));
	}

	vector<unique_ptr<ParsedExpression>> children;
};

class QuantifiedExpression : public PatternExpression {
public:
	QuantifiedExpression(unique_ptr<ParsedExpression> child_p, optional_idx min_count_p, optional_idx max_count_p)
	    : PatternExpression(ExpressionType::QUANTIFIER), child(std::move(child_p)), min_count(min_count_p),
	      max_count(max_count_p) {
		if (min_count.IsValid() && max_count.IsValid() && min_count.GetIndex() > max_count.GetIndex()) {
			throw ParserException("Min count cannot be larger than max count");
		}
	}

	static string QuantifierString(optional_idx min_count, optional_idx max_count) {
		if (!min_count.IsValid() && !max_count.IsValid()) {
			return "";
		}
		if (min_count.IsValid() && min_count.GetIndex() == 0 && !max_count.IsValid()) {
			return "*";
		}
		if (min_count.IsValid() && min_count.GetIndex() == 1 && !max_count.IsValid()) {
			return "+";
		}
		return StringUtil::Format("{%s,%s}", min_count.IsValid() ? to_string(min_count.GetIndex()) : "",
		                          max_count.IsValid() ? to_string(max_count.GetIndex()) : "");
	}

	string ToString() const override {
		return StringUtil::Format("%s%s", child->ToString(), QuantifierString(min_count, max_count));
	}

	unique_ptr<ParsedExpression> Copy() const override {
		return make_uniq<QuantifiedExpression>(child->Copy(), min_count, max_count);
	}
	unique_ptr<ParsedExpression> child;

	optional_idx min_count;
	optional_idx max_count;
	//! {- ... -}: the rows this matches take part in the match but are left out of the output
	bool excluded = false;
};

class AlternationExpression : public PatternExpression {
public:
	AlternationExpression(unique_ptr<ParsedExpression> child_left_p, unique_ptr<ParsedExpression> child_right_p)
	    : PatternExpression(ExpressionType::ALTERNATION), child_left(std::move(child_left_p)),
	      child_right(std::move(child_right_p)) {
	}

	string ToString() const override {
		return StringUtil::Format("(%s)|(%s)", child_left->ToString(), child_right->ToString());
	}

	unique_ptr<ParsedExpression> Copy() const override {
		return make_uniq<AlternationExpression>(child_left->Copy(), child_right->Copy());
	}
	// TODO should this be a child list too?
	unique_ptr<ParsedExpression> child_left;
	unique_ptr<ParsedExpression> child_right;
};

} // namespace duckdb
