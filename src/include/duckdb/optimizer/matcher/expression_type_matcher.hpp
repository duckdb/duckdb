//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/matcher/expression_type_matcher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/vector.hpp"

#include <algorithm>

namespace duckdb {

//! The ExpressionTypeMatcher class contains a set of matchers that can be used to pattern match ExpressionTypes
class ExpressionTypeMatcher {
public:
	virtual ~ExpressionTypeMatcher() {
	}

	virtual bool Match(ExpressionType type) = 0;
};

//! The SpecificExpressionTypeMatcher class matches a single specified Expression type
class SpecificExpressionTypeMatcher : public ExpressionTypeMatcher {
public:
	explicit SpecificExpressionTypeMatcher(ExpressionType type) : type(type) {
	}

	bool Match(ExpressionType type) override {
		return type == this->type;
	}

private:
	ExpressionType type;
};

//! The ManyExpressionTypeMatcher class matches a set of ExpressionTypes
class ManyExpressionTypeMatcher : public ExpressionTypeMatcher {
public:
	explicit ManyExpressionTypeMatcher(vector<ExpressionType> types) : types(std::move(types)) {
	}

	bool Match(ExpressionType type) override {
		return std::find(types.begin(), types.end(), type) != types.end();
	}

private:
	vector<ExpressionType> types;
};

//! The ComparisonExpressionTypeMatcher class matches a comparison expression
class ComparisonExpressionTypeMatcher : public ExpressionTypeMatcher {
public:
	bool Match(ExpressionType type) override {
		return type == ExpressionType::COMPARE_EQUAL || type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
		       type == ExpressionType::COMPARE_LESSTHANOREQUALTO || type == ExpressionType::COMPARE_LESSTHAN ||
		       type == ExpressionType::COMPARE_GREATERTHAN;
	}
};
} // namespace duckdb
