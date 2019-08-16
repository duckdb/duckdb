//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/matcher/operator_type_matcher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/enums/operator_type.hpp"

#include <algorithm>

namespace duckdb {

//! The OperatorTypeMatcher class contains a set of matchers that can be used to pattern match OperatorTypes
class OperatorTypeMatcher {
public:
	virtual ~OperatorTypeMatcher() {
	}

	virtual bool Match(OperatorType type) = 0;

	static bool Match(unique_ptr<OperatorTypeMatcher>& matcher, OperatorType type) {
		if (!matcher) {
			return true;
		}
		return matcher->Match(type);
	}
};

//! The SpecificExpressionTypeMatcher class matches a single specified Expression type
class SpecificOperatorTypeMatcher : public OperatorTypeMatcher {
public:
	SpecificOperatorTypeMatcher(OperatorType type) : type(type) {
	}

	bool Match(OperatorType type) override {
		return type == this->type;
	}

private:
	OperatorType type;
};

//! The ManyOperatorTypeMatcher class matches a set of OperatorTypes
class ManyOperatorTypeMatcher : public OperatorTypeMatcher {
public:
	ManyOperatorTypeMatcher(vector<OperatorType> types) : types(types) {
	}

	bool Match(OperatorType type) override {
		return std::find(types.begin(), types.end(), type) != types.end();
	}

private:
	vector<OperatorType> types;
};

} // namespace duckdb
