//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/matcher/function_matcher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"
#include <algorithm>

namespace duckdb {

//! The FunctionMatcher class contains a set of matchers that can be used to pattern match specific functions
class FunctionMatcher {
public:
	virtual ~FunctionMatcher() {
	}

	virtual bool Match(string &name) = 0;

	static bool Match(unique_ptr<FunctionMatcher> &matcher, string &name) {
		if (!matcher) {
			return true;
		}
		return matcher->Match(name);
	}
};

//! The SpecificFunctionMatcher class matches a single specified function name
class SpecificFunctionMatcher : public FunctionMatcher {
public:
	SpecificFunctionMatcher(string name) : name(move(name)) {
	}

	bool Match(string &name) override {
		return name == this->name;
	}

private:
	string name;
};

//! The ManyFunctionMatcher class matches a set of functions
class ManyFunctionMatcher : public FunctionMatcher {
public:
	ManyFunctionMatcher(unordered_set<string> names) : names(move(names)) {
	}

	bool Match(string &name) override {
		return names.find(name) != names.end();
	}

private:
	unordered_set<string> names;
};

} // namespace duckdb
