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

	virtual bool Match(const string &name) = 0;

	static bool Match(unique_ptr<FunctionMatcher> &matcher, const string &name) {
		if (!matcher) {
			return true;
		}
		return matcher->Match(name);
	}
};

//! The SpecificFunctionMatcher class matches a single specified function name
class SpecificFunctionMatcher : public FunctionMatcher {
public:
	explicit SpecificFunctionMatcher(string name_p) : name(std::move(name_p)) {
	}

	bool Match(const string &matched_name) override {
		return matched_name == this->name;
	}

private:
	string name;
};

//! The ManyFunctionMatcher class matches a set of functions
class ManyFunctionMatcher : public FunctionMatcher {
public:
	explicit ManyFunctionMatcher(unordered_set<string> names_p) : names(std::move(names_p)) {
	}

	bool Match(const string &name) override {
		return names.find(name) != names.end();
	}

private:
	unordered_set<string> names;
};

} // namespace duckdb
