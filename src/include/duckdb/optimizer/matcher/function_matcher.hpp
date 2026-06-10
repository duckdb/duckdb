//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/matcher/function_matcher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/unordered_set.hpp"
#include <algorithm>

namespace duckdb {

//! The FunctionMatcher class contains a set of matchers that can be used to pattern match specific functions
class FunctionMatcher {
public:
	virtual ~FunctionMatcher() {
	}

	virtual bool Match(const Identifier &name) = 0;

	static bool Match(unique_ptr<FunctionMatcher> &matcher, const Identifier &name) {
		if (!matcher) {
			return true;
		}
		return matcher->Match(name);
	}
};

//! The SpecificFunctionMatcher class matches a single specified function name
class SpecificFunctionMatcher : public FunctionMatcher {
public:
	explicit SpecificFunctionMatcher(Identifier name_p) : name(std::move(name_p)) {
	}

	bool Match(const Identifier &matched_name) override {
		return matched_name == this->name;
	}

private:
	Identifier name;
};

//! The ManyFunctionMatcher class matches a set of functions
class ManyFunctionMatcher : public FunctionMatcher {
public:
	explicit ManyFunctionMatcher(identifier_set_t names_p) : names(std::move(names_p)) {
	}

	bool Match(const Identifier &name) override {
		return names.find(name) != names.end();
	}

private:
	identifier_set_t names;
};

} // namespace duckdb
