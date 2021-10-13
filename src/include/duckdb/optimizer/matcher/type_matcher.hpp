//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/matcher/type_matcher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

//! The TypeMatcher class contains a set of matchers that can be used to pattern match TypeIds for Rules
class TypeMatcher {
public:
	virtual ~TypeMatcher() {
	}

	virtual bool Match(LogicalTypeId type) = 0;
};

//! The SpecificTypeMatcher class matches only a single specified type
class SpecificTypeMatcher : public TypeMatcher {
public:
	explicit SpecificTypeMatcher(LogicalTypeId type) : type(type) {
	}

	bool Match(LogicalTypeId type) override {
		return type == this->type;
	}

private:
	LogicalTypeId type;
};

//! The NumericTypeMatcher class matches any numeric type (DECIMAL, INTEGER, etc...)
class NumericTypeMatcher : public TypeMatcher {
public:
	bool Match(LogicalTypeId type) override {
		return TypeIsNumeric(type);
	}
};

//! The IntegerTypeMatcher class matches only integer types (INTEGER, SMALLINT, TINYINT, BIGINT)
class IntegerTypeMatcher : public TypeMatcher {
public:
	bool Match(LogicalTypeId type) override {
		return TypeIsInteger(type);
	}
};

} // namespace duckdb
