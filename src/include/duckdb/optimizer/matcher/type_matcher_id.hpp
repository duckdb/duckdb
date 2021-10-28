//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/matcher/type_matcher_id.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/optimizer/matcher/type_matcher.hpp"
namespace duckdb {

//! The TypeMatcherId class contains a set of matchers that can be used to pattern match TypeIds for Rules
class TypeMatcherId : public TypeMatcher {
public:
	explicit TypeMatcherId(LogicalTypeId type_id_p) : type_id(type_id_p) {
	}

	bool Match(const LogicalType &type) override {
		return type.id() == this->type_id;
	}

private:
	LogicalTypeId type_id;
};

} // namespace duckdb
