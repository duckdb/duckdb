//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/combine_types_rule.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {
class ClientContext;

class LogicalTypeResolver {
public:
	explicit LogicalTypeResolver(optional_ptr<ClientContext> context_p) : context(context_p) {
	}
	virtual ~LogicalTypeResolver() = default;

	virtual bool Operation(const LogicalType &left, const LogicalType &right, LogicalType &result) = 0;

public:
	optional_ptr<ClientContext> context;
};

typedef bool (*combine_types_rule_function_t)(LogicalTypeResolver &resolver, const LogicalType &left,
                                              const LogicalType &right, LogicalType &result);

struct CombineTypesRule {
	bool (*matches)(const LogicalType &left, const LogicalType &right); // order-insensitive in (left, right)
	combine_types_rule_function_t function;
};

//! The built-in combine rules, used to seed CastFunctionSet and as the fallback when no context is available
const vector<CombineTypesRule> &DefaultCombineTypesRules();

} // namespace duckdb
