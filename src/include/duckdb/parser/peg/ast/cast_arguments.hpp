#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

struct CastArguments {
	unique_ptr<ParsedExpression> expression;
	LogicalType type;
};

} // namespace duckdb
