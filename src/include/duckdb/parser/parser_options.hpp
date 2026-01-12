//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parser_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/allow_parser_override.hpp"

namespace duckdb {
class ParserExtension;

struct ParserOptions {
	bool preserve_identifier_case = true;
	bool integer_division = false;
	idx_t max_expression_depth = 1000;
	const vector<ParserExtension> *extensions = nullptr;
	AllowParserOverride parser_override_setting = AllowParserOverride::DEFAULT_OVERRIDE;
};

} // namespace duckdb
