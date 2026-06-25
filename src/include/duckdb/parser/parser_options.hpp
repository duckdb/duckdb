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
#include "duckdb/common/enums/regex_match_operator_semantics.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class ExtensionCallbackManager;
class ParserExtension;
struct ParserCache;

struct ParserOptions {
	bool preserve_identifier_case = true;
	bool integer_division = false;
	RegexMatchOperatorSemantics regex_match_operator_semantics = RegexMatchOperatorSemantics::PARTIAL;
	idx_t max_expression_depth = 1000;
	optional_ptr<const ExtensionCallbackManager> extensions;
	AllowParserOverride parser_override_setting = AllowParserOverride::DEFAULT_OVERRIDE;
	optional_ptr<ParserCache> parser_cache;
};

} // namespace duckdb
