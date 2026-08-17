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
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {
class KeywordExtension;
class ParserExtension;
struct ParserCache;

struct ParserOptions {
	bool preserve_identifier_case = true;
	bool integer_division = false;
	bool debug_transformer_trampoline_style = false;
	RegexMatchOperatorSemantics regex_match_operator_semantics = RegexMatchOperatorSemantics::PARTIAL;
	idx_t max_expression_depth = 1000;
	shared_ptr<const vector<ParserExtension>> parser_extensions;
	shared_ptr<const KeywordExtension> keyword_extension;
	AllowParserOverride parser_override_setting = AllowParserOverride::DEFAULT_OVERRIDE;
	optional_ptr<ParserCache> parser_cache;
};

} // namespace duckdb
