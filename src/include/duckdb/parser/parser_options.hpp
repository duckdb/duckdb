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
#include "duckdb/common/enums/identifier_case_mode.hpp"
#include "duckdb/common/enums/regex_match_operator_semantics.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class ClientContext;
class ExtensionCallbackManager;
class ParserExtension;
struct CompiledGrammar;

struct ParserOptions {
private:
	ParserOptions() {
	}
	friend class ClientContext;

public:
	//! Explicit configuration for parsing without a client context.
	DUCKDB_API static ParserOptions Builtin();

public:
	IdentifierCaseMode identifier_case_mode = IdentifierCaseMode::PRESERVE_CASE;
	bool integer_division = false;
	RegexMatchOperatorSemantics regex_match_operator_semantics = RegexMatchOperatorSemantics::PARTIAL;
	idx_t max_expression_depth = 1000;
	optional_ptr<const ExtensionCallbackManager> extensions;
	AllowParserOverride parser_override_setting = AllowParserOverride::DEFAULT_OVERRIDE;
	shared_ptr<CompiledGrammar> compiled_grammar;
	//! The client context the statement is parsed for (not set when parsing without a client context)
	optional_ptr<ClientContext> context;

	//! equality over every field, pointers by identity (used to detect stale cached parsers)
	bool operator==(const ParserOptions &other) const {
		return identifier_case_mode == other.identifier_case_mode && integer_division == other.integer_division &&
		       regex_match_operator_semantics == other.regex_match_operator_semantics &&
		       max_expression_depth == other.max_expression_depth && extensions == other.extensions &&
		       parser_override_setting == other.parser_override_setting && compiled_grammar == other.compiled_grammar &&
		       context == other.context;
	}
	bool operator!=(const ParserOptions &other) const {
		return !(*this == other);
	}
};

} // namespace duckdb
