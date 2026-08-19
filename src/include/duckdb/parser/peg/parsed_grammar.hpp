//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/parsed_grammar.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/parser/peg/peg_parser.hpp"
#include "duckdb/parser/peg/transformer/transform_result.hpp"

#include <functional>

namespace duckdb {

class ParseResult;
class PEGTransformer;
struct TransformFrameOps;

using grammar_transform_function_t = std::function<unique_ptr<TransformResultValue>(PEGTransformer &, ParseResult &)>;

struct RuleTransformData {
	grammar_transform_function_t transform;
	grammar_transform_function_t trampoline_transform;
	shared_ptr<const TransformFrameOps> trampoline_ops;
};

struct ParsedGrammarRule {
	ParsedGrammarRule(string name_p, PEGRule recipe_p) : name(std::move(name_p)), recipe(std::move(recipe_p)) {
	}

	string name;
	PEGRule recipe;
	optional<RuleTransformData> transform_data;
};

//! Mutable, owning representation of a PEG grammar before matcher compilation.
class ParsedGrammar {
public:
	ParsedGrammar() = default;
	DUCKDB_API ParsedGrammar(ParsedGrammar &&other) noexcept;
	DUCKDB_API ParsedGrammar &operator=(ParsedGrammar &&other) noexcept;
	ParsedGrammar(const ParsedGrammar &) = delete;
	ParsedGrammar &operator=(const ParsedGrammar &) = delete;

	DUCKDB_API static ParsedGrammar Parse(const string &grammar);
	DUCKDB_API static ParsedGrammar CreateDefault();

	DUCKDB_API bool HasRule(const string &rule_name) const;
	DUCKDB_API const ParsedGrammarRule &GetRule(const string &rule_name) const;
	DUCKDB_API void AddRule(const string &rule_definition, optional<RuleTransformData> transform_data = std::nullopt);
	DUCKDB_API void ReplaceRule(const string &rule_definition,
	                            optional<RuleTransformData> transform_data = std::nullopt);
	DUCKDB_API void RemoveRule(const string &rule_name);
	DUCKDB_API void SetTransform(const string &rule_name, RuleTransformData &&transform_data);
	void SetTrampolineOps(const string &rule_name, const TransformFrameOps &ops);

private:
	friend class MatcherFactory;
	friend struct ParserCache;
	friend class PEGTransformerFactory;

	void AddParsedRule(ParsedGrammarRule rule);
	void RegisterStrings(PEGRule &rule);
	ParsedGrammarRule &GetMutableRule(const string &rule_name);
	static ParsedGrammarRule ParseSingleRule(const string &rule_definition);

	StringHeap string_heap;
	case_insensitive_map_t<unique_ptr<ParsedGrammarRule>> rules;
};

//! Immutable semantic data referenced directly by matchers and parse results.
struct CompiledGrammarRule {
	CompiledGrammarRule(string name_p, RuleTransformData &&transform_data)
	    : name(std::move(name_p)), transform_data(std::move(transform_data)) {
	}

	string name;
	RuleTransformData transform_data;
};

} // namespace duckdb
