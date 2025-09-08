#include "transformer/peg_transformer.hpp"
#include "matcher.hpp"
#include "chrono"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/tableref/showref.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformStatement(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	throw NotImplementedException("PEGTransformerFactory::TransformStatement");
}

unique_ptr<SQLStatement> PEGTransformerFactory::Transform(vector<MatcherToken> &tokens, const char *root_rule) {
	string token_stream;
	for (auto &token : tokens) {
		token_stream += token.text + " ";
	}

	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator parse_result_allocator;
	MatchState state(tokens, suggestions, parse_result_allocator);
	MatcherAllocator allocator;
	auto &matcher = Matcher::RootMatcher(allocator);
	// --- TIMING START ---
	auto start_time = std::chrono::high_resolution_clock::now();
	auto match_result = matcher.MatchParseResult(state);
	auto end_time = std::chrono::high_resolution_clock::now();
	// --- TIMING END ---
	auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

	if (match_result == nullptr || state.token_index < state.tokens.size()) {
		// TODO(dtenwolde) add error handling
		string token_list;
		for (idx_t i = 0; i < tokens.size(); i++) {
			if (!token_list.empty()) {
				token_list += "\n";
			}
			if (i < 10) {
				token_list += " ";
			}
			token_list += to_string(i) + ":" + tokens[i].text;
		}
		throw ParserException("Failed to parse query - did not consume all tokens (got to token %d - %s)\nTokens:\n%s",
		                      state.token_index, tokens[state.token_index].text, token_list);
	}

	Printer::Print(match_result->ToString());
	auto t_start_time = std::chrono::high_resolution_clock::now();
	match_result->name = "Statement";
	ArenaAllocator transformer_allocator(Allocator::DefaultAllocator());
	PEGTransformerState transformer_state(tokens);
	PEGTransformer transformer(transformer_allocator, transformer_state, sql_transform_functions, parser.rules,
	                           enum_mappings);
	auto transformed_result = transformer.Transform<unique_ptr<SQLStatement>>(match_result);
	auto t_end_time = std::chrono::high_resolution_clock::now();
	// --- TIMING END ---
	auto t_duration = std::chrono::duration_cast<std::chrono::microseconds>(t_end_time - t_start_time);
	Printer::PrintF("Parsing took: %lld µs\nTransforming took: %lld µs", duration.count(), t_duration.count());
	return transformed_result;
}

#define REGISTER_TRANSFORM(FUNCTION) Register(string(#FUNCTION).substr(9), &FUNCTION)

PEGTransformerFactory::PEGTransformerFactory() {
	// Registering transform functions using the macro for brevity
	REGISTER_TRANSFORM(TransformStatement);
}

optional_ptr<ParseResult> PEGTransformerFactory::ExtractResultFromParens(optional_ptr<ParseResult> parse_result) {
	// Parens(D) <- '(' D ')'
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.children[1];
}

vector<optional_ptr<ParseResult>>
PEGTransformerFactory::ExtractParseResultsFromList(optional_ptr<ParseResult> parse_result) {
	// List(D) <- D (',' D)* ','?
	vector<optional_ptr<ParseResult>> result;
	auto &list_pr = parse_result->Cast<ListParseResult>();
	result.push_back(list_pr.children[0]);
	auto opt_child = list_pr.Child<OptionalParseResult>(1);
	if (opt_child.HasResult()) {
		auto repeat_result = opt_child.optional_result->Cast<RepeatParseResult>();
		for (auto &child : repeat_result.children) {
			auto &list_child = child->Cast<ListParseResult>();
			result.push_back(list_child.children[1]);
		}
	}

	return result;
}

QualifiedName PEGTransformerFactory::StringToQualifiedName(vector<string> input) {
	QualifiedName result;
	if (input.empty()) {
		throw InternalException("QualifiedName cannot be made with an empty input.");
	}
	if (input.size() == 1) {
		result.catalog = INVALID_CATALOG;
		result.schema = INVALID_SCHEMA;
		result.name = input[0];
	} else if (input.size() == 2) {
		result.catalog = INVALID_CATALOG;
		result.schema = input[0];
		result.name = input[1];
	} else if (input.size() == 3) {
		result.catalog = input[0];
		result.schema = input[1];
		result.name = input[2];
	} else {
		throw ParserException("Too many dots found.");
	}
	return result;
}

} // namespace duckdb
