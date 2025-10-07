#include "transformer/peg_transformer.hpp"
#include "matcher.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref/showref.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformStatement(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return transformer.Transform<unique_ptr<SQLStatement>>(choice_pr.result);
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
	auto match_result = matcher.MatchParseResult(state);
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

	match_result->name = root_rule;
	ArenaAllocator transformer_allocator(Allocator::DefaultAllocator());
	PEGTransformerState transformer_state(tokens);
	auto &factory = GetInstance();
	PEGTransformer transformer(transformer_allocator, transformer_state, factory.sql_transform_functions,
	                           factory.parser.rules, factory.enum_mappings);
	auto result = transformer.Transform<unique_ptr<SQLStatement>>(match_result);
	return transformer.Transform<unique_ptr<SQLStatement>>(match_result);
}

#define REGISTER_TRANSFORM(FUNCTION) Register(string(#FUNCTION).substr(9), &FUNCTION)

PEGTransformerFactory &PEGTransformerFactory::GetInstance() {
	static PEGTransformerFactory instance;
	return instance;
}

PEGTransformerFactory::PEGTransformerFactory() {
	REGISTER_TRANSFORM(TransformStatement);

	// expression.gram
	// REGISTER_TRANSFORM(TransformAnyAllOperator);
	// REGISTER_TRANSFORM(TransformAnyOrAll);
	// REGISTER_TRANSFORM(TransformAtTimeZoneOperator);
	REGISTER_TRANSFORM(TransformBaseExpression);
	// REGISTER_TRANSFORM(TransformBaseWindowName);
	// REGISTER_TRANSFORM(TransformBetweenOperator);
	// REGISTER_TRANSFORM(TransformBoundedListExpression);
	// REGISTER_TRANSFORM(TransformCaseElse);
	// REGISTER_TRANSFORM(TransformCaseExpression);
	// REGISTER_TRANSFORM(TransformCaseWhenThen);
	// REGISTER_TRANSFORM(TransformCastExpression);
	// REGISTER_TRANSFORM(TransformCastOperator);
	// REGISTER_TRANSFORM(TransformCastOrTryCast);
	// REGISTER_TRANSFORM(TransformCatalogReservedSchemaFunctionName);
	// REGISTER_TRANSFORM(TransformCatalogReservedSchemaTableColumnName);
	// REGISTER_TRANSFORM(TransformCoalesceExpression);
	// REGISTER_TRANSFORM(TransformColLabelParameter);
	// REGISTER_TRANSFORM(TransformCollateOperator);
	// REGISTER_TRANSFORM(TransformColumnReference);
	// REGISTER_TRANSFORM(TransformColumnsExpression);
	// REGISTER_TRANSFORM(TransformComparisonOperator);
	// REGISTER_TRANSFORM(TransformConjunctionOperator);
	// REGISTER_TRANSFORM(TransformDefaultExpression);
	// REGISTER_TRANSFORM(TransformDistinctFrom);
	// REGISTER_TRANSFORM(TransformDistinctOrAll);
	// REGISTER_TRANSFORM(TransformDotOperator);
	// REGISTER_TRANSFORM(TransformEscapeOperator);
	// REGISTER_TRANSFORM(TransformExcludeList);
	// REGISTER_TRANSFORM(TransformExcludeName);
	// REGISTER_TRANSFORM(TransformExportClause);
	REGISTER_TRANSFORM(TransformExpression);
	// REGISTER_TRANSFORM(TransformExtractExpression);
	// REGISTER_TRANSFORM(TransformFilterClause);
	// REGISTER_TRANSFORM(TransformFrameBound);
	// REGISTER_TRANSFORM(TransformFrameClause);
	// REGISTER_TRANSFORM(TransformFrameExtent);
	// REGISTER_TRANSFORM(TransformFraming);
	// REGISTER_TRANSFORM(TransformFunctionExpression);
	// REGISTER_TRANSFORM(TransformFunctionIdentifier);
	// REGISTER_TRANSFORM(TransformGroupingExpression);
	// REGISTER_TRANSFORM(TransformGroupingOrGroupingId);
	// REGISTER_TRANSFORM(TransformIgnoreNulls);
	// REGISTER_TRANSFORM(TransformInOperator);
	// REGISTER_TRANSFORM(TransformIndirection);
	// REGISTER_TRANSFORM(TransformIntervalLiteral);
	// REGISTER_TRANSFORM(TransformIntervalParameter);
	// REGISTER_TRANSFORM(TransformIntervalUnit);
	// REGISTER_TRANSFORM(TransformIsOperator);
	// REGISTER_TRANSFORM(TransformLambdaExpression);
	// REGISTER_TRANSFORM(TransformLambdaOperator);
	// REGISTER_TRANSFORM(TransformLikeOperator);
	// REGISTER_TRANSFORM(TransformLikeOrSimilarTo);
	// REGISTER_TRANSFORM(TransformListComprehensionExpression);
	// REGISTER_TRANSFORM(TransformListComprehensionFilter);
	// REGISTER_TRANSFORM(TransformListExpression);
	REGISTER_TRANSFORM(TransformLiteralExpression);
	// REGISTER_TRANSFORM(TransformMapExpression);
	// REGISTER_TRANSFORM(TransformNotNull);
	// REGISTER_TRANSFORM(TransformNullIfExpression);
	// REGISTER_TRANSFORM(TransformNumberedParameter);
	// REGISTER_TRANSFORM(TransformOperator);
	// REGISTER_TRANSFORM(TransformOperatorLiteral);
	// REGISTER_TRANSFORM(TransformOverClause);
	// REGISTER_TRANSFORM(TransformParameter);
	// REGISTER_TRANSFORM(TransformParenthesisExpression);
	// REGISTER_TRANSFORM(TransformPositionExpression);
	// REGISTER_TRANSFORM(TransformPositionalExpression);
	// REGISTER_TRANSFORM(TransformPostfixOperator);
	// REGISTER_TRANSFORM(TransformPrefixExpression);
	// REGISTER_TRANSFORM(TransformPrefixOperator);
	// REGISTER_TRANSFORM(TransformRecursiveExpression);
	// REGISTER_TRANSFORM(TransformRenameEntry);
	// REGISTER_TRANSFORM(TransformRenameList);
	// REGISTER_TRANSFORM(TransformReplaceEntry);
	// REGISTER_TRANSFORM(TransformReplaceList);
	// REGISTER_TRANSFORM(TransformRowExpression);
	// REGISTER_TRANSFORM(TransformSchemaReservedFunctionName);
	// REGISTER_TRANSFORM(TransformSchemaReservedTableColumnName);
	REGISTER_TRANSFORM(TransformSingleExpression);
	// REGISTER_TRANSFORM(TransformSliceBound);
	// REGISTER_TRANSFORM(TransformSliceExpression);
	// REGISTER_TRANSFORM(TransformSpecialFunctionExpression);
	// REGISTER_TRANSFORM(TransformStarExpression);
	// REGISTER_TRANSFORM(TransformStructExpression);
	// REGISTER_TRANSFORM(TransformStructField);
	// REGISTER_TRANSFORM(TransformSubqueryExpression);
	// REGISTER_TRANSFORM(TransformSubstringExpression);
	// REGISTER_TRANSFORM(TransformSubstringParameters);
	// REGISTER_TRANSFORM(TransformTableReservedColumnName);
	// REGISTER_TRANSFORM(TransformTrimDirection);
	// REGISTER_TRANSFORM(TransformTrimExpression);
	// REGISTER_TRANSFORM(TransformTrimSource);
	// REGISTER_TRANSFORM(TransformTypeLiteral);
	// REGISTER_TRANSFORM(TransformUnpackExpression);
	// REGISTER_TRANSFORM(TransformWindowExcludeClause);
	// REGISTER_TRANSFORM(TransformWindowExcludeElement);
	// REGISTER_TRANSFORM(TransformWindowFrame);
	// REGISTER_TRANSFORM(TransformWindowFrameContents);
	// REGISTER_TRANSFORM(TransformWindowFrameDefinition);
	// REGISTER_TRANSFORM(TransformWindowPartition);
	// REGISTER_TRANSFORM(TransformWithinGroupClause);

	// use.gram
	REGISTER_TRANSFORM(TransformUseStatement);
	REGISTER_TRANSFORM(TransformUseTarget);

	// set.gram
	REGISTER_TRANSFORM(TransformResetStatement);
	REGISTER_TRANSFORM(TransformSetAssignment);
	REGISTER_TRANSFORM(TransformSetSetting);
	REGISTER_TRANSFORM(TransformSetStatement);
	REGISTER_TRANSFORM(TransformSetTimeZone);
	REGISTER_TRANSFORM(TransformSetVariable);
	REGISTER_TRANSFORM(TransformStandardAssignment);
	REGISTER_TRANSFORM(TransformVariableList);

	RegisterEnum<SetScope>("LocalScope", SetScope::LOCAL);
	RegisterEnum<SetScope>("GlobalScope", SetScope::GLOBAL);
	RegisterEnum<SetScope>("SessionScope", SetScope::SESSION);
	RegisterEnum<SetScope>("VariableScope", SetScope::VARIABLE);
}

vector<optional_ptr<ParseResult>>
PEGTransformerFactory::ExtractParseResultsFromList(optional_ptr<ParseResult> parse_result) {
	// List(D) <- D (',' D)* ','?
	vector<optional_ptr<ParseResult>> result;
	auto &list_pr = parse_result->Cast<ListParseResult>();
	result.push_back(list_pr.GetChild(0));
	auto opt_child = list_pr.Child<OptionalParseResult>(1);
	if (opt_child.HasResult()) {
		auto repeat_result = opt_child.optional_result->Cast<RepeatParseResult>();
		for (auto &child : repeat_result.children) {
			auto &list_child = child->Cast<ListParseResult>();
			result.push_back(list_child.GetChild(1));
		}
	}

	return result;
}
} // namespace duckdb
