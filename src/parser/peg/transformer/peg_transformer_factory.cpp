#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/common/enums/trigger_type.hpp"
#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/enums/merge_action_type.hpp"
#include "duckdb/common/enums/subquery_type.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformStatement(PEGTransformer &transformer,
                                                                   ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	auto result = transformer.Transform<unique_ptr<SQLStatement>>(choice_pr.GetResult());
	if (!transformer.named_parameter_map.empty()) {
		// Avoid overriding a previous move with nothing
		result->named_param_map = transformer.named_parameter_map;
	}
	return result;
}

static unique_ptr<SQLStatement> ExtractAndTransformStatement(PEGTransformer &transformer,
                                                             const vector<MatcherToken> &tokens, ParseResult &stmt_pr,
                                                             optional_idx terminator_offset) {
	auto stmt = transformer.Transform<unique_ptr<SQLStatement>>(stmt_pr);

	if (!transformer.named_parameter_map.empty()) {
		stmt->named_param_map = transformer.named_parameter_map;
	}
	if (!transformer.pivot_entries.empty()) {
		stmt = transformer.CreatePivotStatement(std::move(stmt));
	}
	transformer.Clear();

	// Calculate location and length cleanly
	if (stmt_pr.offset.IsValid()) {
		stmt->stmt_location = stmt_pr.offset.GetIndex();

		idx_t end_index =
		    terminator_offset.IsValid() ? terminator_offset.GetIndex() : (tokens.back().offset + tokens.back().length);

		stmt->stmt_length = end_index - stmt->stmt_location;
	}

	return stmt;
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformTopLevelStatement(vector<MatcherToken> &tokens,
                                                                           ParserOptions &options,
                                                                           Matcher &root_matcher, idx_t &token_cursor) {
	if (token_cursor >= tokens.size()) {
		return nullptr;
	}
	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator parse_result_allocator;
	idx_t max_token_index = token_cursor;
	MatchState state(tokens, suggestions, parse_result_allocator, max_token_index, options.preserve_identifier_case,
	                 token_cursor);
	auto match_result = root_matcher.MatchParseResult(state);
	if (match_result == nullptr) {
		// syntax error — surface as a parser exception in the same shape as Transform()
		string token_stream;
		for (auto &token : tokens) {
			token_stream += token.text + " ";
		}
		idx_t error_token_idx = state.GetMaxTokenIndex();
		if (error_token_idx >= tokens.size()) {
			error_token_idx = tokens.size() - 1;
		}
		// Walk back past the EOI sentinel so the error message names a real token.
		if (error_token_idx > 0 && (tokens[error_token_idx].type == TokenType::END_OF_INPUT ||
		                            tokens[error_token_idx].type == TokenType::END_OF_INPUT_AUTOCOMPLETE)) {
			error_token_idx--;
		}
		auto &error_token = tokens[error_token_idx];
		auto error_message = "syntax error at or near \"" + error_token.text + "\"";
		throw ParserException::SyntaxError(token_stream, error_message, error_token.offset);
	}

	// Advance the caller's cursor past the consumed tokens.
	token_cursor = state.token_index;

	// TopLevelStatement <- Statement? (';'+ / EndOfInput)
	//   child 0: Optional<Statement>
	//   child 1: bracket-wrapper list around Choice<';'+ | EndOfInput>
	auto &tls = match_result->Cast<ListParseResult>();
	auto &stmt_opt = tls.Child<OptionalParseResult>(0);
	if (!stmt_opt.HasResult()) {
		// separator-only or EOI-only TopLevelStatement — no statement to yield
		return nullptr;
	}
	auto &term_wrapper = tls.Child<ListParseResult>(1);
	auto &term_inner = term_wrapper.Child<ChoiceParseResult>(0).GetResult();
	optional_idx terminator_offset;
	if (term_inner.type != ParseResultType::END_OF_INPUT) {
		auto semi_children = term_inner.Cast<RepeatParseResult>().GetChildren();
		if (!semi_children.empty()) {
			terminator_offset = semi_children[0].get().offset;
		}
	}

	ArenaAllocator transformer_allocator(Allocator::DefaultAllocator());
	PEGTransformerState transformer_state(tokens);
	PEGTransformer transformer(transformer_allocator, transformer_state, sql_transform_functions, parser.rules,
	                           enum_mappings, options);

	return ExtractAndTransformStatement(transformer, tokens, stmt_opt.GetResult(), terminator_offset);
}

#define REGISTER_TRANSFORM(FUNCTION) Register(string(#FUNCTION).substr(9), &FUNCTION)

void PEGTransformerFactory::RegisterComment() {
	// comment.gram
	REGISTER_TRANSFORM(TransformCommentValue);
}

void PEGTransformerFactory::RegisterCommon() {
	// common.gram
	REGISTER_TRANSFORM(TransformNumberLiteral);
	REGISTER_TRANSFORM(TransformStringLiteral);
	REGISTER_TRANSFORM(TransformIntervalToIntervalAsType);
}

void PEGTransformerFactory::RegisterCreateMacro() {
	// create_macro.gram
}

void PEGTransformerFactory::RegisterCreateTable() {
	// create_table.gram
	REGISTER_TRANSFORM(TransformColLabelOrString);
	REGISTER_TRANSFORM(TransformIdentifier);
	REGISTER_TRANSFORM(TransformCubeOrRollup);
}

void PEGTransformerFactory::RegisterExpression() {
	// expression.gram
	REGISTER_TRANSFORM(TransformBaseExpression);
	REGISTER_TRANSFORM(TransformExpression);
	Register("ColumnDefaultExpr", &TransformExpression);
	Register("ColDefOrExpr", &TransformLogicalOrExpression);
	Register("ColDefAndExpr", &TransformLogicalAndExpression);
	REGISTER_TRANSFORM(TransformLambdaArrowExpression);
	REGISTER_TRANSFORM(TransformLogicalOrExpression);
	REGISTER_TRANSFORM(TransformLogicalAndExpression);
	REGISTER_TRANSFORM(TransformLogicalNotExpression);
	REGISTER_TRANSFORM(TransformIsExpression);
	REGISTER_TRANSFORM(TransformIsTest);
	REGISTER_TRANSFORM(TransformIsLiteral);
	REGISTER_TRANSFORM(TransformNotNull);
	REGISTER_TRANSFORM(TransformIsNull);
	REGISTER_TRANSFORM(TransformIsDistinctFromExpression);
	REGISTER_TRANSFORM(TransformBetweenInLikeExpression);
	REGISTER_TRANSFORM(TransformBetweenInLikeOp);
	REGISTER_TRANSFORM(TransformInClause);
	REGISTER_TRANSFORM(TransformInExpression);
	REGISTER_TRANSFORM(TransformInExpressionList);
	REGISTER_TRANSFORM(TransformInSelectStatement);
	REGISTER_TRANSFORM(TransformBetweenClause);
	REGISTER_TRANSFORM(TransformLikeClause);
	REGISTER_TRANSFORM(TransformEscapeClause);
	REGISTER_TRANSFORM(TransformLikeVariations);
	REGISTER_TRANSFORM(TransformComparisonExpression);
	REGISTER_TRANSFORM(TransformComparisonOperator);
	REGISTER_TRANSFORM(TransformOtherOperatorExpression);
	REGISTER_TRANSFORM(TransformOtherOperator);
	REGISTER_TRANSFORM(TransformQualifiedOperator);
	REGISTER_TRANSFORM(TransformAnyOp);
	REGISTER_TRANSFORM(TransformStringOperator);
	REGISTER_TRANSFORM(TransformJsonOperator);
	REGISTER_TRANSFORM(TransformInetOperator);
	REGISTER_TRANSFORM(TransformAnyAllOperator);
	REGISTER_TRANSFORM(TransformAnyOrAll);
	REGISTER_TRANSFORM(TransformListOperator);
	REGISTER_TRANSFORM(TransformLambdaOperator);
	REGISTER_TRANSFORM(TransformBitwiseExpression);
	REGISTER_TRANSFORM(TransformBitOperator);
	REGISTER_TRANSFORM(TransformAdditiveExpression);
	REGISTER_TRANSFORM(TransformTerm);
	REGISTER_TRANSFORM(TransformMultiplicativeExpression);
	REGISTER_TRANSFORM(TransformFactor);
	REGISTER_TRANSFORM(TransformExponentiationExpression);
	REGISTER_TRANSFORM(TransformExponentOperator);
	REGISTER_TRANSFORM(TransformPostfixOperator);
	REGISTER_TRANSFORM(TransformCollateExpression);
	REGISTER_TRANSFORM(TransformAtTimeZoneExpression);
	REGISTER_TRANSFORM(TransformPrefixExpression);

	REGISTER_TRANSFORM(TransformColumnReference);
	REGISTER_TRANSFORM(TransformCatalogReservedSchemaTableColumnName);
	REGISTER_TRANSFORM(TransformSchemaReservedTableColumnName);

	REGISTER_TRANSFORM(TransformParameter);
	REGISTER_TRANSFORM(TransformAnonymousParameter);
	REGISTER_TRANSFORM(TransformQuestionMarkNumberedParameter);
	REGISTER_TRANSFORM(TransformColLabelParameter);
	REGISTER_TRANSFORM(TransformNumberedParameter);
	REGISTER_TRANSFORM(TransformPositionalExpression);

	REGISTER_TRANSFORM(TransformLiteralExpression);
	REGISTER_TRANSFORM(TransformParensExpression);
	REGISTER_TRANSFORM(TransformSingleExpression);
	REGISTER_TRANSFORM(TransformConstantLiteral);
	REGISTER_TRANSFORM(TransformFalseLiteral);
	REGISTER_TRANSFORM(TransformTrueLiteral);
	REGISTER_TRANSFORM(TransformNullLiteral);
	REGISTER_TRANSFORM(TransformUnknownLiteral);

	REGISTER_TRANSFORM(TransformPrefixOperator);
	REGISTER_TRANSFORM(TransformListExpression);
	REGISTER_TRANSFORM(TransformStructExpression);
	REGISTER_TRANSFORM(TransformStructField);
	REGISTER_TRANSFORM(TransformBoundedListExpression);
	REGISTER_TRANSFORM(TransformArrayBoundedListExpression);
	REGISTER_TRANSFORM(TransformArrayParensSelect);
	REGISTER_TRANSFORM(TransformFunctionExpression);
	REGISTER_TRANSFORM(TransformWithinGroupClause);
	REGISTER_TRANSFORM(TransformFilterClause);
	REGISTER_TRANSFORM(TransformFunctionIdentifier);
	REGISTER_TRANSFORM(TransformSchemaReservedFunctionName);
	REGISTER_TRANSFORM(TransformCatalogReservedSchemaFunctionName);
	REGISTER_TRANSFORM(TransformParenthesisExpression);
	REGISTER_TRANSFORM(TransformIndirection);
	REGISTER_TRANSFORM(TransformCastOperator);
	REGISTER_TRANSFORM(TransformDotOperator);
	REGISTER_TRANSFORM(TransformSliceExpression);
	REGISTER_TRANSFORM(TransformSliceBound);
	REGISTER_TRANSFORM(TransformEndSliceBound);
	REGISTER_TRANSFORM(TransformStepSliceBound);

	REGISTER_TRANSFORM(TransformTableReservedColumnName);
	REGISTER_TRANSFORM(TransformColIdDot);
	REGISTER_TRANSFORM(TransformStarExpression);
	REGISTER_TRANSFORM(TransformExcludeList);
	REGISTER_TRANSFORM(TransformExcludeNameList);
	REGISTER_TRANSFORM(TransformExcludeNameSingle);
	REGISTER_TRANSFORM(TransformExcludeName);
	REGISTER_TRANSFORM(TransformReplaceList);
	REGISTER_TRANSFORM(TransformReplaceEntries);
	REGISTER_TRANSFORM(TransformReplaceEntrySingle);
	REGISTER_TRANSFORM(TransformReplaceEntryList);
	REGISTER_TRANSFORM(TransformReplaceEntry);

	REGISTER_TRANSFORM(TransformOverClause);
	REGISTER_TRANSFORM(TransformWindowFrame);
	REGISTER_TRANSFORM(TransformParensIdentifier);
	REGISTER_TRANSFORM(TransformWindowFrameDefinition);
	REGISTER_TRANSFORM(TransformWindowFrameContentsParens);
	REGISTER_TRANSFORM(TransformWindowFrameNameContentsParens);
	REGISTER_TRANSFORM(TransformBaseWindowName);
	REGISTER_TRANSFORM(TransformWindowFrameContents);
	REGISTER_TRANSFORM(TransformFraming);
	REGISTER_TRANSFORM(TransformFrameExtent);
	REGISTER_TRANSFORM(TransformBetweenFrameExtent);
	REGISTER_TRANSFORM(TransformSingleFrameExtent);
	REGISTER_TRANSFORM(TransformFrameBound);
	REGISTER_TRANSFORM(TransformFrameUnbounded);
	REGISTER_TRANSFORM(TransformFrameCurrentRow);
	REGISTER_TRANSFORM(TransformFrameExpression);
	REGISTER_TRANSFORM(TransformPrecedingOrFollowing);

	REGISTER_TRANSFORM(TransformFrameClause);
	REGISTER_TRANSFORM(TransformWindowExcludeClause);
	REGISTER_TRANSFORM(TransformWindowExcludeElement);

	REGISTER_TRANSFORM(TransformWindowPartition);

	REGISTER_TRANSFORM(TransformSpecialFunctionExpression);
	REGISTER_TRANSFORM(TransformCoalesceExpression);
	REGISTER_TRANSFORM(TransformUnpackExpression);
	REGISTER_TRANSFORM(TransformTryExpression);
	REGISTER_TRANSFORM(TransformColumnsExpression);
	REGISTER_TRANSFORM(TransformExtractExpression);
	REGISTER_TRANSFORM(TransformExtractArgument);
	REGISTER_TRANSFORM(TransformLambdaExpression);
	REGISTER_TRANSFORM(TransformNullIfExpression);
	REGISTER_TRANSFORM(TransformRowExpression);
	REGISTER_TRANSFORM(TransformSubstringExpression);
	REGISTER_TRANSFORM(TransformSubstringArguments);
	REGISTER_TRANSFORM(TransformSubstringExpressionList);
	REGISTER_TRANSFORM(TransformSubstringParameters);
	REGISTER_TRANSFORM(TransformSubstringFromFor);
	REGISTER_TRANSFORM(TransformSubstringFromOptionalFor);
	REGISTER_TRANSFORM(TransformSubstringFor);
	REGISTER_TRANSFORM(TransformTrimExpression);
	REGISTER_TRANSFORM(TransformTrimDirection);
	REGISTER_TRANSFORM(TransformTrimSource);
	REGISTER_TRANSFORM(TransformOverlayExpression);
	REGISTER_TRANSFORM(TransformOverlayArguments);
	REGISTER_TRANSFORM(TransformOverlayParameters);
	REGISTER_TRANSFORM(TransformFromExpression);
	REGISTER_TRANSFORM(TransformForExpression);
	REGISTER_TRANSFORM(TransformOverlayExpressionList);
	REGISTER_TRANSFORM(TransformPositionExpression);
	REGISTER_TRANSFORM(TransformCastExpression);
	REGISTER_TRANSFORM(TransformCastOrTryCast);
	REGISTER_TRANSFORM(TransformCaseExpression);
	REGISTER_TRANSFORM(TransformCaseElse);
	REGISTER_TRANSFORM(TransformCaseWhenThen);
	REGISTER_TRANSFORM(TransformTypeLiteral);
	REGISTER_TRANSFORM(TransformDefaultExpression);
	REGISTER_TRANSFORM(TransformIntervalLiteral);
	REGISTER_TRANSFORM(TransformIntervalParameter);
	REGISTER_TRANSFORM(TransformSubqueryExpression);
	REGISTER_TRANSFORM(TransformMapExpression);
	REGISTER_TRANSFORM(TransformMapStructExpression);
	REGISTER_TRANSFORM(TransformMapStructField);
	REGISTER_TRANSFORM(TransformListComprehensionExpression);
	REGISTER_TRANSFORM(TransformListComprehensionFilter);
	REGISTER_TRANSFORM(TransformIsDistinctFromOp);
	REGISTER_TRANSFORM(TransformGroupingExpression);
	REGISTER_TRANSFORM(TransformMethodExpression);
	REGISTER_TRANSFORM(TransformRenameList);
	REGISTER_TRANSFORM(TransformRenameEntryList);
	REGISTER_TRANSFORM(TransformSingleRenameEntry);
	REGISTER_TRANSFORM(TransformRenameEntry);

	REGISTER_TRANSFORM(TransformIgnoreOrRespectNulls);
}

void PEGTransformerFactory::RegisterConnect() {
	// connect.gram — both rules are hand-written; the generator skips them because of the
	// optional SessionTarget sub-rule.
	REGISTER_TRANSFORM(TransformConnectStatement);
}

void PEGTransformerFactory::RegisterPivot() {
	// PivotStatement and UnpivotStatement measure parameter usage while transforming
	// the source table, so their top-level wrappers remain manual.
	REGISTER_TRANSFORM(TransformPivotStatement);
	REGISTER_TRANSFORM(TransformUnpivotStatement);
}

void PEGTransformerFactory::RegisterSelect() {
	// select.gram
	REGISTER_TRANSFORM(TransformSelectStatement);
	REGISTER_TRANSFORM(TransformSelectStatementInternal);
	REGISTER_TRANSFORM(TransformSelectSetOpChain);
	REGISTER_TRANSFORM(TransformIntersectChain);
	REGISTER_TRANSFORM(TransformSelectAtom);
	REGISTER_TRANSFORM(TransformSetopClause);
	REGISTER_TRANSFORM(TransformSetIntersectClause);
	REGISTER_TRANSFORM(TransformSetopType);
	REGISTER_TRANSFORM(TransformDistinctOrAll);
	REGISTER_TRANSFORM(TransformSelectParens);
	REGISTER_TRANSFORM(TransformSelectStatementType);
	REGISTER_TRANSFORM(TransformOptionalParensSimpleSelect);
	REGISTER_TRANSFORM(TransformSimpleSelectParens);
	REGISTER_TRANSFORM(TransformSimpleSelect);
	REGISTER_TRANSFORM(TransformSelectFrom);
	REGISTER_TRANSFORM(TransformSelectFromClause);
	REGISTER_TRANSFORM(TransformFromSelectClause);
	REGISTER_TRANSFORM(TransformFromClause);
	REGISTER_TRANSFORM(TransformSelectClause);
	REGISTER_TRANSFORM(TransformDistinctClause);
	REGISTER_TRANSFORM(TransformDistinctOn);
	REGISTER_TRANSFORM(TransformDistinctOnTargets);
	REGISTER_TRANSFORM(TransformDistinctAll);
	REGISTER_TRANSFORM(TransformFunctionArgument);
	REGISTER_TRANSFORM(TransformBaseTableName);
	REGISTER_TRANSFORM(TransformSchemaReservedTable);
	REGISTER_TRANSFORM(TransformCatalogReservedSchemaTable);
	REGISTER_TRANSFORM(TransformWhereClause);
	REGISTER_TRANSFORM(TransformTableFunctionArguments);

	REGISTER_TRANSFORM(TransformTargetList);
	REGISTER_TRANSFORM(TransformAliasedExpression);
	REGISTER_TRANSFORM(TransformExpressionAsCollabel);
	REGISTER_TRANSFORM(TransformColIdExpression);
	REGISTER_TRANSFORM(TransformExpressionOptIdentifier);
	REGISTER_TRANSFORM(TransformTableAlias);
	REGISTER_TRANSFORM(TransformTableAliasAs);
	REGISTER_TRANSFORM(TransformTableAliasWithoutAs);
	REGISTER_TRANSFORM(TransformColumnAliases);
	REGISTER_TRANSFORM(TransformNamedParameter);
	REGISTER_TRANSFORM(TransformTableRef);

	REGISTER_TRANSFORM(TransformOrderByClause);
	REGISTER_TRANSFORM(TransformOrderByExpressions);
	REGISTER_TRANSFORM(TransformOrderByExpressionList);
	REGISTER_TRANSFORM(TransformOrderByAll);
	REGISTER_TRANSFORM(TransformOrderByExpression);
	REGISTER_TRANSFORM(TransformDescOrAsc);
	REGISTER_TRANSFORM(TransformNullsFirstOrLast);

	REGISTER_TRANSFORM(TransformJoinOrPivot);
	REGISTER_TRANSFORM(TransformJoinClause);
	REGISTER_TRANSFORM(TransformRegularJoinClause);
	REGISTER_TRANSFORM(TransformJoinType);
	REGISTER_TRANSFORM(TransformJoinQualifier);
	REGISTER_TRANSFORM(TransformOnClause);
	REGISTER_TRANSFORM(TransformUsingClause);
	REGISTER_TRANSFORM(TransformJoinWithoutOnClause);
	REGISTER_TRANSFORM(TransformJoinPrefix);
	REGISTER_TRANSFORM(TransformCrossJoinPrefix);
	REGISTER_TRANSFORM(TransformNaturalJoinPrefix);
	REGISTER_TRANSFORM(TransformPositionalJoinPrefix);
	REGISTER_TRANSFORM(TransformTableUnpivotClause);
	REGISTER_TRANSFORM(TransformUnpivotValueList);
	REGISTER_TRANSFORM(TransformUnpivotTargetList);

	REGISTER_TRANSFORM(TransformTablePivotClause);
	REGISTER_TRANSFORM(TransformPivotValueList);
	REGISTER_TRANSFORM(TransformPivotHeader);
	REGISTER_TRANSFORM(TransformPivotGroupByList);
	REGISTER_TRANSFORM(TransformPivotTargetList);

	REGISTER_TRANSFORM(TransformInnerTableRef);
	REGISTER_TRANSFORM(TransformTableFunction);
	REGISTER_TRANSFORM(TransformTableFunctionLateralOpt);
	REGISTER_TRANSFORM(TransformTableFunctionAliasColon);
	REGISTER_TRANSFORM(TransformTableAliasColon);
	REGISTER_TRANSFORM(TransformQualifiedTableFunction);
	REGISTER_TRANSFORM(TransformTableSubquery);
	REGISTER_TRANSFORM(TransformSubqueryReference);
	REGISTER_TRANSFORM(TransformBaseTableRef);
	REGISTER_TRANSFORM(TransformAtClause);
	REGISTER_TRANSFORM(TransformAtSpecifier);
	REGISTER_TRANSFORM(TransformAtUnit);
	REGISTER_TRANSFORM(TransformValuesRef);
	REGISTER_TRANSFORM(TransformValuesClause);
	REGISTER_TRANSFORM(TransformValuesExpressions);
	REGISTER_TRANSFORM(TransformTableStatement);
	REGISTER_TRANSFORM(TransformParensTableRef);

	REGISTER_TRANSFORM(TransformResultModifiers);
	REGISTER_TRANSFORM(TransformLimitOffset);
	REGISTER_TRANSFORM(TransformLimitOffsetClause);
	REGISTER_TRANSFORM(TransformOffsetLimitClause);
	REGISTER_TRANSFORM(TransformLimitClause);
	REGISTER_TRANSFORM(TransformLimitValue);
	REGISTER_TRANSFORM(TransformLimitAll);
	REGISTER_TRANSFORM(TransformLimitLiteralPercent);
	REGISTER_TRANSFORM(TransformLimitExpression);
	REGISTER_TRANSFORM(TransformOffsetClause);
	REGISTER_TRANSFORM(TransformGroupByClause);
	REGISTER_TRANSFORM(TransformGroupByExpressions);
	REGISTER_TRANSFORM(TransformGroupByAll);
	REGISTER_TRANSFORM(TransformGroupByList);
	REGISTER_TRANSFORM(TransformGroupByExpression);
	REGISTER_TRANSFORM(TransformEmptyGroupingItem);
	REGISTER_TRANSFORM(TransformCubeOrRollupClause);
	REGISTER_TRANSFORM(TransformGroupingSetsClause);

	REGISTER_TRANSFORM(TransformWithClause);
	REGISTER_TRANSFORM(TransformWithStatement);
	REGISTER_TRANSFORM(TransformCTEBody);
	REGISTER_TRANSFORM(TransformMaterialized);
	REGISTER_TRANSFORM(TransformHavingClause);
	REGISTER_TRANSFORM(TransformOffsetValue);
	REGISTER_TRANSFORM(TransformQualifyClause);
	REGISTER_TRANSFORM(TransformWindowClause);
	REGISTER_TRANSFORM(TransformWindowDefinition);
	REGISTER_TRANSFORM(TransformUsingKey);

	REGISTER_TRANSFORM(TransformSampleClause);
	REGISTER_TRANSFORM(TransformSampleEntry);
	REGISTER_TRANSFORM(TransformSampleEntryFunction);
	REGISTER_TRANSFORM(TransformSampleEntryCount);
	REGISTER_TRANSFORM(TransformSampleCount);
	REGISTER_TRANSFORM(TransformSampleValue);
	REGISTER_TRANSFORM(TransformSampleUnit);
	REGISTER_TRANSFORM(TransformSampleProperties);
	REGISTER_TRANSFORM(TransformSampleSeed);
	REGISTER_TRANSFORM(TransformSampleFunction);
	REGISTER_TRANSFORM(TransformRepeatableSample);
}

void PEGTransformerFactory::RegisterKeywordsAndIdentifiers() {
	Register("PragmaName", &TransformIdentifierOrKeyword);
	Register("TypeName", &TransformIdentifierOrKeyword);
	Register("ColLabel", &TransformIdentifierOrKeyword);
	Register("PlainIdentifier", &TransformIdentifierOrKeyword);
	Register("QuotedIdentifier", &TransformIdentifierOrKeyword);
	Register("ReservedKeyword", &TransformIdentifierOrKeyword);
	Register("UnreservedKeyword", &TransformIdentifierOrKeyword);
	Register("ColumnNameKeyword", &TransformIdentifierOrKeyword);
	Register("FuncNameKeyword", &TransformIdentifierOrKeyword);
	Register("TypeNameKeyword", &TransformIdentifierOrKeyword);
	Register("SettingName", &TransformIdentifierOrKeyword);
	Register("ExplainOptionName", &TransformIdentifierOrKeyword);
}

void PEGTransformerFactory::RegisterEnums() {
	RegisterEnum<CatalogType>("MaterializedViewEntry", CatalogType::VIEW_ENTRY);

	RegisterEnum<string>("MinusPrefixOperator", "-");
	RegisterEnum<string>("PlusPrefixOperator", "+");
	RegisterEnum<string>("TildePrefixOperator", "~");

	RegisterEnum<OrderType>("DescendingOrder", OrderType::DESCENDING);
	RegisterEnum<OrderType>("AscendingOrder", OrderType::ASCENDING);
	RegisterEnum<OrderByNullType>("NullsFirst", OrderByNullType::NULLS_FIRST);
	RegisterEnum<OrderByNullType>("NullsLast", OrderByNullType::NULLS_LAST);

	RegisterEnum<JoinType>("FullJoin", JoinType::OUTER);
	RegisterEnum<JoinType>("LeftJoin", JoinType::LEFT);
	RegisterEnum<JoinType>("RightJoin", JoinType::RIGHT);
	RegisterEnum<JoinType>("SemiJoin", JoinType::SEMI);
	RegisterEnum<JoinType>("AntiJoin", JoinType::ANTI);
	RegisterEnum<JoinType>("InnerJoin", JoinType::INNER);

	RegisterEnum<ExpressionType>("OperatorEqual", ExpressionType::COMPARE_EQUAL);
	RegisterEnum<ExpressionType>("OperatorNotEqual", ExpressionType::COMPARE_NOTEQUAL);
	RegisterEnum<ExpressionType>("OperatorLessThan", ExpressionType::COMPARE_LESSTHAN);
	RegisterEnum<ExpressionType>("OperatorGreaterThan", ExpressionType::COMPARE_GREATERTHAN);
	RegisterEnum<ExpressionType>("OperatorLessThanEquals", ExpressionType::COMPARE_LESSTHANOREQUALTO);
	RegisterEnum<ExpressionType>("OperatorGreaterThanEquals", ExpressionType::COMPARE_GREATERTHANOREQUALTO);

	RegisterEnum<SetOperationType>("SetopUnion", SetOperationType::UNION);
	RegisterEnum<SetOperationType>("SetopExcept", SetOperationType::EXCEPT);

	RegisterEnum<string>("TrimBoth", "trim");
	RegisterEnum<string>("TrimLeading", "ltrim");
	RegisterEnum<string>("TrimTrailing", "rtrim");

	RegisterEnum<string>("LikeToken", "~~");
	RegisterEnum<string>("ILikeToken", "~~*");
	RegisterEnum<string>("GlobToken", "~~~");
	RegisterEnum<string>("SimilarToToken", "regexp_full_match");
	RegisterEnum<string>("NotILikeOp", "!~~*");
	RegisterEnum<string>("NotLikeOp", "!~~");
	RegisterEnum<string>("NotSimilarToOp", "!~");

	RegisterEnum<WindowExcludeMode>("ExcludeCurrentRow", WindowExcludeMode::CURRENT_ROW);
	RegisterEnum<WindowExcludeMode>("ExcludeGroup", WindowExcludeMode::GROUP);
	RegisterEnum<WindowExcludeMode>("ExcludeTies", WindowExcludeMode::TIES);
	RegisterEnum<WindowExcludeMode>("ExcludeNoOthers", WindowExcludeMode::NO_OTHER);

	RegisterEnum<bool>("SamplePercentage", true);
	RegisterEnum<bool>("SampleRows", false);

	RegisterEnum<bool>("SubqueryAny", true);
	RegisterEnum<bool>("SubqueryAll", false);

	RegisterEnum<bool>("IgnoreNulls", true);
	RegisterEnum<bool>("RespectNulls", false);

	RegisterEnum<bool>("IncludeNulls", true);
	RegisterEnum<bool>("ExcludeNulls", false);
}

PEGTransformerFactory::PEGTransformerFactory() {
	RegisterGenerated();
	REGISTER_TRANSFORM(TransformStatement);
	RegisterComment();
	RegisterCommon();
	RegisterCreateMacro();
	RegisterCreateTable();
	RegisterExpression();
	RegisterConnect();
	RegisterPivot();
	RegisterSelect();
	RegisterKeywordsAndIdentifiers();
	RegisterEnums();
}

vector<reference<ParseResult>> PEGTransformerFactory::ExtractParseResultsFromList(ParseResult &parse_result) {
	// List(D) <- D (',' D)* ','?
	vector<reference<ParseResult>> result;
	auto &list_pr = parse_result.Cast<ListParseResult>();
	result.push_back(list_pr.GetChild(0));
	auto &opt_child = list_pr.Child<OptionalParseResult>(1);
	if (opt_child.HasResult()) {
		auto &repeat_result = opt_child.GetResult().Cast<RepeatParseResult>();
		for (auto &child : repeat_result.GetChildren()) {
			auto &list_child = child.get().Cast<ListParseResult>();
			result.push_back(list_child.GetChild(1));
		}
	}
	return result;
}

ParseResult &PEGTransformerFactory::ExtractResultFromParens(ParseResult &parse_result) {
	// Parens(D) <- '(' D ')'
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return list_pr.GetChild(1);
}

bool PEGTransformerFactory::ExpressionIsEmptyStar(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::STAR) {
		return false;
	}
	auto &star = expr.Cast<StarExpression>();
	if (!star.IsColumns() && star.ExcludeList().empty() && star.ReplaceList().empty()) {
		return true;
	}
	return false;
}

QualifiedName PEGTransformerFactory::StringToQualifiedName(vector<string> input) {
	QualifiedName result;
	if (input.empty()) {
		throw InternalException("QualifiedName cannot be made with an empty input.");
	}
	if (input.size() == 1) {
		result.catalog = Identifier::InvalidCatalog();
		result.schema = Identifier::InvalidSchema();
		result.name = Identifier(input[0]);
	} else if (input.size() == 2) {
		result.catalog = Identifier::InvalidCatalog();
		result.schema = Identifier(input[0]);
		result.name = Identifier(input[1]);
	} else if (input.size() == 3) {
		result.catalog = Identifier(input[0]);
		result.schema = Identifier(input[1]);
		result.name = Identifier(input[2]);
	} else {
		throw ParserException("Too many qualifications found - expected [catalog.schema.name] or [schema.name]");
	}
	return result;
}

LogicalType PEGTransformerFactory::GetIntervalTargetType(DatePartSpecifier date_part) {
	switch (date_part) {
	case DatePartSpecifier::YEAR:
	case DatePartSpecifier::MONTH:
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::QUARTER:
	case DatePartSpecifier::DECADE:
	case DatePartSpecifier::CENTURY:
	case DatePartSpecifier::MILLENNIUM:
		return LogicalType::INTEGER;
	case DatePartSpecifier::HOUR:
	case DatePartSpecifier::MINUTE:
	case DatePartSpecifier::MICROSECONDS:
		return LogicalType::BIGINT;
	case DatePartSpecifier::MILLISECONDS:
	case DatePartSpecifier::SECOND:
		return LogicalType::DOUBLE;
	default:
		throw InternalException("Unsupported interval post-fix");
	}
}

bool PEGTransformerFactory::ConstructConstantFromExpression(const ParsedExpression &expr, Value &value) {
	// We have to construct it like this because we don't have the ClientContext for binding/executing the expr here
	switch (expr.GetExpressionType()) {
	case ExpressionType::FUNCTION: {
		auto &function = expr.Cast<FunctionExpression>();
		if (function.FunctionName() == "struct_pack") {
			identifier_set_t unique_names;
			child_list_t<Value> values;
			values.reserve(function.GetArguments().size());
			for (const auto &child : function.GetArguments()) {
				if (!unique_names.insert(child.GetExpression().GetAlias()).second) {
					throw BinderException("Duplicate struct entry name \"%s\"",
					                      child.GetExpression().GetAlias().GetIdentifierName());
				}
				Value child_value;
				if (!ConstructConstantFromExpression(child.GetExpression(), child_value)) {
					return false;
				}
				values.emplace_back(child.GetExpression().GetAlias(), std::move(child_value));
			}
			value = Value::STRUCT(std::move(values));
			return true;
		} else if (function.FunctionName() == "list_value") {
			vector<Value> values;
			values.reserve(function.GetArguments().size());
			for (const auto &child : function.GetArguments()) {
				Value child_value;
				if (!ConstructConstantFromExpression(child.GetExpression(), child_value)) {
					return false;
				}
				values.emplace_back(std::move(child_value));
			}

			// figure out child type
			LogicalType child_type(LogicalTypeId::SQLNULL);
			for (auto &child_value : values) {
				child_type = LogicalType::DefaultForceMaxLogicalType(child_type, child_value.type());
			}

			// finally create the list
			value = Value::LIST(child_type, values);
			return true;
		} else if (function.FunctionName() == "map") {
			Value keys;
			if (!ConstructConstantFromExpression(function.GetArguments()[0].GetExpression(), keys)) {
				return false;
			}

			Value values;
			if (!ConstructConstantFromExpression(function.GetArguments()[1].GetExpression(), values)) {
				return false;
			}

			vector<Value> keys_unpacked = ListValue::GetChildren(keys);
			vector<Value> values_unpacked = ListValue::GetChildren(values);

			value = Value::MAP(ListType::GetChildType(keys.type()), ListType::GetChildType(values.type()),
			                   keys_unpacked, values_unpacked);
			return true;
		} else {
			return false;
		}
	}
	case ExpressionType::VALUE_CONSTANT: {
		auto &constant = expr.Cast<ConstantExpression>();
		value = constant.GetValue();
		return true;
	}
	case ExpressionType::OPERATOR_CAST: {
		auto &cast = expr.Cast<CastExpression>();
		Value dummy_value;
		if (!ConstructConstantFromExpression(cast.Child(), dummy_value)) {
			return false;
		}

		string error_message;
		if (!dummy_value.DefaultTryCastAs(cast.TargetType(), value, &error_message)) {
			throw ConversionException("Unable to cast %s to %s", dummy_value.ToString(),
			                          EnumUtil::ToString(cast.TargetType().id()));
		}
		return true;
	}
	default:
		return false;
	}
}

} // namespace duckdb
