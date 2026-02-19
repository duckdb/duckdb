#include "transformer/peg_transformer.hpp"
#include "matcher.hpp"
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
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	auto result = transformer.Transform<unique_ptr<SQLStatement>>(choice_pr.result);
	if (!transformer.named_parameter_map.empty()) {
		// Avoid overriding a previous move with nothing
		result->named_param_map = transformer.named_parameter_map;
	}
	return result;
}

unique_ptr<SQLStatement> PEGTransformerFactory::Transform(vector<MatcherToken> &tokens, ParserOptions &options) {
	string token_stream;
	for (auto &token : tokens) {
		token_stream += token.text + " ";
	}
	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator parse_result_allocator;
	idx_t max_token_index = 0;
	MatchState state(tokens, suggestions, parse_result_allocator, max_token_index);
	MatcherAllocator allocator;
	auto &matcher = Matcher::RootMatcher(allocator);
	auto match_result = matcher.MatchParseResult(state);
	if (match_result == nullptr || state.token_index < state.tokens.size()) {
		idx_t error_token_idx = state.GetMaxTokenIndex();
		if (error_token_idx >= tokens.size()) {
			error_token_idx = tokens.size() - 1;
		}
		auto &error_token = tokens[error_token_idx];
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
		auto error_message = "Syntax error at or near \"" + error_token.text + "\"";
		throw ParserException::SyntaxError(token_stream, error_message, error_token.offset);
	}
	match_result->name = "Statement";
	ArenaAllocator transformer_allocator(Allocator::DefaultAllocator());
	PEGTransformerState transformer_state(tokens);
	auto &factory = GetInstance();
	PEGTransformer transformer(transformer_allocator, transformer_state, factory.sql_transform_functions,
	                           factory.parser.rules, factory.enum_mappings, options);
	auto result = transformer.Transform<unique_ptr<SQLStatement>>(match_result);
	if (!transformer.pivot_entries.empty()) {
		result = transformer.CreatePivotStatement(std::move(result));
	}
	transformer.Clear();
	return result;
}

#define REGISTER_TRANSFORM(FUNCTION) Register(string(#FUNCTION).substr(9), &FUNCTION)

PEGTransformerFactory &PEGTransformerFactory::GetInstance() {
	static PEGTransformerFactory instance;
	return instance;
}

void PEGTransformerFactory::RegisterAlter() {
	// alter.gram
	REGISTER_TRANSFORM(TransformAlterStatement);
	REGISTER_TRANSFORM(TransformAlterOptions);
	REGISTER_TRANSFORM(TransformAlterTableStmt);
	REGISTER_TRANSFORM(TransformAlterViewStmt);
	REGISTER_TRANSFORM(TransformAlterDatabaseStmt);
	REGISTER_TRANSFORM(TransformAlterSequenceStmt);
	REGISTER_TRANSFORM(TransformAlterSequenceOptions);
	REGISTER_TRANSFORM(TransformSetSequenceOption);
	REGISTER_TRANSFORM(TransformAlterTableOptions);
	REGISTER_TRANSFORM(TransformAddColumn);
	REGISTER_TRANSFORM(TransformAddColumnEntry);
	REGISTER_TRANSFORM(TransformDropColumn);
	REGISTER_TRANSFORM(TransformSetPartitionedBy);
	REGISTER_TRANSFORM(TransformResetPartitionedBy);
	REGISTER_TRANSFORM(TransformAlterColumn);
	REGISTER_TRANSFORM(TransformAlterColumnEntry);
	REGISTER_TRANSFORM(TransformDropDefault);
	REGISTER_TRANSFORM(TransformChangeNullability);
	REGISTER_TRANSFORM(TransformAlterType);
	REGISTER_TRANSFORM(TransformUsingExpression);
	REGISTER_TRANSFORM(TransformDropOrSet);
	REGISTER_TRANSFORM(TransformAddOrDropDefault);
	REGISTER_TRANSFORM(TransformAddDefault);
	REGISTER_TRANSFORM(TransformRenameColumn);
	REGISTER_TRANSFORM(TransformRenameAlter);
	REGISTER_TRANSFORM(TransformAddConstraint);
	REGISTER_TRANSFORM(TransformQualifiedSequenceName);
	REGISTER_TRANSFORM(TransformSequenceName);
	REGISTER_TRANSFORM(TransformSetSortedBy);
	REGISTER_TRANSFORM(TransformResetSortedBy);
}

void PEGTransformerFactory::RegisterAttach() {
	// attach.gram
	REGISTER_TRANSFORM(TransformAttachStatement);
	REGISTER_TRANSFORM(TransformAttachAlias);
	REGISTER_TRANSFORM(TransformAttachOptions);
	REGISTER_TRANSFORM(TransformGenericCopyOptionList);
	REGISTER_TRANSFORM(TransformGenericCopyOption);
	REGISTER_TRANSFORM(TransformDatabasePath);
}

void PEGTransformerFactory::RegisterAnalyze() {
	// analyze.gram
	REGISTER_TRANSFORM(TransformAnalyzeStatement);
	REGISTER_TRANSFORM(TransformAnalyzeTarget);
}

void PEGTransformerFactory::RegisterCall() {
	// call.gram
	REGISTER_TRANSFORM(TransformCallStatement);
	REGISTER_TRANSFORM(TransformTableFunctionArguments);
}

void PEGTransformerFactory::RegisterCheckpoint() {
	// checkpoint.gram
	REGISTER_TRANSFORM(TransformCheckpointStatement);
}

void PEGTransformerFactory::RegisterComment() {
	// comment.gram
	REGISTER_TRANSFORM(TransformCommentStatement);
	REGISTER_TRANSFORM(TransformCommentOnType);
	REGISTER_TRANSFORM(TransformCommentValue);
}

void PEGTransformerFactory::RegisterCommon() {
	// common.gram
	REGISTER_TRANSFORM(TransformNumberLiteral);
	REGISTER_TRANSFORM(TransformStringLiteral);
	REGISTER_TRANSFORM(TransformType);
	REGISTER_TRANSFORM(TransformArrayBounds);
	REGISTER_TRANSFORM(TransformArrayKeyword);
	REGISTER_TRANSFORM(TransformSquareBracketsArray);
	REGISTER_TRANSFORM(TransformTimeType);
	REGISTER_TRANSFORM(TransformTimeZone);
	REGISTER_TRANSFORM(TransformWithOrWithout);
	REGISTER_TRANSFORM(TransformTimeOrTimestamp);
	REGISTER_TRANSFORM(TransformNumericType);
	REGISTER_TRANSFORM(TransformSimpleNumericType);
	REGISTER_TRANSFORM(TransformDecimalNumericType);
	REGISTER_TRANSFORM(TransformFloatType);
	REGISTER_TRANSFORM(TransformDecimalType);
	REGISTER_TRANSFORM(TransformTypeModifiers);
	REGISTER_TRANSFORM(TransformSimpleType);
	REGISTER_TRANSFORM(TransformQualifiedTypeName);
	REGISTER_TRANSFORM(TransformCharacterType);
	REGISTER_TRANSFORM(TransformMapType);
	REGISTER_TRANSFORM(TransformRowType);
	REGISTER_TRANSFORM(TransformGeometryType);
	REGISTER_TRANSFORM(TransformVariantType);
	REGISTER_TRANSFORM(TransformUnionType);
	REGISTER_TRANSFORM(TransformColIdTypeList);
	REGISTER_TRANSFORM(TransformColIdType);
	REGISTER_TRANSFORM(TransformBitType);
	REGISTER_TRANSFORM(TransformIntervalType);
	REGISTER_TRANSFORM(TransformIntervalInterval);
	REGISTER_TRANSFORM(TransformInterval);
	REGISTER_TRANSFORM(TransformSetofType);
	Register("NumericModType", &TransformDecimalType);
	Register("DecType", &TransformDecimalType);
}

void PEGTransformerFactory::RegisterCopy() {
	// copy.gram
	REGISTER_TRANSFORM(TransformCopyStatement);
	REGISTER_TRANSFORM(TransformCopySelect);
	REGISTER_TRANSFORM(TransformCopyFromDatabase);
	REGISTER_TRANSFORM(TransformCopyDatabaseFlag);
	REGISTER_TRANSFORM(TransformCopyTable);
	REGISTER_TRANSFORM(TransformFromOrTo);
	REGISTER_TRANSFORM(TransformCopyFileName);
	REGISTER_TRANSFORM(TransformIdentifierColId);
	REGISTER_TRANSFORM(TransformCopyOptions);
	REGISTER_TRANSFORM(TransformSpecializedOptionList);
	REGISTER_TRANSFORM(TransformSpecializedOption);
	REGISTER_TRANSFORM(TransformSingleOption);
	REGISTER_TRANSFORM(TransformEncodingOption);
	REGISTER_TRANSFORM(TransformForceQuoteOption);
	REGISTER_TRANSFORM(TransformQuoteAsOption);
	REGISTER_TRANSFORM(TransformForceNullOption);
	REGISTER_TRANSFORM(TransformPartitionByOption);
	REGISTER_TRANSFORM(TransformNullAsOption);
	REGISTER_TRANSFORM(TransformDelimiterAsOption);
	REGISTER_TRANSFORM(TransformEscapeAsOption);
	REGISTER_TRANSFORM(TransformSchemaOrData);
}

void PEGTransformerFactory::RegisterCreateIndex() {
	// create_index.gram
	REGISTER_TRANSFORM(TransformCreateIndexStmt);
	REGISTER_TRANSFORM(TransformIndexType);
	REGISTER_TRANSFORM(TransformIndexElement);
	REGISTER_TRANSFORM(TransformWithList);
	REGISTER_TRANSFORM(TransformRelOptionOrOids);
	REGISTER_TRANSFORM(TransformRelOptionList);
	REGISTER_TRANSFORM(TransformOids);
	REGISTER_TRANSFORM(TransformRelOption);
	REGISTER_TRANSFORM(TransformIndexName);
}

void PEGTransformerFactory::RegisterCreateMacro() {
	// create_macro.gram
	REGISTER_TRANSFORM(TransformCreateMacroStmt);
	REGISTER_TRANSFORM(TransformMacroDefinition);
	REGISTER_TRANSFORM(TransformTableMacroDefinition);
	REGISTER_TRANSFORM(TransformScalarMacroDefinition);
	REGISTER_TRANSFORM(TransformMacroParameters);
	REGISTER_TRANSFORM(TransformMacroParameter);
	REGISTER_TRANSFORM(TransformSimpleParameter);
}

void PEGTransformerFactory::RegisterCreateSchema() {
	REGISTER_TRANSFORM(TransformCreateSchemaStmt);
}

void PEGTransformerFactory::RegisterCreateSecret() {
	// create_secret.gram
	REGISTER_TRANSFORM(TransformCreateSecretStmt);
	REGISTER_TRANSFORM(TransformSecretStorageSpecifier);
	REGISTER_TRANSFORM(TransformSecretName);
}

void PEGTransformerFactory::RegisterCreateSequence() {
	REGISTER_TRANSFORM(TransformCreateSequenceStmt);
	REGISTER_TRANSFORM(TransformSequenceOption);
	REGISTER_TRANSFORM(TransformSeqSetCycle);
	REGISTER_TRANSFORM(TransformSeqSetIncrement);
	REGISTER_TRANSFORM(TransformSeqSetMinMax);
	REGISTER_TRANSFORM(TransformSeqMinOrMax);
	REGISTER_TRANSFORM(TransformSeqNoMinMax);
	REGISTER_TRANSFORM(TransformSeqStartWith);
	REGISTER_TRANSFORM(TransformSeqOwnedBy);
}

void PEGTransformerFactory::RegisterCreateTable() {
	// create_table.gram
	REGISTER_TRANSFORM(TransformCreateStatement);
	REGISTER_TRANSFORM(TransformTemporary);
	REGISTER_TRANSFORM(TransformCreateStatementVariation);
	REGISTER_TRANSFORM(TransformCreateTableStmt);
	REGISTER_TRANSFORM(TransformCreateTableAs);
	REGISTER_TRANSFORM(TransformIdentifierList);
	REGISTER_TRANSFORM(TransformCreateColumnList);
	REGISTER_TRANSFORM(TransformCreateTableColumnList);
	REGISTER_TRANSFORM(TransformIdentifierOrStringLiteral);
	REGISTER_TRANSFORM(TransformColIdOrString);
	REGISTER_TRANSFORM(TransformColLabelOrString);
	REGISTER_TRANSFORM(TransformColId);
	REGISTER_TRANSFORM(TransformColumnIdList);
	REGISTER_TRANSFORM(TransformTypeFuncName);
	REGISTER_TRANSFORM(TransformIdentifier);
	REGISTER_TRANSFORM(TransformDottedIdentifier);
	REGISTER_TRANSFORM(TransformColumnDefinition);
	REGISTER_TRANSFORM(TransformTopLevelConstraint);
	REGISTER_TRANSFORM(TransformTopLevelConstraintList);
	REGISTER_TRANSFORM(TransformTopPrimaryKeyConstraint);
	REGISTER_TRANSFORM(TransformTopUniqueConstraint);
	REGISTER_TRANSFORM(TransformCheckConstraint);
	REGISTER_TRANSFORM(TransformTopForeignKeyConstraint);
	REGISTER_TRANSFORM(TransformForeignKeyConstraint);
	REGISTER_TRANSFORM(TransformDefaultValue);
	REGISTER_TRANSFORM(TransformGeneratedColumn);
	REGISTER_TRANSFORM(TransformColumnCompression);
	REGISTER_TRANSFORM(TransformPrimaryKeyConstraint);
	REGISTER_TRANSFORM(TransformUniqueConstraint);
	REGISTER_TRANSFORM(TransformNotNullConstraint);
	REGISTER_TRANSFORM(TransformKeyActions);
	REGISTER_TRANSFORM(TransformKeyAction);
	REGISTER_TRANSFORM(TransformNoKeyAction);
	REGISTER_TRANSFORM(TransformRestrictKeyAction);
	REGISTER_TRANSFORM(TransformCascadeKeyAction);
	REGISTER_TRANSFORM(TransformSetNullKeyAction);
	REGISTER_TRANSFORM(TransformSetDefaultKeyAction);
	REGISTER_TRANSFORM(TransformCubeOrRollup);

	REGISTER_TRANSFORM(TransformUpdateAction);
	REGISTER_TRANSFORM(TransformDeleteAction);
	REGISTER_TRANSFORM(TransformColumnCollation);
	REGISTER_TRANSFORM(TransformWithData);
	REGISTER_TRANSFORM(TransformCommitAction);
	REGISTER_TRANSFORM(TransformPreserveOrDelete);
	REGISTER_TRANSFORM(TransformGeneratedColumnType);
}

void PEGTransformerFactory::RegisterCreateType() {
	// create_type.gram
	REGISTER_TRANSFORM(TransformCreateTypeStmt);
	REGISTER_TRANSFORM(TransformCreateType);
	REGISTER_TRANSFORM(TransformEnumSelectType);
	REGISTER_TRANSFORM(TransformEnumStringLiteralList);
}

void PEGTransformerFactory::RegisterCreateView() {
	REGISTER_TRANSFORM(TransformCreateViewStmt);
}

void PEGTransformerFactory::RegisterDeallocate() {
	// deallocate.gram
	REGISTER_TRANSFORM(TransformDeallocateStatement);
}

void PEGTransformerFactory::RegisterDelete() {
	// delete.gram
	REGISTER_TRANSFORM(TransformDeleteStatement);
	REGISTER_TRANSFORM(TransformTargetOptAlias);
	REGISTER_TRANSFORM(TransformDeleteUsingClause);
	REGISTER_TRANSFORM(TransformTruncateStatement);
}

void PEGTransformerFactory::RegisterDescribe() {
	// describe.gram
	REGISTER_TRANSFORM(TransformDescribeStatement);
	REGISTER_TRANSFORM(TransformShowSelect);
	REGISTER_TRANSFORM(TransformShowTables);
	REGISTER_TRANSFORM(TransformShowAllTables);
	REGISTER_TRANSFORM(TransformShowQualifiedName);
	REGISTER_TRANSFORM(TransformShowOrDescribeOrSummarize);
	REGISTER_TRANSFORM(TransformShowOrDescribe);
	REGISTER_TRANSFORM(TransformSummarize);
}

void PEGTransformerFactory::RegisterDetach() {
	// detach.gram
	REGISTER_TRANSFORM(TransformDetachStatement);
}

void PEGTransformerFactory::RegisterDrop() {
	// drop.gram
	REGISTER_TRANSFORM(TransformDropStatement);
	REGISTER_TRANSFORM(TransformDropEntries);
	REGISTER_TRANSFORM(TransformDropTable);
	REGISTER_TRANSFORM(TransformTableOrView);
	REGISTER_TRANSFORM(TransformDropTableFunction);
	REGISTER_TRANSFORM(TransformDropFunction);
	REGISTER_TRANSFORM(TransformDropSchema);
	REGISTER_TRANSFORM(TransformQualifiedSchemaName);
	REGISTER_TRANSFORM(TransformDropIndex);
	REGISTER_TRANSFORM(TransformQualifiedIndexName);
	REGISTER_TRANSFORM(TransformDropSequence);
	REGISTER_TRANSFORM(TransformDropCollation);
	REGISTER_TRANSFORM(TransformDropType);
	REGISTER_TRANSFORM(TransformDropBehavior);
	REGISTER_TRANSFORM(TransformDropSecret);
	REGISTER_TRANSFORM(TransformDropSecretStorage);
}

void PEGTransformerFactory::RegisterExecute() {
	// execute.gram
	REGISTER_TRANSFORM(TransformExecuteStatement);
}

void PEGTransformerFactory::RegisterExplain() {
	// explain.gram
	REGISTER_TRANSFORM(TransformExplainStatement);
	REGISTER_TRANSFORM(TransformExplainableStatements);
}

void PEGTransformerFactory::RegisterExport() {
	REGISTER_TRANSFORM(TransformExportSource);
	REGISTER_TRANSFORM(TransformExportStatement);
}

void PEGTransformerFactory::RegisterExpression() {
	// expression.gram
	REGISTER_TRANSFORM(TransformBaseExpression);
	REGISTER_TRANSFORM(TransformExpression);
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

	REGISTER_TRANSFORM(TransformNestedColumnName);
	REGISTER_TRANSFORM(TransformColumnReference);
	REGISTER_TRANSFORM(TransformCatalogReservedSchemaTableColumnName);
	REGISTER_TRANSFORM(TransformSchemaReservedTableColumnName);
	REGISTER_TRANSFORM(TransformReservedTableQualification);

	REGISTER_TRANSFORM(TransformParameter);
	REGISTER_TRANSFORM(TransformAnonymousParameter);
	REGISTER_TRANSFORM(TransformColLabelParameter);
	REGISTER_TRANSFORM(TransformNumberedParameter);
	REGISTER_TRANSFORM(TransformPositionalExpression);

	REGISTER_TRANSFORM(TransformLiteralExpression);
	REGISTER_TRANSFORM(TransformParensExpression);
	REGISTER_TRANSFORM(TransformSingleExpression);
	REGISTER_TRANSFORM(TransformConstantLiteral);
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
	REGISTER_TRANSFORM(TransformOperator);
	REGISTER_TRANSFORM(TransformConjunctionOperator);
	REGISTER_TRANSFORM(TransformIsOperator);
	REGISTER_TRANSFORM(TransformInOperator);
	REGISTER_TRANSFORM(TransformBetweenOperator);
	REGISTER_TRANSFORM(TransformParenthesisExpression);
	REGISTER_TRANSFORM(TransformIndirection);
	REGISTER_TRANSFORM(TransformCastOperator);
	REGISTER_TRANSFORM(TransformDotOperator);
	REGISTER_TRANSFORM(TransformSliceExpression);
	REGISTER_TRANSFORM(TransformSliceBound);
	REGISTER_TRANSFORM(TransformEndSliceBound);
	REGISTER_TRANSFORM(TransformStepSliceBound);

	REGISTER_TRANSFORM(TransformTableReservedColumnName);
	REGISTER_TRANSFORM(TransformTableQualification);
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
	REGISTER_TRANSFORM(TransformTrimExpression);
	REGISTER_TRANSFORM(TransformTrimDirection);
	REGISTER_TRANSFORM(TransformTrimSource);
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

void PEGTransformerFactory::RegisterImport() {
	REGISTER_TRANSFORM(TransformImportStatement);
}

void PEGTransformerFactory::RegisterInsert() {
	// insert.gram
	REGISTER_TRANSFORM(TransformInsertStatement);
	REGISTER_TRANSFORM(TransformOrAction);
	REGISTER_TRANSFORM(TransformInsertTarget);
	REGISTER_TRANSFORM(TransformInsertAlias);
	REGISTER_TRANSFORM(TransformOnConflictClause);
	REGISTER_TRANSFORM(TransformOnConflictTarget);
	REGISTER_TRANSFORM(TransformOnConflictExpressionTarget);
	REGISTER_TRANSFORM(TransformOnConflictAction);
	REGISTER_TRANSFORM(TransformOnConflictUpdate);
	REGISTER_TRANSFORM(TransformOnConflictNothing);
	REGISTER_TRANSFORM(TransformInsertValues);
	REGISTER_TRANSFORM(TransformByNameOrPosition);
	REGISTER_TRANSFORM(TransformInsertColumnList);
	REGISTER_TRANSFORM(TransformColumnList);
	REGISTER_TRANSFORM(TransformReturningClause);
}

void PEGTransformerFactory::RegisterLoad() {
	// load.gram
	REGISTER_TRANSFORM(TransformLoadStatement);
	REGISTER_TRANSFORM(TransformInstallStatement);
	REGISTER_TRANSFORM(TransformFromSource);
	REGISTER_TRANSFORM(TransformVersionNumber);
}

void PEGTransformerFactory::RegisterMergeInto() {
	// merge_into.gram
	REGISTER_TRANSFORM(TransformMergeIntoStatement);
	REGISTER_TRANSFORM(TransformMergeIntoUsingClause);
	REGISTER_TRANSFORM(TransformMergeMatch);
	REGISTER_TRANSFORM(TransformMatchedClause);
	REGISTER_TRANSFORM(TransformMatchedClauseAction);
	REGISTER_TRANSFORM(TransformUpdateMatchClause);
	REGISTER_TRANSFORM(TransformUpdateMatchInfo);
	REGISTER_TRANSFORM(TransformDeleteMatchClause);
	REGISTER_TRANSFORM(TransformInsertMatchClause);
	REGISTER_TRANSFORM(TransformDoNothingMatchClause);
	REGISTER_TRANSFORM(TransformErrorMatchClause);
	REGISTER_TRANSFORM(TransformUpdateMatchSetClause);
	REGISTER_TRANSFORM(TransformAndExpression);
	REGISTER_TRANSFORM(TransformNotMatchedClause);
	REGISTER_TRANSFORM(TransformBySourceOrTarget);
	REGISTER_TRANSFORM(TransformInsertMatchInfo);
	REGISTER_TRANSFORM(TransformInsertDefaultValues);
	REGISTER_TRANSFORM(TransformInsertByNameOrPosition);
	REGISTER_TRANSFORM(TransformInsertValuesList);
}

void PEGTransformerFactory::RegisterPivot() {
	// pivot.gram
	REGISTER_TRANSFORM(TransformPivotStatement);
	REGISTER_TRANSFORM(TransformPivotUsing);
	REGISTER_TRANSFORM(TransformPivotOn);
	REGISTER_TRANSFORM(TransformPivotColumnList);
	REGISTER_TRANSFORM(TransformPivotColumnEntry);
	REGISTER_TRANSFORM(TransformPivotColumnSubquery);
	REGISTER_TRANSFORM(TransformPivotColumnEntryInternal);
	REGISTER_TRANSFORM(TransformUnpivotStatement);
	REGISTER_TRANSFORM(TransformIntoNameValues);

	REGISTER_TRANSFORM(TransformUnpivotHeader);
	REGISTER_TRANSFORM(TransformUnpivotHeaderSingle);
	REGISTER_TRANSFORM(TransformUnpivotHeaderList);
	REGISTER_TRANSFORM(TransformIncludeOrExcludeNulls);
}

void PEGTransformerFactory::RegisterPragma() {
	// pragma.gram
	REGISTER_TRANSFORM(TransformPragmaStatement);
	REGISTER_TRANSFORM(TransformPragmaAssign);
	REGISTER_TRANSFORM(TransformPragmaFunction);
	REGISTER_TRANSFORM(TransformPragmaParameters);
}

void PEGTransformerFactory::RegisterPrepare() {
	// prepare.gram
	REGISTER_TRANSFORM(TransformPrepareStatement);
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
	REGISTER_TRANSFORM(TransformSchemaQualification);
	REGISTER_TRANSFORM(TransformCatalogQualification);
	REGISTER_TRANSFORM(TransformQualifiedName);
	REGISTER_TRANSFORM(TransformCatalogReservedSchemaIdentifierOrStringLiteral);
	REGISTER_TRANSFORM(TransformCatalogReservedSchemaIdentifier);
	REGISTER_TRANSFORM(TransformSchemaReservedIdentifierOrStringLiteral);
	REGISTER_TRANSFORM(TransformReservedIdentifierOrStringLiteral);
	REGISTER_TRANSFORM(TransformTableNameIdentifierOrStringLiteral);
	REGISTER_TRANSFORM(TransformWhereClause);

	REGISTER_TRANSFORM(TransformTargetList);
	REGISTER_TRANSFORM(TransformAliasedExpression);
	REGISTER_TRANSFORM(TransformExpressionAsCollabel);
	REGISTER_TRANSFORM(TransformColIdExpression);
	REGISTER_TRANSFORM(TransformExpressionOptIdentifier);
	REGISTER_TRANSFORM(TransformTableAlias);
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

void PEGTransformerFactory::RegisterUse() {
	// use.gram
	REGISTER_TRANSFORM(TransformUseStatement);
	REGISTER_TRANSFORM(TransformUseTarget);
}

void PEGTransformerFactory::RegisterSet() {
	// set.gram
	REGISTER_TRANSFORM(TransformResetStatement);
	REGISTER_TRANSFORM(TransformSetAssignment);
	REGISTER_TRANSFORM(TransformSetSetting);
	REGISTER_TRANSFORM(TransformSetStatement);
	REGISTER_TRANSFORM(TransformSetTimeZone);
	REGISTER_TRANSFORM(TransformSetVariable);
	REGISTER_TRANSFORM(TransformStandardAssignment);
	REGISTER_TRANSFORM(TransformVariableList);
}

void PEGTransformerFactory::RegisterTransaction() {
	// transaction.gram
	REGISTER_TRANSFORM(TransformTransactionStatement);
	REGISTER_TRANSFORM(TransformBeginTransaction);
	REGISTER_TRANSFORM(TransformReadOrWrite);
	REGISTER_TRANSFORM(TransformReadOnlyOrReadWrite);
	REGISTER_TRANSFORM(TransformCommitTransaction);
	REGISTER_TRANSFORM(TransformRollbackTransaction);
}

void PEGTransformerFactory::RegisterUpdate() {
	// update.gram
	REGISTER_TRANSFORM(TransformUpdateStatement);
	REGISTER_TRANSFORM(TransformUpdateTarget);
	REGISTER_TRANSFORM(TransformBaseTableSet);
	REGISTER_TRANSFORM(TransformBaseTableAliasSet);
	REGISTER_TRANSFORM(TransformUpdateAlias);
	REGISTER_TRANSFORM(TransformUpdateSetClause);
	REGISTER_TRANSFORM(TransformUpdateSetTuple);
	REGISTER_TRANSFORM(TransformUpdateSetElementList);
	REGISTER_TRANSFORM(TransformUpdateSetElement);
}

void PEGTransformerFactory::RegisterVacuum() {
	REGISTER_TRANSFORM(TransformVacuumStatement);
	REGISTER_TRANSFORM(TransformVacuumOptions);
	REGISTER_TRANSFORM(TransformVacuumLegacyOptions);
	REGISTER_TRANSFORM(TransformVacuumParensOptions);
	REGISTER_TRANSFORM(TransformVacuumOption);
	REGISTER_TRANSFORM(TransformNameList);
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
	Register("ReservedSchemaQualification", &TransformSchemaQualification);
}

void PEGTransformerFactory::RegisterEnums() {
	RegisterEnum<SetScope>("LocalScope", SetScope::LOCAL);
	RegisterEnum<SetScope>("GlobalScope", SetScope::GLOBAL);
	RegisterEnum<SetScope>("SessionScope", SetScope::SESSION);
	RegisterEnum<SetScope>("VariableScope", SetScope::VARIABLE);

	RegisterEnum<Value>("FalseLiteral", Value(false));
	RegisterEnum<Value>("TrueLiteral", Value(true));
	RegisterEnum<Value>("NullLiteral", Value());
	RegisterEnum<Value>("UnknownLiteral", Value());

	RegisterEnum<TransactionModifierType>("ReadOnly", TransactionModifierType::TRANSACTION_READ_ONLY);
	RegisterEnum<TransactionModifierType>("ReadWrite", TransactionModifierType::TRANSACTION_READ_WRITE);

	RegisterEnum<CopyDatabaseType>("CopySchema", CopyDatabaseType::COPY_SCHEMA);
	RegisterEnum<CopyDatabaseType>("CopyData", CopyDatabaseType::COPY_DATA);

	RegisterEnum<string>("IntType", LogicalTypeIdToString(LogicalTypeId::INTEGER));
	RegisterEnum<string>("IntegerType", LogicalTypeIdToString(LogicalTypeId::INTEGER));
	RegisterEnum<string>("SmallintType", LogicalTypeIdToString(LogicalTypeId::SMALLINT));
	RegisterEnum<string>("BigintType", LogicalTypeIdToString(LogicalTypeId::BIGINT));
	RegisterEnum<string>("RealType", LogicalTypeIdToString(LogicalTypeId::FLOAT));
	RegisterEnum<string>("DoubleType", LogicalTypeIdToString(LogicalTypeId::DOUBLE));
	RegisterEnum<string>("BooleanType", LogicalTypeIdToString(LogicalTypeId::BOOLEAN));

	RegisterEnum<DatePartSpecifier>("YearKeyword", DatePartSpecifier::YEAR);
	RegisterEnum<DatePartSpecifier>("MonthKeyword", DatePartSpecifier::MONTH);
	RegisterEnum<DatePartSpecifier>("DayKeyword", DatePartSpecifier::DAY);
	RegisterEnum<DatePartSpecifier>("HourKeyword", DatePartSpecifier::HOUR);
	RegisterEnum<DatePartSpecifier>("MinuteKeyword", DatePartSpecifier::MINUTE);
	RegisterEnum<DatePartSpecifier>("SecondKeyword", DatePartSpecifier::SECOND);
	RegisterEnum<DatePartSpecifier>("MillisecondKeyword", DatePartSpecifier::MILLISECONDS);
	RegisterEnum<DatePartSpecifier>("MicrosecondKeyword", DatePartSpecifier::MICROSECONDS);
	RegisterEnum<DatePartSpecifier>("WeekKeyword", DatePartSpecifier::WEEK);
	RegisterEnum<DatePartSpecifier>("QuarterKeyword", DatePartSpecifier::QUARTER);
	RegisterEnum<DatePartSpecifier>("DecadeKeyword", DatePartSpecifier::DECADE);
	RegisterEnum<DatePartSpecifier>("CenturyKeyword", DatePartSpecifier::CENTURY);
	RegisterEnum<DatePartSpecifier>("MillenniumKeyword", DatePartSpecifier::MILLENNIUM);

	RegisterEnum<LogicalTypeId>("TimeTypeId", LogicalTypeId::TIME);
	RegisterEnum<LogicalTypeId>("TimestampTypeId", LogicalTypeId::TIMESTAMP);
	RegisterEnum<bool>("WithRule", true);
	RegisterEnum<bool>("WithoutRule", false);

	RegisterEnum<SecretPersistType>("TempPersistent", SecretPersistType::TEMPORARY);
	RegisterEnum<SecretPersistType>("TemporaryPersistent", SecretPersistType::TEMPORARY);
	RegisterEnum<SecretPersistType>("Persistent", SecretPersistType::PERSISTENT);

	RegisterEnum<CatalogType>("CommentTable", CatalogType::TABLE_ENTRY);
	RegisterEnum<CatalogType>("CommentSequence", CatalogType::SEQUENCE_ENTRY);
	RegisterEnum<CatalogType>("CommentFunction", CatalogType::MACRO_ENTRY);
	RegisterEnum<CatalogType>("CommentMacroTable", CatalogType::TABLE_MACRO_ENTRY);
	RegisterEnum<CatalogType>("CommentMacro", CatalogType::MACRO_ENTRY);
	RegisterEnum<CatalogType>("CommentView", CatalogType::VIEW_ENTRY);
	RegisterEnum<CatalogType>("MaterializedViewEntry", CatalogType::VIEW_ENTRY);
	RegisterEnum<CatalogType>("CommentDatabase", CatalogType::DATABASE_ENTRY);
	RegisterEnum<CatalogType>("CommentIndex", CatalogType::INDEX_ENTRY);
	RegisterEnum<CatalogType>("CommentSchema", CatalogType::SCHEMA_ENTRY);
	RegisterEnum<CatalogType>("CommentType", CatalogType::TYPE_ENTRY);
	RegisterEnum<CatalogType>("CommentColumn", CatalogType::INVALID);

	RegisterEnum<string>("MinValue", "minvalue");
	RegisterEnum<string>("MaxValue", "maxvalue");

	RegisterEnum<string>("MinusPrefixOperator", "-");
	RegisterEnum<string>("PlusPrefixOperator", "+");
	RegisterEnum<string>("TildePrefixOperator", "~");

	RegisterEnum<ShowType>("SummarizeRule", ShowType::SUMMARY);
	RegisterEnum<ShowType>("ShowRule", ShowType::DESCRIBE);
	RegisterEnum<ShowType>("DescribeRule", ShowType::DESCRIBE);

	RegisterEnum<InsertColumnOrder>("InsertByName", InsertColumnOrder::INSERT_BY_NAME);
	RegisterEnum<InsertColumnOrder>("InsertByPosition", InsertColumnOrder::INSERT_BY_POSITION);

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

	RegisterEnum<string>("OptAnalyze", "analyze");
	RegisterEnum<string>("OptFreeze", "freeze");
	RegisterEnum<string>("OptFull", "full");
	RegisterEnum<string>("OptVerbose", "verbose");

	RegisterEnum<MergeActionCondition>("BySource", MergeActionCondition::WHEN_NOT_MATCHED_BY_SOURCE);
	RegisterEnum<MergeActionCondition>("ByTarget", MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET);

	RegisterEnum<WindowExcludeMode>("ExcludeCurrentRow", WindowExcludeMode::CURRENT_ROW);
	RegisterEnum<WindowExcludeMode>("ExcludeGroup", WindowExcludeMode::GROUP);
	RegisterEnum<WindowExcludeMode>("ExcludeTies", WindowExcludeMode::TIES);
	RegisterEnum<WindowExcludeMode>("ExcludeNoOthers", WindowExcludeMode::NO_OTHER);

	RegisterEnum<GenericCopyOption>("BinaryOption", GenericCopyOption("format", Value("binary")));
	RegisterEnum<GenericCopyOption>("FreezeOption", GenericCopyOption("freeze", Value()));
	RegisterEnum<GenericCopyOption>("OidsOption", GenericCopyOption("oids", Value()));
	RegisterEnum<GenericCopyOption>("CsvOption", GenericCopyOption("format", Value("csv")));
	RegisterEnum<GenericCopyOption>("HeaderOption", GenericCopyOption("header", Value(true)));

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
	REGISTER_TRANSFORM(TransformStatement);
	RegisterAlter();
	RegisterAttach();
	RegisterAnalyze();
	RegisterCall();
	RegisterCheckpoint();
	RegisterComment();
	RegisterCommon();
	RegisterCopy();
	RegisterCreateIndex();
	RegisterCreateMacro();
	RegisterCreateSchema();
	RegisterCreateSequence();
	RegisterCreateSecret();
	RegisterCreateTable();
	RegisterCreateType();
	RegisterCreateView();
	RegisterDeallocate();
	RegisterDelete();
	RegisterDetach();
	RegisterDescribe();
	RegisterDrop();
	RegisterExecute();
	RegisterExplain();
	RegisterExport();
	RegisterExpression();
	RegisterImport();
	RegisterInsert();
	RegisterLoad();
	RegisterMergeInto();
	RegisterPivot();
	RegisterPragma();
	RegisterPrepare();
	RegisterSelect();
	RegisterUse();
	RegisterSet();
	RegisterTransaction();
	RegisterUpdate();
	RegisterVacuum();
	RegisterKeywordsAndIdentifiers();
	RegisterEnums();
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

optional_ptr<ParseResult> PEGTransformerFactory::ExtractResultFromParens(optional_ptr<ParseResult> parse_result) {
	// Parens(D) <- '(' D ')'
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.GetChild(1);
}

bool PEGTransformerFactory::ExpressionIsEmptyStar(ParsedExpression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::STAR) {
		return false;
	}
	auto &star = expr.Cast<StarExpression>();
	if (!star.columns && star.exclude_list.empty() && star.replace_list.empty()) {
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
		if (function.function_name == "struct_pack") {
			unordered_set<string> unique_names;
			child_list_t<Value> values;
			values.reserve(function.children.size());
			for (const auto &child : function.children) {
				if (!unique_names.insert(child->GetAlias()).second) {
					throw BinderException("Duplicate struct entry name \"%s\"", child->GetAlias());
				}
				Value child_value;
				if (!ConstructConstantFromExpression(*child, child_value)) {
					return false;
				}
				values.emplace_back(child->GetAlias(), std::move(child_value));
			}
			value = Value::STRUCT(std::move(values));
			return true;
		} else if (function.function_name == "list_value") {
			vector<Value> values;
			values.reserve(function.children.size());
			for (const auto &child : function.children) {
				Value child_value;
				if (!ConstructConstantFromExpression(*child, child_value)) {
					return false;
				}
				values.emplace_back(std::move(child_value));
			}

			// figure out child type
			LogicalType child_type(LogicalTypeId::SQLNULL);
			for (auto &child_value : values) {
				child_type = LogicalType::ForceMaxLogicalType(child_type, child_value.type());
			}

			// finally create the list
			value = Value::LIST(child_type, values);
			return true;
		} else if (function.function_name == "map") {
			Value keys;
			if (!ConstructConstantFromExpression(*function.children[0], keys)) {
				return false;
			}

			Value values;
			if (!ConstructConstantFromExpression(*function.children[1], values)) {
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
		value = constant.value;
		return true;
	}
	case ExpressionType::OPERATOR_CAST: {
		auto &cast = expr.Cast<CastExpression>();
		Value dummy_value;
		if (!ConstructConstantFromExpression(*cast.child, dummy_value)) {
			return false;
		}

		string error_message;
		if (!dummy_value.DefaultTryCastAs(cast.cast_type, value, &error_message)) {
			throw ConversionException("Unable to cast %s to %s", dummy_value.ToString(),
			                          EnumUtil::ToString(cast.cast_type.id()));
		}
		return true;
	}
	default:
		return false;
	}
}

} // namespace duckdb
