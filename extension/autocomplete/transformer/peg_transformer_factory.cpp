#include "transformer/peg_transformer.hpp"
#include "matcher.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"

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
	REGISTER_TRANSFORM(TransformUnionType);
	REGISTER_TRANSFORM(TransformColIdTypeList);
	REGISTER_TRANSFORM(TransformColIdType);
	REGISTER_TRANSFORM(TransformBitType);
	REGISTER_TRANSFORM(TransformIntervalType);
	REGISTER_TRANSFORM(TransformIntervalInterval);
	REGISTER_TRANSFORM(TransformInterval);
}

void PEGTransformerFactory::RegisterCreateSequence() {
	REGISTER_TRANSFORM(TransformCreateSequenceStmt);
	REGISTER_TRANSFORM(TransformSequenceOption);
	REGISTER_TRANSFORM(TransformSeqSetCycle);
	REGISTER_TRANSFORM(TransformSeqSetIncrement);
	REGISTER_TRANSFORM(TransformSeqSetMinMax);
	REGISTER_TRANSFORM(TransformSeqMinOrMax);
	REGISTER_TRANSFORM(TransformNoMinMax);
	REGISTER_TRANSFORM(TransformSeqStartWith);
	REGISTER_TRANSFORM(TransformSeqOwnedBy);
}

void PEGTransformerFactory::RegisterCreateTable() {
	// create_table.gram
	REGISTER_TRANSFORM(TransformIdentifierOrStringLiteral);
	REGISTER_TRANSFORM(TransformColIdOrString);
	REGISTER_TRANSFORM(TransformColLabelOrString);
	REGISTER_TRANSFORM(TransformColId);
	REGISTER_TRANSFORM(TransformColumnIdList);
	REGISTER_TRANSFORM(TransformIdentifier);
	REGISTER_TRANSFORM(TransformDottedIdentifier);
	REGISTER_TRANSFORM(TransformColumnDefinition);
	REGISTER_TRANSFORM(TransformTypeOrGenerated);
	REGISTER_TRANSFORM(TransformTopLevelConstraint);
	REGISTER_TRANSFORM(TransformTopLevelConstraintList);
	REGISTER_TRANSFORM(TransformTopPrimaryKeyConstraint);
	REGISTER_TRANSFORM(TransformTopUniqueConstraint);
	REGISTER_TRANSFORM(TransformCheckConstraint);
	REGISTER_TRANSFORM(TransformTopForeignKeyConstraint);
	REGISTER_TRANSFORM(TransformDefaultValue);
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

void PEGTransformerFactory::RegisterExpression() {
	// expression.gram
	REGISTER_TRANSFORM(TransformBaseExpression);
	REGISTER_TRANSFORM(TransformExpression);
	REGISTER_TRANSFORM(TransformLogicalOrExpression);
	REGISTER_TRANSFORM(TransformLogicalAndExpression);
	REGISTER_TRANSFORM(TransformLogicalNotExpression);
	REGISTER_TRANSFORM(TransformIsExpression);
	REGISTER_TRANSFORM(TransformIsTest);
	REGISTER_TRANSFORM(TransformIsLiteral);
	REGISTER_TRANSFORM(TransformIsNotNull);
	REGISTER_TRANSFORM(TransformIsNull);
	REGISTER_TRANSFORM(TransformIsDistinctFromExpression);
	REGISTER_TRANSFORM(TransformBetweenInLikeExpression);
	REGISTER_TRANSFORM(TransformBetweenInLikeOp);
	REGISTER_TRANSFORM(TransformBetweenClause);
	REGISTER_TRANSFORM(TransformComparisonExpression);
	REGISTER_TRANSFORM(TransformComparisonOperator);
	REGISTER_TRANSFORM(TransformOtherOperatorExpression);
	REGISTER_TRANSFORM(TransformBitwiseExpression);
	REGISTER_TRANSFORM(TransformAdditiveExpression);
	REGISTER_TRANSFORM(TransformTerm);
	REGISTER_TRANSFORM(TransformMultiplicativeExpression);
	REGISTER_TRANSFORM(TransformFactor);
	REGISTER_TRANSFORM(TransformExponentiationExpression);
	REGISTER_TRANSFORM(TransformCollateExpression);
	REGISTER_TRANSFORM(TransformAtTimeZoneExpression);
	REGISTER_TRANSFORM(TransformPrefixExpression);

	REGISTER_TRANSFORM(TransformNestedColumnName);
	REGISTER_TRANSFORM(TransformColumnReference);
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
	REGISTER_TRANSFORM(TransformFunctionIdentifier);
	REGISTER_TRANSFORM(TransformSchemaReservedFunctionName);
	REGISTER_TRANSFORM(TransformCatalogReservedSchemaFunctionName);
	REGISTER_TRANSFORM(TransformOperator);
	REGISTER_TRANSFORM(TransformConjunctionOperator);
	REGISTER_TRANSFORM(TransformIsOperator);
	REGISTER_TRANSFORM(TransformInOperator);
	REGISTER_TRANSFORM(TransformLambdaOperator);
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
}

void PEGTransformerFactory::RegisterInsert() {
	// insert.gram
	REGISTER_TRANSFORM(TransformInsertStatement);
	REGISTER_TRANSFORM(TransformInsertTarget);
	REGISTER_TRANSFORM(TransformOnConflictClause);
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

void PEGTransformerFactory::RegisterSelect() {
	// select.gram
	REGISTER_TRANSFORM(TransformFunctionArgument);
	REGISTER_TRANSFORM(TransformBaseTableName);
	REGISTER_TRANSFORM(TransformSchemaReservedTable);
	REGISTER_TRANSFORM(TransformCatalogReservedSchemaTable);
	REGISTER_TRANSFORM(TransformSchemaQualification);
	REGISTER_TRANSFORM(TransformCatalogQualification);
	REGISTER_TRANSFORM(TransformQualifiedName);
	REGISTER_TRANSFORM(TransformCatalogReservedSchemaIdentifierOrStringLiteral);
	REGISTER_TRANSFORM(TransformSchemaReservedIdentifierOrStringLiteral);
	REGISTER_TRANSFORM(TransformReservedIdentifierOrStringLiteral);
	REGISTER_TRANSFORM(TransformTableNameIdentifierOrStringLiteral);
	REGISTER_TRANSFORM(TransformWhereClause);

	REGISTER_TRANSFORM(TransformTargetList);
	REGISTER_TRANSFORM(TransformAliasedExpression);
	REGISTER_TRANSFORM(TransformExpressionAsCollabel);
	REGISTER_TRANSFORM(TransformColIdExpression);
	REGISTER_TRANSFORM(TransformExpressionOptIdentifier);
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
	REGISTER_TRANSFORM(TransformCommitTransaction);
	REGISTER_TRANSFORM(TransformRollbackTransaction);
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

	RegisterEnum<LogicalType>("IntType", LogicalType(LogicalTypeId::INTEGER));
	RegisterEnum<LogicalType>("IntegerType", LogicalType(LogicalTypeId::INTEGER));
	RegisterEnum<LogicalType>("SmallintType", LogicalType(LogicalTypeId::SMALLINT));
	RegisterEnum<LogicalType>("BigintType", LogicalType(LogicalTypeId::BIGINT));
	RegisterEnum<LogicalType>("RealType", LogicalType(LogicalTypeId::FLOAT));
	RegisterEnum<LogicalType>("DoubleType", LogicalType(LogicalTypeId::DOUBLE));
	RegisterEnum<LogicalType>("BooleanType", LogicalType(LogicalTypeId::BOOLEAN));

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
	RegisterEnum<ShowType>("ShowRule", ShowType::SHOW_FROM);
	RegisterEnum<ShowType>("DescribeRule", ShowType::DESCRIBE);
	RegisterEnum<ShowType>("DescRule", ShowType::DESCRIBE);

	RegisterEnum<InsertColumnOrder>("InsertByName", InsertColumnOrder::INSERT_BY_NAME);
	RegisterEnum<InsertColumnOrder>("InsertByPosition", InsertColumnOrder::INSERT_BY_POSITION);

	RegisterEnum<OrderType>("DescendingOrder", OrderType::DESCENDING);
	RegisterEnum<OrderType>("AscendingOrder", OrderType::ASCENDING);
	RegisterEnum<OrderByNullType>("NullsFirst", OrderByNullType::NULLS_FIRST);
	RegisterEnum<OrderByNullType>("NullsLast", OrderByNullType::NULLS_LAST);

	RegisterEnum<ExpressionType>("ConjunctionOr", ExpressionType::CONJUNCTION_OR);
	RegisterEnum<ExpressionType>("ConjunctionAnd", ExpressionType::CONJUNCTION_AND);

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
}

PEGTransformerFactory::PEGTransformerFactory() {
	REGISTER_TRANSFORM(TransformStatement);
	RegisterAlter();
	RegisterAttach();
	RegisterCall();
	RegisterCheckpoint();
	RegisterComment();
	RegisterCommon();
	RegisterCreateSequence();
	RegisterCreateTable();
	RegisterDeallocate();
	RegisterDelete();
	RegisterDetach();
	RegisterDrop();
	RegisterExpression();
	RegisterInsert();
	RegisterLoad();
	RegisterSelect();
	RegisterUse();
	RegisterSet();
	RegisterTransaction();
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

} // namespace duckdb
