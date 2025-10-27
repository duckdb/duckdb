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
	// Printer::Print(token_stream);
	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator parse_result_allocator;
	MatchState state(tokens, suggestions, parse_result_allocator);
	MatcherAllocator allocator;
	auto &matcher = Matcher::RootMatcher(allocator);
	auto match_result = matcher.MatchParseResult(state);
	// Printer::Print(match_result->ToString());
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
	// Printer::PrintF("Final result %s", result->ToString());
	return transformer.Transform<unique_ptr<SQLStatement>>(match_result);
}

#define REGISTER_TRANSFORM(FUNCTION) Register(string(#FUNCTION).substr(9), &FUNCTION)

PEGTransformerFactory &PEGTransformerFactory::GetInstance() {
	static PEGTransformerFactory instance;
	return instance;
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

void PEGTransformerFactory::RegisterCreateTable() {
	// create_table.gram
	REGISTER_TRANSFORM(TransformIdentifierOrStringLiteral);
	REGISTER_TRANSFORM(TransformColIdOrString);
	REGISTER_TRANSFORM(TransformColLabelOrString);
	REGISTER_TRANSFORM(TransformColId);
	REGISTER_TRANSFORM(TransformIdentifier);
	REGISTER_TRANSFORM(TransformDottedIdentifier);
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
	RegisterAttach();
	RegisterCall();
	RegisterCheckpoint();
	RegisterCommon();
	RegisterCreateTable();
	RegisterDeallocate();
	RegisterDelete();
	RegisterDetach();
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

} // namespace duckdb
