#pragma once

#include "tokenizer.hpp"
#include "parse_result.hpp"
#include "transform_enum_result.hpp"
#include "transform_result.hpp"
#include "ast/setting_info.hpp"
#include "duckdb/function/macro_function.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"
#include "duckdb/parser/statement/copy_database_statement.hpp"
#include "duckdb/parser/statement/set_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "parser/peg_parser.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"

namespace duckdb {

// Forward declare
struct QualifiedName;
struct MatcherToken;

struct PEGTransformerState {
	explicit PEGTransformerState(const vector<MatcherToken> &tokens_p) : tokens(tokens_p), token_index(0) {
	}

	const vector<MatcherToken> &tokens;
	idx_t token_index;
};

class PEGTransformer {
public:
	using AnyTransformFunction =
	    std::function<unique_ptr<TransformResultValue>(PEGTransformer &, optional_ptr<ParseResult>)>;

	PEGTransformer(ArenaAllocator &allocator, PEGTransformerState &state,
	               const case_insensitive_map_t<AnyTransformFunction> &transform_functions,
	               const case_insensitive_map_t<PEGRule> &grammar_rules,
	               const case_insensitive_map_t<unique_ptr<TransformEnumValue>> &enum_mappings)
	    : allocator(allocator), state(state), grammar_rules(grammar_rules), transform_functions(transform_functions),
	      enum_mappings(enum_mappings) {
	}

public:
	template <typename T>
	T Transform(optional_ptr<ParseResult> parse_result) {
		auto it = transform_functions.find(parse_result->name);
		if (it == transform_functions.end()) {
			throw NotImplementedException("No transformer function found for rule '%s'", parse_result->name);
		}
		auto &func = it->second;

		unique_ptr<TransformResultValue> base_result = func(*this, parse_result);
		if (!base_result) {
			throw InternalException("Transformer for rule '%s' returned a nullptr.", parse_result->name);
		}

		auto *typed_result_ptr = dynamic_cast<TypedTransformResult<T> *>(base_result.get());
		if (!typed_result_ptr) {
			throw InternalException("Transformer for rule '" + parse_result->name + "' returned an unexpected type.");
		}

		return std::move(typed_result_ptr->value);
	}

	template <typename T>
	T Transform(ListParseResult &parse_result, idx_t child_index) {
		auto child_parse_result = parse_result.GetChild(child_index);
		return Transform<T>(child_parse_result);
	}

	template <typename T>
	T TransformEnum(optional_ptr<ParseResult> parse_result) {
		auto enum_rule_name = parse_result->name;

		auto rule_value = enum_mappings.find(enum_rule_name);
		if (rule_value == enum_mappings.end()) {
			throw ParserException("Enum transform failed: could not find mapping for '%s'", enum_rule_name);
		}

		auto *typed_enum_ptr = dynamic_cast<TypedTransformEnumResult<T> *>(rule_value->second.get());
		if (!typed_enum_ptr) {
			throw InternalException("Enum mapping for rule '%s' has an unexpected type.", enum_rule_name);
		}

		return typed_enum_ptr->value;
	}

	template <typename T>
	void TransformOptional(ListParseResult &list_pr, idx_t child_idx, T &target) {
		auto &opt = list_pr.Child<OptionalParseResult>(child_idx);
		if (opt.HasResult()) {
			target = Transform<T>(opt.optional_result);
		}
	}

	// Make overloads return raw pointers, as ownership is handled by the ArenaAllocator.
	template <class T, typename... Args>
	T *Make(Args &&...args) {
		return allocator.Make<T>(std::forward<Args>(args)...);
	}

	void ClearParameters();
	static void ParamTypeCheck(PreparedParamType last_type, PreparedParamType new_type);
	void SetParam(const string &name, idx_t index, PreparedParamType type);
	bool GetParam(const string &name, idx_t &index, PreparedParamType type);

public:
	ArenaAllocator &allocator;
	PEGTransformerState &state;
	const case_insensitive_map_t<PEGRule> &grammar_rules;
	const case_insensitive_map_t<AnyTransformFunction> &transform_functions;
	const case_insensitive_map_t<unique_ptr<TransformEnumValue>> &enum_mappings;
	case_insensitive_map_t<idx_t> named_parameter_map;
	idx_t prepared_statement_parameter_index = 0;
	PreparedParamType last_param_type = PreparedParamType::INVALID;
};

class PEGTransformerFactory {
public:
	static PEGTransformerFactory &GetInstance();
	explicit PEGTransformerFactory();
	static unique_ptr<SQLStatement> Transform(vector<MatcherToken> &tokens, const char *root_rule = "Statement");

private:
	template <typename T>
	void RegisterEnum(const string &rule_name, T value) {
		auto existing_rule = enum_mappings.find(rule_name);
		if (existing_rule != enum_mappings.end()) {
			throw InternalException("EnumRule %s already exists", rule_name);
		}
		enum_mappings[rule_name] = make_uniq<TypedTransformEnumResult<T>>(value);
	}

	template <class FUNC>
	void Register(const string &rule_name, FUNC function) {
		auto existing_rule = sql_transform_functions.find(rule_name);
		if (existing_rule != sql_transform_functions.end()) {
			throw InternalException("Rule %s already exists", rule_name);
		}
		sql_transform_functions[rule_name] =
		    [function](PEGTransformer &transformer,
		               optional_ptr<ParseResult> parse_result) -> unique_ptr<TransformResultValue> {
			auto result_value = function(transformer, parse_result);
			return make_uniq<TypedTransformResult<decltype(result_value)>>(std::move(result_value));
		};
	}

	PEGTransformerFactory(const PEGTransformerFactory &) = delete;

	static unique_ptr<SQLStatement> TransformStatement(PEGTransformer &, optional_ptr<ParseResult> list);

	// common.gram
	// static unique_ptr<SQLStatement> TransformArrayBounds(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformBitType(PEGTransformer &transformer,
	// optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement> TransformCatalogName(PEGTransformer
	// &transformer, optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement>
	// TransformCenturyKeyword(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result); static
	// unique_ptr<SQLStatement> TransformCharacterType(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformColIdType(PEGTransformer &transformer,
	// optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement> TransformCollationName(PEGTransformer
	// &transformer, optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement>
	// TransformColumnName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result); static
	// unique_ptr<SQLStatement> TransformConstraintName(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformCopyOptionName(PEGTransformer &transformer,
	// optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement> TransformDayKeyword(PEGTransformer
	// &transformer, optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement>
	// TransformDecadeKeyword(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result); static
	// unique_ptr<SQLStatement> TransformFunctionName(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformHourKeyword(PEGTransformer &transformer,
	// optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement> TransformIndexName(PEGTransformer
	// &transformer, optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement>
	// TransformInterval(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result); static
	// unique_ptr<SQLStatement> TransformIntervalType(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformMapType(PEGTransformer &transformer,
	// optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement>
	// TransformMicrosecondKeyword(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result); static
	// unique_ptr<SQLStatement> TransformMillenniumKeyword(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformMillisecondKeyword(PEGTransformer &transformer,
	// optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement> TransformMinuteKeyword(PEGTransformer
	// &transformer, optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement>
	// TransformMonthKeyword(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformNumberLiteral(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformNumericType(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformPragmaName(PEGTransformer &transformer,
	// optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement>
	// TransformQualifiedTypeName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result); static
	// unique_ptr<SQLStatement> TransformQuarterKeyword(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformReservedColumnName(PEGTransformer &transformer,
	// optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement>
	// TransformReservedFunctionName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result); static
	// unique_ptr<SQLStatement> TransformReservedIdentifier(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformReservedSchemaName(PEGTransformer &transformer,
	// optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement>
	// TransformReservedTableName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result); static
	// unique_ptr<SQLStatement> TransformRowOrStruct(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformRowType(PEGTransformer &transformer,
	// optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement> TransformSchemaName(PEGTransformer
	// &transformer, optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement>
	// TransformSecondKeyword(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result); static
	// unique_ptr<SQLStatement> TransformSecretName(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformSequenceName(PEGTransformer &transformer,
	// optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement> TransformSetofType(PEGTransformer
	// &transformer, optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement>
	// TransformSettingName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result); static
	// unique_ptr<SQLStatement> TransformSimpleType(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformStringLiteral(PEGTransformer &transformer,
	// optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement>
	// TransformTableFunctionName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result); static
	// unique_ptr<SQLStatement> TransformTableName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformTimeOrTimestamp(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformTimeType(PEGTransformer &transformer,
	// optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement> TransformTimeZone(PEGTransformer
	// &transformer, optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement>
	// TransformType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result); static
	// unique_ptr<SQLStatement> TransformTypeModifiers(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformUnionType(PEGTransformer &transformer,
	// optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement> TransformWeekKeyword(PEGTransformer
	// &transformer, optional_ptr<ParseResult> parse_result); static unique_ptr<SQLStatement>
	// TransformWithOrWithout(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result); static
	// unique_ptr<SQLStatement> TransformYearKeyword(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result);

	// expression.gram
	// static unique_ptr<SQLStatement> TransformAnyAllOperator(PEGTransformer &transformer,
	//                                                         optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformAnyOrAll(PEGTransformer &transformer,
	//                                                   optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformAtTimeZoneOperator(PEGTransformer &transformer,
	//                                                             optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformBaseExpression(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformBaseWindowName(PEGTransformer &transformer,
	//                                                         optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformBetweenOperator(PEGTransformer &transformer,
	//                                                          optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformBoundedListExpression(PEGTransformer &transformer,
	//                                                                optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformCaseElse(PEGTransformer &transformer,
	//                                                   optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformCaseExpression(PEGTransformer &transformer,
	//                                                         optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformCaseWhenThen(PEGTransformer &transformer,
	//                                                       optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformCastExpression(PEGTransformer &transformer,
	//                                                         optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformCastOperator(PEGTransformer &transformer,
	//                                                       optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformCastOrTryCast(PEGTransformer &transformer,
	//                                                        optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformCatalogReservedSchemaFunctionName(PEGTransformer &transformer,
	//                                                                            optional_ptr<ParseResult>
	//                                                                            parse_result);
	// static unique_ptr<SQLStatement>
	// TransformCatalogReservedSchemaTableColumnName(PEGTransformer &transformer, optional_ptr<ParseResult>
	// parse_result); static unique_ptr<SQLStatement> TransformCoalesceExpression(PEGTransformer &transformer,
	//                                                             optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformColLabelParameter(PEGTransformer &transformer,
	//                                                            optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformCollateOperator(PEGTransformer &transformer,
	//                                                          optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformColumnReference(PEGTransformer &transformer,
	//                                                          optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformColumnsExpression(PEGTransformer &transformer,
	//                                                            optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformComparisonOperator(PEGTransformer &transformer,
	//                                                             optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformConjunctionOperator(PEGTransformer &transformer,
	//                                                              optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformDefaultExpression(PEGTransformer &transformer,
	//                                                            optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformDistinctFrom(PEGTransformer &transformer,
	//                                                       optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformDistinctOrAll(PEGTransformer &transformer,
	//                                                        optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformDotOperator(PEGTransformer &transformer,
	//                                                      optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformEscapeOperator(PEGTransformer &transformer,
	//                                                         optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformExcludeList(PEGTransformer &transformer,
	//                                                      optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformExcludeName(PEGTransformer &transformer,
	//                                                      optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformExportClause(PEGTransformer &transformer,
	//                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformExpression(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformExtractExpression(PEGTransformer &transformer,
	//                                                            optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformFilterClause(PEGTransformer &transformer,
	//                                                       optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformFrameBound(PEGTransformer &transformer,
	//                                                     optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformFrameClause(PEGTransformer &transformer,
	//                                                      optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformFrameExtent(PEGTransformer &transformer,
	//                                                      optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformFraming(PEGTransformer &transformer,
	//                                                  optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformFunctionExpression(PEGTransformer &transformer,
	//                                                             optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformFunctionIdentifier(PEGTransformer &transformer,
	//                                                             optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformGroupingExpression(PEGTransformer &transformer,
	//                                                             optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformGroupingOrGroupingId(PEGTransformer &transformer,
	//                                                               optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformIgnoreNulls(PEGTransformer &transformer,
	//                                                      optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformInOperator(PEGTransformer &transformer,
	//                                                     optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformIndirection(PEGTransformer &transformer,
	//                                                      optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformIntervalLiteral(PEGTransformer &transformer,
	//                                                          optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformIntervalParameter(PEGTransformer &transformer,
	//                                                            optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformIntervalUnit(PEGTransformer &transformer,
	//                                                       optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformIsOperator(PEGTransformer &transformer,
	//                                                     optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformLambdaExpression(PEGTransformer &transformer,
	//                                                           optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformLambdaOperator(PEGTransformer &transformer,
	//                                                         optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformLikeOperator(PEGTransformer &transformer,
	//                                                       optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformLikeOrSimilarTo(PEGTransformer &transformer,
	//                                                          optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformListComprehensionExpression(PEGTransformer &transformer,
	//                                                                      optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformListComprehensionFilter(PEGTransformer &transformer,
	//                                                                  optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformListExpression(PEGTransformer &transformer,
	//                                                         optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformLiteralExpression(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformMapExpression(PEGTransformer &transformer,
	//                                                        optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformNotNull(PEGTransformer &transformer,
	//                                                  optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformNullIfExpression(PEGTransformer &transformer,
	//                                                           optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformNumberedParameter(PEGTransformer &transformer,
	//                                                            optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformOperator(PEGTransformer &transformer,
	//                                                   optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformOperatorLiteral(PEGTransformer &transformer,
	//                                                          optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformOverClause(PEGTransformer &transformer,
	//                                                     optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformParameter(PEGTransformer &transformer,
	//                                                    optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformParenthesisExpression(PEGTransformer &transformer,
	//                                                                optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformPositionExpression(PEGTransformer &transformer,
	//                                                             optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformPositionalExpression(PEGTransformer &transformer,
	//                                                               optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformPostfixOperator(PEGTransformer &transformer,
	//                                                          optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformPrefixExpression(PEGTransformer &transformer,
	//                                                           optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformPrefixOperator(PEGTransformer &transformer,
	//                                                         optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformRecursiveExpression(PEGTransformer &transformer,
	//                                                              optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformRenameEntry(PEGTransformer &transformer,
	//                                                      optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformRenameList(PEGTransformer &transformer,
	//                                                     optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformReplaceEntry(PEGTransformer &transformer,
	//                                                       optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformReplaceList(PEGTransformer &transformer,
	//                                                      optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformRowExpression(PEGTransformer &transformer,
	//                                                        optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformSchemaReservedFunctionName(PEGTransformer &transformer,
	//                                                                     optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformSchemaReservedTableColumnName(PEGTransformer &transformer,
	//                                                                        optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformSingleExpression(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformSliceBound(PEGTransformer &transformer,
	//                                                     optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformSliceExpression(PEGTransformer &transformer,
	//                                                          optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformSpecialFunctionExpression(PEGTransformer &transformer,
	//                                                                    optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformStarExpression(PEGTransformer &transformer,
	//                                                         optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformStructExpression(PEGTransformer &transformer,
	//                                                           optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformStructField(PEGTransformer &transformer,
	//                                                      optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformSubqueryExpression(PEGTransformer &transformer,
	//                                                             optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformSubstringExpression(PEGTransformer &transformer,
	//                                                              optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformSubstringParameters(PEGTransformer &transformer,
	//                                                              optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformTableReservedColumnName(PEGTransformer &transformer,
	//                                                                  optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformTrimDirection(PEGTransformer &transformer,
	//                                                        optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformTrimExpression(PEGTransformer &transformer,
	//                                                         optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformTrimSource(PEGTransformer &transformer,
	//                                                     optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformTypeLiteral(PEGTransformer &transformer,
	//                                                      optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformUnpackExpression(PEGTransformer &transformer,
	//                                                           optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformWindowExcludeClause(PEGTransformer &transformer,
	//                                                              optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformWindowExcludeElement(PEGTransformer &transformer,
	//                                                               optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformWindowFrame(PEGTransformer &transformer,
	//                                                      optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformWindowFrameContents(PEGTransformer &transformer,
	//                                                              optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformWindowFrameDefinition(PEGTransformer &transformer,
	//                                                                optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformWindowPartition(PEGTransformer &transformer,
	//                                                          optional_ptr<ParseResult> parse_result);
	// static unique_ptr<SQLStatement> TransformWithinGroupClause(PEGTransformer &transformer,
	//                                                            optional_ptr<ParseResult> parse_result);

	// use.gram
	static unique_ptr<SQLStatement> TransformUseStatement(PEGTransformer &transformer,
	                                                      optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformUseTarget(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// set.gram
	static unique_ptr<SQLStatement> TransformResetStatement(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformSetAssignment(PEGTransformer &transformer,
	                                                                   optional_ptr<ParseResult> parse_result);
	static SettingInfo TransformSetSetting(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformSetStatement(PEGTransformer &transformer,
	                                                      optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformSetTimeZone(PEGTransformer &transformer,
	                                                     optional_ptr<ParseResult> parse_result);
	static SettingInfo TransformSetVariable(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformStandardAssignment(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformVariableList(PEGTransformer &transformer,
	                                                                  optional_ptr<ParseResult> parse_result);

	//! Helper functions
	static vector<optional_ptr<ParseResult>> ExtractParseResultsFromList(optional_ptr<ParseResult> parse_result);

private:
	PEGParser parser;
	case_insensitive_map_t<PEGTransformer::AnyTransformFunction> sql_transform_functions;
	case_insensitive_map_t<unique_ptr<TransformEnumValue>> enum_mappings;
};

} // namespace duckdb
