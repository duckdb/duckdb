#pragma once

#include "tokenizer.hpp"
#include "parse_result.hpp"
#include "transform_enum_result.hpp"
#include "transform_result.hpp"
#include "ast/extension_repository_info.hpp"
#include "ast/generic_copy_option.hpp"
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
	static optional_ptr<ParseResult> ExtractResultFromParens(optional_ptr<ParseResult> parse_result);
	static bool ExpressionIsEmptyStar(ParsedExpression &expr);

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

	// attach.gram
	static unique_ptr<SQLStatement> TransformAttachStatement(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static string TransformAttachAlias(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static vector<GenericCopyOption> TransformAttachOptions(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static vector<GenericCopyOption> TransformGenericCopyOptionList(PEGTransformer &transformer,
	                                                                optional_ptr<ParseResult> parse_result);
	static GenericCopyOption TransformGenericCopyOption(PEGTransformer &transformer,
	                                                    optional_ptr<ParseResult> parse_result);

	// call.gram
	static unique_ptr<SQLStatement> TransformCallStatement(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);

	// checkpoint.gram
	static unique_ptr<SQLStatement> TransformCheckpointStatement(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);

	// common.gram
	static unique_ptr<ParsedExpression> TransformNumberLiteral(PEGTransformer &transformer,
	                                                           optional_ptr<ParseResult> parse_result);
	static string TransformStringLiteral(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// deallocate.gram
	static unique_ptr<SQLStatement> TransformDeallocateStatement(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);

	// delete.gram
	static unique_ptr<SQLStatement> TransformDeleteStatement(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static unique_ptr<BaseTableRef> TransformTargetOptAlias(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);

	static vector<unique_ptr<TableRef>> TransformDeleteUsingClause(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformTruncateStatement(PEGTransformer &transformer,
	                                                           optional_ptr<ParseResult> parse_result);

	// expression.gram
	static unique_ptr<ParsedExpression> TransformBaseExpression(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformExpression(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformRecursiveExpression(PEGTransformer &transformer,
	                                                                 optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformConstantLiteral(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static unique_ptr<ColumnRefExpression> TransformNestedColumnName(PEGTransformer &transformer,
	                                                                 optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformColumnReference(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformLiteralExpression(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformSingleExpression(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);

	static unique_ptr<ParsedExpression> TransformPrefixExpression(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static string TransformPrefixOperator(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformListExpression(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformArrayBoundedListExpression(PEGTransformer &transformer,
	                                                                        optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformArrayParensSelect(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformStructExpression(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformStructField(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformBoundedListExpression(PEGTransformer &transformer,
	                                                                           optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformFunctionExpression(PEGTransformer &transformer,
	                                                                optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformFunctionIdentifier(PEGTransformer &transformer,
	                                                 optional_ptr<ParseResult> parse_result);

	static ExpressionType TransformOperator(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static ExpressionType TransformConjunctionOperator(PEGTransformer &transformer,
	                                                   optional_ptr<ParseResult> parse_result);
	static ExpressionType TransformIsOperator(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static ExpressionType TransformInOperator(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static ExpressionType TransformLambdaOperator(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static ExpressionType TransformBetweenOperator(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// create_table.gram
	static string TransformIdentifierOrStringLiteral(PEGTransformer &transformer,
	                                                 optional_ptr<ParseResult> parse_result);
	static string TransformColIdOrString(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static string TransformColId(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static string TransformIdentifier(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static vector<string> TransformDottedIdentifier(PEGTransformer &transformer,
	                                                optional_ptr<ParseResult> parse_result);

	// detach.gram
	static unique_ptr<SQLStatement> TransformDetachStatement(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);

	// load.gram
	static unique_ptr<SQLStatement> TransformLoadStatement(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformInstallStatement(PEGTransformer &transformer,
	                                                          optional_ptr<ParseResult> parse_result);
	static ExtensionRepositoryInfo TransformFromSource(PEGTransformer &transformer,
	                                                   optional_ptr<ParseResult> parse_result);
	static string TransformVersionNumber(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// select.gram
	static unique_ptr<ParsedExpression> TransformFunctionArgument(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformNamedParameter(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformTableFunctionArguments(PEGTransformer &transformer,
	                                                                            optional_ptr<ParseResult> parse_result);

	static unique_ptr<BaseTableRef> TransformBaseTableName(PEGTransformer &transformer,
																		   optional_ptr<ParseResult> parse_result);
	static unique_ptr<BaseTableRef> TransformSchemaReservedTable(PEGTransformer &transformer,
																				 optional_ptr<ParseResult> parse_result);
	static unique_ptr<BaseTableRef>
		TransformCatalogReservedSchemaTable(PEGTransformer &transformer,
																   optional_ptr<ParseResult> parse_result);
	static string TransformSchemaQualification(PEGTransformer &transformer,
															   optional_ptr<ParseResult> parse_result);
	static string TransformCatalogQualification(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformQualifiedName(PEGTransformer &transformer,
																optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformCatalogReservedSchemaIdentifierOrStringLiteral(PEGTransformer &transformer,
																				  optional_ptr<ParseResult> parse_result);

	static QualifiedName TransformSchemaReservedIdentifierOrStringLiteral(PEGTransformer &transformer,
																				optional_ptr<ParseResult> parse_result);
	static string TransformReservedIdentifierOrStringLiteral(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformTableNameIdentifierOrStringLiteral(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

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
	static unique_ptr<SetVariableStatement> TransformStandardAssignment(PEGTransformer &transformer,
	                                                                    optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformVariableList(PEGTransformer &transformer,
	                                                                  optional_ptr<ParseResult> parse_result);

	static string TransformIdentifierOrKeyword(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// transaction.gram
	static unique_ptr<SQLStatement> TransformTransactionStatement(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static unique_ptr<TransactionStatement> TransformBeginTransaction(PEGTransformer &transformer,
	                                                                  optional_ptr<ParseResult> parse_result);
	static TransactionModifierType TransformReadOrWrite(PEGTransformer &transformer,
	                                                    optional_ptr<ParseResult> parse_result);
	static unique_ptr<TransactionStatement> TransformCommitTransaction(PEGTransformer &, optional_ptr<ParseResult>);
	static unique_ptr<TransactionStatement> TransformRollbackTransaction(PEGTransformer &, optional_ptr<ParseResult>);

	//! Helper functions
	static vector<optional_ptr<ParseResult>> ExtractParseResultsFromList(optional_ptr<ParseResult> parse_result);

private:
	PEGParser parser;
	case_insensitive_map_t<PEGTransformer::AnyTransformFunction> sql_transform_functions;
	case_insensitive_map_t<unique_ptr<TransformEnumValue>> enum_mappings;
};

} // namespace duckdb
