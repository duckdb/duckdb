#pragma once

#include "duckdb/parser/peg/ast/unpivot_name_values.hpp"
#include "duckdb/parser/peg/transformer/parse_result.hpp"
#include "duckdb/parser/peg/transformer/transform_result.hpp"
#include "duckdb/parser/peg/ast/add_column_entry.hpp"
#include "duckdb/parser/peg/ast/column_constraint_entry.hpp"
#include "duckdb/parser/peg/ast/analyze_target.hpp"
#include "duckdb/parser/peg/ast/column_elements.hpp"
#include "duckdb/parser/peg/ast/create_table_column_element.hpp"
#include "duckdb/parser/peg/ast/create_table_definition.hpp"
#include "duckdb/parser/peg/ast/partition_sorted_options.hpp"
#include "duckdb/parser/peg/ast/distinct_clause.hpp"
#include "duckdb/parser/peg/ast/describe_target.hpp"
#include "duckdb/parser/peg/ast/extension_repository_info.hpp"
#include "duckdb/parser/peg/ast/generated_column_definition.hpp"
#include "duckdb/parser/peg/ast/generic_copy_option.hpp"
#include "duckdb/parser/peg/ast/generic_copy_option_value.hpp"
#include "duckdb/parser/peg/ast/insert_values.hpp"
#include "duckdb/parser/peg/ast/create_pivot_entry.hpp"
#include "duckdb/parser/peg/ast/join_prefix.hpp"
#include "duckdb/parser/peg/ast/join_qualifier.hpp"
#include "duckdb/parser/peg/ast/key_actions.hpp"
#include "duckdb/parser/peg/ast/limit_percent_result.hpp"
#include "duckdb/parser/peg/ast/macro_parameter.hpp"
#include "duckdb/parser/peg/ast/on_conflict_expression_target.hpp"
#include "duckdb/parser/peg/ast/sequence_option.hpp"
#include "duckdb/parser/peg/ast/setting_info.hpp"
#include "duckdb/parser/peg/ast/table_alias.hpp"
#include "duckdb/parser/peg/ast/cast_arguments.hpp"
#include "duckdb/parser/peg/ast/expression_chain.hpp"
#include "duckdb/parser/peg/ast/method_arguments.hpp"
#include "duckdb/parser/peg/ast/trim_arguments.hpp"
#include "duckdb/parser/peg/ast/trigger_event_info.hpp"
#include "duckdb/parser/peg/ast/trigger_table_referencing_info.hpp"
#include "duckdb/parser/peg/ast/window_frame.hpp"
#include "duckdb/function/macro_function.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/parser_options.hpp"
#include "duckdb/common/stack_checker.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parsed_data/connect_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"
#include "duckdb/parser/statement/copy_database_statement.hpp"
#include "duckdb/parser/statement/set_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/peg/peg_parser.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"

namespace duckdb {

// Forward declare
struct QualifiedName;
struct MatcherToken;
struct GroupingExpressionMap;
class Matcher;

enum class GroupByExpressionInfoType : uint8_t { EXPRESSION, EMPTY, CUBE, ROLLUP, GROUPING_SETS };

struct GroupByExpressionInfo {
	GroupByExpressionInfoType type = GroupByExpressionInfoType::EMPTY;
	unique_ptr<ParsedExpression> expression;
	vector<unique_ptr<ParsedExpression>> expressions;
	vector<GroupByExpressionInfo> children;
};

struct PEGTransformerState {
	explicit PEGTransformerState(const vector<MatcherToken> &tokens_p) : tokens(tokens_p), token_index(0) {
	}

	const vector<MatcherToken> &tokens;
	idx_t token_index;
};

class PEGTransformer;
class TransformStack;
struct TransformStackFrame;

using transform_frame_function_t = void (*)(PEGTransformer &transformer, TransformStack &stack,
                                            TransformStackFrame &frame);
using transform_frame_finalize_t = unique_ptr<TransformResultValue> (*)(PEGTransformer &transformer,
                                                                        TransformStack &stack,
                                                                        TransformStackFrame &frame);
using transform_frame_index_t = idx_t;

enum class TransformFrameState : uint8_t { INITIALIZE, WAITING };

struct TransformFrameOps {
	const char *name;
	transform_frame_function_t initialize;
	transform_frame_finalize_t finalize;
};

struct TransformFrameResultTarget {
	TransformFrameResultTarget(transform_frame_index_t frame_index, idx_t slot);

	const transform_frame_index_t frame_index;
	const idx_t slot;
};

struct TransformStackFrame {
	TransformStackFrame(transform_frame_index_t frame_index, ParseResult &parse_result, const TransformFrameOps &ops,
	                    optional<TransformFrameResultTarget> result_target);

	void ReserveChildSlots(idx_t count);
	void SetChildResult(idx_t slot, unique_ptr<TransformResultValue> result);

	template <class T>
	T TakeResult(idx_t slot) {
		if (slot >= child_results.size() || !child_results[slot]) {
			throw InternalException("Missing trampoline transformer result for slot %llu in rule '%s'", slot, ops.name);
		}
		auto *typed_result = dynamic_cast<TypedTransformResult<T> *>(child_results[slot].get());
		if (!typed_result) {
			throw InternalException("Unexpected trampoline transformer result type for slot %llu in rule '%s'", slot,
			                        ops.name);
		}
		auto result = std::move(typed_result->value);
		child_results[slot].reset();
		return result;
	}

	const transform_frame_index_t frame_index;
	ParseResult &parse_result;
	const TransformFrameOps &ops;
	const optional<TransformFrameResultTarget> result_target;
	TransformFrameState state = TransformFrameState::INITIALIZE;
	vector<unique_ptr<TransformResultValue>> child_results;
};

class TransformStack {
public:
	explicit TransformStack(PEGTransformer &transformer);

	transform_frame_index_t PushFrame(ParseResult &parse_result, const TransformFrameOps &ops,
	                                  optional<TransformFrameResultTarget> result_target);

	template <class T>
	T Execute(ParseResult &parse_result, const TransformFrameOps &ops) {
		auto base_result = ExecuteInternal(parse_result, ops);
		auto *typed_result = dynamic_cast<TypedTransformResult<T> *>(base_result.get());
		if (!typed_result) {
			throw InternalException("Unexpected trampoline transformer result type for root rule '%s'", ops.name);
		}
		return std::move(typed_result->value);
	}

	TransformStackFrame &GetFrame(transform_frame_index_t frame_index);
	const TransformStackFrame &GetFrame(transform_frame_index_t frame_index) const;

	string FormatFrame(transform_frame_index_t frame_index) const;
	string FormatParentChain(transform_frame_index_t frame_index) const;
	string FormatStack() const;

private:
	unique_ptr<TransformResultValue> ExecuteInternal(ParseResult &parse_result, const TransformFrameOps &ops);
	void DeliverResult(TransformStackFrame &frame, unique_ptr<TransformResultValue> result);

private:
	PEGTransformer &transformer;
	vector<unique_ptr<TransformStackFrame>> frames;
	vector<transform_frame_index_t> frame_stack;
};

class PEGTransformer {
public:
	using AnyTransformFunction = std::function<unique_ptr<TransformResultValue>(PEGTransformer &, ParseResult &)>;

	PEGTransformer(ArenaAllocator &allocator, PEGTransformerState &state,
	               const case_insensitive_map_t<AnyTransformFunction> &transform_functions,
	               const case_insensitive_map_t<PEGRule> &grammar_rules, ParserOptions &options_p)
	    : allocator(allocator), state(state), grammar_rules(grammar_rules), transform_functions(transform_functions),
	      options(options_p) {
	}

public:
	template <typename T>
	T Transform(ParseResult &parse_result) {
		auto it = transform_functions.find(parse_result.name);
		if (it == transform_functions.end()) {
			throw NotImplementedException("No transformer function found for rule '%s'", parse_result.name);
		}
		auto &func = it->second;

		unique_ptr<TransformResultValue> base_result = func(*this, parse_result);
		if (!base_result) {
			throw InternalException("Transformer for rule '%s' returned a nullptr.", parse_result.name);
		}

		auto *typed_result_ptr = dynamic_cast<TypedTransformResult<T> *>(base_result.get());
		if (!typed_result_ptr) {
			// allow transparent bridging between string-typed and Identifier-typed rules
			auto bridged = TryBridgeTransformResult<T>(*base_result);
			if (bridged) {
				auto bridged_result = std::move(bridged->value);
				SetResultLocation(bridged_result, parse_result.offset);
				return bridged_result;
			}
			throw InternalException("Transformer for rule '" + parse_result.name + "' returned an unexpected type.");
		}

		auto result = std::move(typed_result_ptr->value);
		SetResultLocation(result, parse_result.offset);
		return result;
	}

	//! Bridge between string-typed and Identifier-typed rule results (and their vector forms).
	//! The generic form performs no bridging; the specializations below convert transparently.
	template <typename T>
	static unique_ptr<TypedTransformResult<T>> TryBridgeTransformResult(TransformResultValue &base_result) {
		return nullptr;
	}

	template <typename T>
	T Transform(ListParseResult &parse_result, idx_t child_index) {
		auto &child_parse_result = parse_result.GetChild(child_index);
		return Transform<T>(child_parse_result);
	}

	template <typename T>
	void TransformOptional(ListParseResult &list_pr, idx_t child_idx, T &target) {
		auto &opt = list_pr.Child<OptionalParseResult>(child_idx);
		if (opt.HasResult()) {
			target = Transform<T>(opt.GetResult());
		}
	}

	// Make overloads return raw pointers, as ownership is handled by the ArenaAllocator.
	template <class T, typename... Args>
	T *Make(Args &&...args) {
		return allocator.Make<T>(std::forward<Args>(args)...);
	}

	void Clear();
	void ClearParameters();
	static void ParamTypeCheck(PreparedParamType last_type, PreparedParamType new_type);
	void SetParam(const Identifier &name, idx_t index, PreparedParamType type);
	bool GetParam(const Identifier &name, idx_t &index, PreparedParamType type);
	void SetParamCount(idx_t new_count);
	idx_t ParamCount() const;
	unique_ptr<SQLStatement> CreatePivotStatement(unique_ptr<SQLStatement> statement);
	unique_ptr<SQLStatement> GenerateCreateEnumStmt(unique_ptr<CreatePivotEntry> entry);
	void PivotEntryCheck(const string &type);
	void ExtractCTEsRecursive(CommonTableExpressionMap &cte_map);
	bool IsWindowFrameDefault(WindowBoundary start, WindowBoundary end);
	unique_ptr<WindowExpression> GetWindowClause(const Identifier &window_name);
	void SetQueryLocation(ParsedExpression &expr, optional_idx query_location);
	void SetQueryLocation(TableRef &ref, optional_idx query_location);

private:
	template <typename T>
	void SetResultLocation(T &, optional_idx) {
	}
	void SetResultLocation(unique_ptr<ParsedExpression> &expr, optional_idx offset) {
		if (!expr) {
			return;
		}
		if (offset.IsValid() && !expr->GetQueryLocation().IsValid()) {
			SetQueryLocation(*expr, offset);
		}
	}
	void SetResultLocation(unique_ptr<TableRef> &ref, optional_idx offset) {
		if (!ref) {
			return;
		}
		if (offset.IsValid() && !ref->query_location.IsValid()) {
			SetQueryLocation(*ref, offset.GetIndex());
		}
	}

public:
	ArenaAllocator &allocator;
	PEGTransformerState &state;
	const case_insensitive_map_t<PEGRule> &grammar_rules;
	const case_insensitive_map_t<AnyTransformFunction> &transform_functions;
	identifier_map_t<idx_t> named_parameter_map;
	idx_t prepared_statement_parameter_index = 0;
	PreparedParamType last_param_type = PreparedParamType::INVALID;

	identifier_map_t<unique_ptr<WindowExpression>> window_clauses;

	vector<unique_ptr<CreatePivotEntry>> pivot_entries;
	vector<reference<CommonTableExpressionMap>> stored_cte_map;

	bool in_window_definition = false;
	bool has_anonymous_parameters = false;

	friend class StackChecker<PEGTransformer>;
	idx_t stack_depth = 0;

	StackChecker<PEGTransformer> StackCheck(idx_t extra_stack = 1) {
		if (stack_depth + extra_stack >= options.max_expression_depth) {
			throw ParserException(
			    "Max expression depth limit of %lld exceeded. Use \"SET max_expression_depth TO x\" to "
			    "increase the maximum expression depth.",
			    options.max_expression_depth);
		}
		return StackChecker<PEGTransformer>(*this, extra_stack);
	}

	ParserOptions options;
};

//! Transparent bridging between string-typed and Identifier-typed transform results.
template <>
inline unique_ptr<TypedTransformResult<string>>
PEGTransformer::TryBridgeTransformResult<string>(TransformResultValue &base_result) {
	if (auto *ident = dynamic_cast<TypedTransformResult<Identifier> *>(&base_result)) {
		return make_uniq<TypedTransformResult<string>>(ident->value.GetIdentifierName());
	}
	return nullptr;
}

template <>
inline unique_ptr<TypedTransformResult<Identifier>>
PEGTransformer::TryBridgeTransformResult<Identifier>(TransformResultValue &base_result) {
	if (auto *str = dynamic_cast<TypedTransformResult<string> *>(&base_result)) {
		return make_uniq<TypedTransformResult<Identifier>>(Identifier(str->value));
	}
	return nullptr;
}

template <>
inline unique_ptr<TypedTransformResult<vector<string>>>
PEGTransformer::TryBridgeTransformResult<vector<string>>(TransformResultValue &base_result) {
	if (auto *idents = dynamic_cast<TypedTransformResult<vector<Identifier>> *>(&base_result)) {
		return make_uniq<TypedTransformResult<vector<string>>>(IdentifiersToStrings(idents->value));
	}
	return nullptr;
}

template <>
inline unique_ptr<TypedTransformResult<vector<Identifier>>>
PEGTransformer::TryBridgeTransformResult<vector<Identifier>>(TransformResultValue &base_result) {
	if (auto *strs = dynamic_cast<TypedTransformResult<vector<string>> *>(&base_result)) {
		return make_uniq<TypedTransformResult<vector<Identifier>>>(StringsToIdentifiers(strs->value));
	}
	return nullptr;
}

typedef unique_ptr<TransformResultValue> (*transform_function_t)(PEGTransformer &transformer,
                                                                 ParseResult &parse_result);

struct TransformRule {
	const char *name;
	transform_function_t transform;
};

class PEGTransformerFactory {
public:
	explicit PEGTransformerFactory();

	//! Match a single TopLevelStatement from `tokens` starting at `token_cursor` and transform it
	//! into a SQLStatement. Returns nullptr if the matched TLS was separator-only (no statement).
	//! Throws on syntax error. `token_cursor` is in/out: it's the token index where matching
	//! starts, and on return holds the token index immediately past the last consumed token.
	unique_ptr<SQLStatement> TransformTopLevelStatement(vector<MatcherToken> &tokens, ParserOptions &options,
	                                                    Matcher &root_matcher, idx_t &token_cursor);
	static ParseResult &ExtractResultFromParens(ParseResult &parse_result);
	static vector<reference<ParseResult>> ExtractParseResultsFromList(ParseResult &parse_result);
	static bool ExpressionIsEmptyStar(const ParsedExpression &expr);
	static QualifiedName StringToQualifiedName(vector<string> input);
	static LogicalType GetIntervalTargetType(DatePartSpecifier date_part);
	static bool ConstructConstantFromExpression(const ParsedExpression &expr, Value &value);
	static unique_ptr<ParsedExpression> TryNegateValue(const ConstantExpression &expr);
	static unique_ptr<ParsedExpression> ConvertNumberToValue(string val);
	static void AddGroupByExpression(unique_ptr<ParsedExpression> expression, GroupingExpressionMap &map,
	                                 GroupByNode &result, vector<ProjectionIndex> &result_set);
	static vector<GroupingSet> GroupByExpressionUnfolding(GroupByExpressionInfo &group_by_expr,
	                                                      GroupingExpressionMap &map, GroupByNode &result);
	static unique_ptr<ResultModifier> VerifyLimitOffset(LimitPercentResult &limit, LimitPercentResult &offset);
	static unique_ptr<QueryNode> ToRecursiveCTE(unique_ptr<QueryNode> node, const Identifier &name,
	                                            vector<Identifier> &aliases,
	                                            vector<unique_ptr<ParsedExpression>> &key_targets);
	static void WrapRecursiveView(unique_ptr<CreateViewInfo> &info, unique_ptr<QueryNode> inner_node);
	static void ConvertToRecursiveView(unique_ptr<CreateViewInfo> &info, unique_ptr<QueryNode> &node);
	static void VerifyColumnRefs(const ParsedExpression &expr);
	static void RemoveOrderQualificationRecursive(unique_ptr<ParsedExpression> &root_expr);
	static void GetValueFromExpression(unique_ptr<ParsedExpression> &expr, vector<Value> &result);
	static bool TransformPivotInList(unique_ptr<ParsedExpression> &expr, PivotColumnEntry &entry);
	static void AddPivotEntry(PEGTransformer &transformer, string enum_name, unique_ptr<SelectNode> base,
	                          unique_ptr<ParsedExpression> column, unique_ptr<QueryNode> subquery, bool has_parameters);
	static Value GetConstantExpressionValue(unique_ptr<ParsedExpression> &expr);
	static void AddToMultiStatement(const unique_ptr<MultiStatement> &multi_statement,
	                                unique_ptr<AlterInfo> alter_info);
	static void AddUpdateToMultiStatement(const unique_ptr<MultiStatement> &multi_statement, const string &column_name,
	                                      const AlterEntryData &table_data,
	                                      const unique_ptr<ParsedExpression> &original_expression);
	static unique_ptr<MultiStatement> TransformAndMaterializeAlter(AlterEntryData &data,
	                                                               unique_ptr<AlterInfo> info_with_null_placeholder,
	                                                               const string &column_name,
	                                                               unique_ptr<ParsedExpression> expression);

	//===--------------------------------------------------------------------===//
	// START GENERATED TRAMPOLINE RULES
	//===--------------------------------------------------------------------===//
	static void InitializeUseStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUseStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUseTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUseTargetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSchemaNameAsUseTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSchemaNameAsUseTargetTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeCatalogNameAsUseTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCatalogNameAsUseTargetTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeUseTargetCatalogSchemaTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeUseTargetCatalogSchemaTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeDotIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDotIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	//===--------------------------------------------------------------------===//
	// END GENERATED TRAMPOLINE RULES
	//===--------------------------------------------------------------------===//

	// Registration methods
	void RegisterComment();
	void RegisterCommon();
	void RegisterCreateTable();
	void RegisterExpression();
	void RegisterPivot();
	void RegisterSelect();
	void RegisterKeywordsAndIdentifiers();
	void RegisterGenerated();
	void RegisterGeneratedTrampoline();

	template <class FUNC>
	void Register(const string &rule_name, FUNC function) {
		auto existing_rule = sql_transform_functions.find(rule_name);
		if (existing_rule != sql_transform_functions.end()) {
			throw InternalException("Rule %s already exists", rule_name);
		}
		sql_transform_functions[rule_name] = [function](PEGTransformer &transformer,
		                                                ParseResult &parse_result) -> unique_ptr<TransformResultValue> {
			auto result_value = function(transformer, parse_result);
			return make_uniq<TypedTransformResult<decltype(result_value)>>(std::move(result_value));
		};
	}

	PEGTransformerFactory(const PEGTransformerFactory &) = delete;

	static unique_ptr<SQLStatement> TransformStatement(PEGTransformer &, ParseResult &list);
	static unique_ptr<SQLStatement> TransformStatementTrampoline(PEGTransformer &transformer,
	                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformStatementTrampolineInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static const case_insensitive_map_t<const TransformFrameOps *> &GeneratedTrampolineOps();

	// comment.gram
	static Value TransformCommentValue(PEGTransformer &transformer, ParseResult &parse_result);

	// common.gram
	static unique_ptr<ParsedExpression> TransformNumberLiteral(PEGTransformer &transformer, ParseResult &parse_result);
	static string TransformStringLiteral(PEGTransformer &transformer, ParseResult &parse_result);
	static DatePartSpecifier TransformIntervalToIntervalAsType(PEGTransformer &transformer, ParseResult &parse_result);

	static string ExtractFormat(const string &file_path);

	// create_table.gram
	static string TransformColLabelOrString(PEGTransformer &transformer, ParseResult &parse_result);
	static string TransformIdentifier(PEGTransformer &transformer, ParseResult &parse_result);

	// expression.gram
	static unique_ptr<ParsedExpression> TransformExpression(PEGTransformer &transformer, ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformPrefixExpression(PEGTransformer &transformer,
	                                                              ParseResult &parse_result);
	static unique_ptr<WindowExpression> TransformOverClause(PEGTransformer &transformer, ParseResult &parse_result);

	// pivot.gram
	static unique_ptr<SelectStatement> TransformPivotStatement(PEGTransformer &transformer, ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformUnpivotStatement(PEGTransformer &transformer,
	                                                             ParseResult &parse_result);

	// select.gram
	static unique_ptr<SelectStatement> TransformSelectStatementInternalRule(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformSimpleSelect(PEGTransformer &transformer, ParseResult &parse_result);

	static unique_ptr<TableRef> TransformTableRef(PEGTransformer &transformer, ParseResult &parse_result);

	static CommonTableExpressionMap TransformWithClause(PEGTransformer &transformer, ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformWindowDefinition(PEGTransformer &transformer,
	                                                              ParseResult &parse_result);
	static string TransformIdentifierOrKeyword(PEGTransformer &transformer, ParseResult &parse_result);

	//===--------------------------------------------------------------------===//
	// START GENERATED RULES
	//===--------------------------------------------------------------------===//
	static unique_ptr<TransformResultValue> TransformAlterStatementInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformAlterStatement(PEGTransformer &transformer,
	                                                        unique_ptr<AlterInfo> alter_options);
	static unique_ptr<TransformResultValue> TransformAlterOptionsInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformAlterTableStmtInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<AlterInfo> TransformAlterTableStmt(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                     unique_ptr<BaseTableRef> base_table_name,
	                                                     vector<unique_ptr<AlterTableInfo>> alter_table_options);
	static unique_ptr<TransformResultValue> TransformAlterSchemaStmtInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<AlterInfo> TransformAlterSchemaStmt(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                      const QualifiedName &qualified_name,
	                                                      unique_ptr<AlterTableInfo> rename_alter);
	static unique_ptr<TransformResultValue> TransformAlterTableOptionsInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformAddConstraintInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformAddConstraint(PEGTransformer &transformer,
	                                                         unique_ptr<Constraint> top_level_constraint);
	static unique_ptr<TransformResultValue> TransformAddColumnInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformAddColumn(PEGTransformer &transformer, const bool &has_result,
	                                                     const optional<bool> &if_not_exists,
	                                                     AddColumnEntry add_column_entry);
	static unique_ptr<TransformResultValue> TransformAddColumnEntryInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static AddColumnEntry TransformAddColumnEntry(PEGTransformer &transformer, const vector<string> &dotted_identifier,
	                                              const optional<LogicalType> &type,
	                                              optional<GeneratedColumnDefinition> generated_column,
	                                              optional<vector<ColumnConstraintEntry>> column_constraint);
	static unique_ptr<TransformResultValue> TransformDropColumnInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformDropColumn(PEGTransformer &transformer, const bool &has_result,
	                                                      const optional<bool> &if_exists,
	                                                      unique_ptr<ColumnRefExpression> nested_column_name,
	                                                      const optional<bool> &drop_behavior);
	static unique_ptr<TransformResultValue> TransformAlterColumnInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformAlterColumn(PEGTransformer &transformer, const bool &has_result,
	                                                       unique_ptr<ColumnRefExpression> nested_column_name,
	                                                       unique_ptr<AlterTableInfo> alter_column_entry);
	static unique_ptr<TransformResultValue> TransformRenameColumnInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformRenameColumn(PEGTransformer &transformer, const bool &has_result,
	                                                        unique_ptr<ColumnRefExpression> nested_column_name,
	                                                        const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformNestedColumnNameInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ColumnRefExpression> TransformNestedColumnName(PEGTransformer &transformer,
	                                                                 const optional<vector<Identifier>> &identifier_dot,
	                                                                 const Identifier &column_name);
	static unique_ptr<TransformResultValue> TransformIdentifierDotInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static Identifier TransformIdentifierDot(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformRenameAlterInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformRenameAlter(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformSetPartitionedByInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformSetPartitionedBy(PEGTransformer &transformer,
	                                                            vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformResetPartitionedByInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformResetPartitionedBy(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSetSortedByInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformSetSortedBy(PEGTransformer &transformer,
	                                                       vector<OrderByNode> order_by_expressions);
	static unique_ptr<TransformResultValue> TransformResetSortedByInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformResetSortedBy(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSetOptionsInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<AlterTableInfo>
	TransformSetOptions(PEGTransformer &transformer,
	                    case_insensitive_map_t<unique_ptr<ParsedExpression>> rel_option_list);
	static unique_ptr<TransformResultValue> TransformResetOptionsInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<AlterTableInfo>
	TransformResetOptions(PEGTransformer &transformer,
	                      case_insensitive_map_t<unique_ptr<ParsedExpression>> rel_option_list);
	static unique_ptr<TransformResultValue> TransformAlterColumnEntryInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformAddOrDropDefaultInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformAddDefaultInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformAddDefault(PEGTransformer &transformer,
	                                                      unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformDropDefaultInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformDropDefault(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformChangeNullabilityInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformChangeNullability(PEGTransformer &transformer,
	                                                             const string &drop_or_set);
	static unique_ptr<TransformResultValue> TransformDropOrSetInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDropNullabilityInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static string TransformDropNullability(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSetNullabilityInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static string TransformSetNullability(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformAlterTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<AlterTableInfo> TransformAlterType(PEGTransformer &transformer, const bool &has_result,
	                                                     const optional<LogicalType> &type,
	                                                     optional<unique_ptr<ParsedExpression>> using_expression);
	static unique_ptr<TransformResultValue> TransformUsingExpressionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformUsingExpression(PEGTransformer &transformer,
	                                                             unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformAlterViewStmtInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<AlterInfo> TransformAlterViewStmt(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                    unique_ptr<BaseTableRef> base_table_name,
	                                                    unique_ptr<AlterTableInfo> rename_alter);
	static unique_ptr<TransformResultValue> TransformAlterSequenceStmtInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<AlterInfo> TransformAlterSequenceStmt(PEGTransformer &transformer,
	                                                        const optional<bool> &if_exists,
	                                                        const QualifiedName &qualified_sequence_name,
	                                                        unique_ptr<AlterInfo> alter_sequence_options);
	static unique_ptr<TransformResultValue> TransformQualifiedSequenceNameInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static QualifiedName TransformQualifiedSequenceName(PEGTransformer &transformer,
	                                                    const optional<Identifier> &catalog_qualification,
	                                                    const optional<Identifier> &schema_qualification,
	                                                    const Identifier &sequence_name);
	static unique_ptr<TransformResultValue> TransformAlterSequenceOptionsInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<AlterInfo> TransformAlterSequenceOptions(PEGTransformer &transformer, ParseResult &choice_result);
	static unique_ptr<TransformResultValue> TransformSetSequenceOptionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<AlterInfo>
	TransformSetSequenceOption(PEGTransformer &transformer,
	                           vector<pair<string, unique_ptr<SequenceOption>>> sequence_option);
	static unique_ptr<TransformResultValue> TransformAlterDatabaseStmtInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<AlterInfo> TransformAlterDatabaseStmt(PEGTransformer &transformer,
	                                                        const optional<bool> &if_exists,
	                                                        const Identifier &identifier,
	                                                        const Identifier &identifier_1);
	static unique_ptr<TransformResultValue> TransformAnalyzeStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformAnalyzeStatement(PEGTransformer &transformer,
	                                                          const optional<bool> &analyze_verbose,
	                                                          optional<AnalyzeTarget> analyze_target);
	static unique_ptr<TransformResultValue> TransformAnalyzeTargetInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static AnalyzeTarget TransformAnalyzeTarget(PEGTransformer &transformer, unique_ptr<BaseTableRef> base_table_name,
	                                            const optional<vector<string>> &name_list);
	static unique_ptr<TransformResultValue> TransformAnalyzeVerboseInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformAnalyzeVerbose(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformAttachStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformAttachStatement(PEGTransformer &transformer, const optional<bool> &or_replace,
	                         const optional<bool> &if_not_exists, const bool &has_result,
	                         unique_ptr<ParsedExpression> database_path, const optional<Identifier> &attach_alias,
	                         const optional<vector<GenericCopyOption>> &attach_options);
	static unique_ptr<TransformResultValue> TransformDatabasePathInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDatabasePath(PEGTransformer &transformer,
	                                                          unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformAttachAliasInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static Identifier TransformAttachAlias(PEGTransformer &transformer, const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformAttachOptionsInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static vector<GenericCopyOption> TransformAttachOptions(PEGTransformer &transformer,
	                                                        const vector<GenericCopyOption> &generic_copy_option_list);
	static unique_ptr<TransformResultValue> TransformCallStatementInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCallStatement(PEGTransformer &transformer,
	                                                       const QualifiedName &qualified_table_function,
	                                                       vector<FunctionArgument> table_function_arguments);
	static unique_ptr<TransformResultValue> TransformCheckpointStatementInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCheckpointStatement(PEGTransformer &transformer,
	                                                             const optional<bool> &checkpoint_force,
	                                                             const optional<Identifier> &catalog_name);
	static unique_ptr<TransformResultValue> TransformCheckpointForceInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static bool TransformCheckpointForce(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCommentStatement(PEGTransformer &transformer,
	                                                          const CatalogType &comment_on_type,
	                                                          const vector<string> &dotted_identifier,
	                                                          const Value &comment_value);
	static unique_ptr<TransformResultValue> TransformCommentOnTypeInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCommentTableInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static CatalogType TransformCommentTable(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentSequenceInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static CatalogType TransformCommentSequence(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentFunctionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static CatalogType TransformCommentFunction(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentMacroTableInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static CatalogType TransformCommentMacroTable(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentMacroInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static CatalogType TransformCommentMacro(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentViewInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static CatalogType TransformCommentView(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentDatabaseInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static CatalogType TransformCommentDatabase(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentIndexInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static CatalogType TransformCommentIndex(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentSchemaInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static CatalogType TransformCommentSchema(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentTypeInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static CatalogType TransformCommentType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCommentColumnInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static CatalogType TransformCommentColumn(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformExpressionStatementInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformExpressionStatement(PEGTransformer &transformer,
	                                                             vector<unique_ptr<ParsedExpression>> expression_alias);
	static unique_ptr<TransformResultValue> TransformExpressionAliasInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformConstraintNameInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static Identifier TransformConstraintName(PEGTransformer &transformer, const Identifier &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformCollationNameInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static Identifier TransformCollationName(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformTypeInternal(PEGTransformer &transformer,
	                                                              ParseResult &parse_result);
	static LogicalType TransformType(PEGTransformer &transformer, unique_ptr<ParsedExpression> type_variations,
	                                 const optional<vector<int64_t>> &array_bounds);
	static unique_ptr<TransformResultValue> TransformTypeVariationsInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSimpleTypeInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCharacterSimpleTypeInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformCharacterSimpleType(PEGTransformer &transformer,
	                             optional<vector<unique_ptr<ParsedExpression>>> type_modifiers);
	static unique_ptr<TransformResultValue> TransformQualifiedSimpleTypeInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformQualifiedSimpleType(PEGTransformer &transformer, const QualifiedName &qualified_type_name,
	                             optional<vector<unique_ptr<ParsedExpression>>> type_modifiers);
	static unique_ptr<TransformResultValue> TransformIntervalTypeInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIntervalIntervalInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIntervalWithSpecifierInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIntervalWithRangeSpecifierInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformIntervalWithRangeSpecifier(PEGTransformer &transformer,
	                                    const DatePartSpecifier &interval_to_interval_as_type);
	static unique_ptr<TransformResultValue> TransformIntervalWithSimpleSpecifierInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIntervalWithSimpleSpecifier(PEGTransformer &transformer,
	                                                                         const DatePartSpecifier &interval);
	static unique_ptr<TransformResultValue> TransformIntervalWithoutSpecifierInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIntervalWithoutSpecifier(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformYearKeywordInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static DatePartSpecifier TransformYearKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformMonthKeywordInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static DatePartSpecifier TransformMonthKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDayKeywordInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static DatePartSpecifier TransformDayKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformHourKeywordInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static DatePartSpecifier TransformHourKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformMinuteKeywordInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static DatePartSpecifier TransformMinuteKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSecondKeywordInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static DatePartSpecifier TransformSecondKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformMillisecondKeywordInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static DatePartSpecifier TransformMillisecondKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformMicrosecondKeywordInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static DatePartSpecifier TransformMicrosecondKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformWeekKeywordInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static DatePartSpecifier TransformWeekKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformQuarterKeywordInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static DatePartSpecifier TransformQuarterKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDecadeKeywordInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static DatePartSpecifier TransformDecadeKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCenturyKeywordInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static DatePartSpecifier TransformCenturyKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformMillenniumKeywordInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static DatePartSpecifier TransformMillenniumKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIntervalInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIntervalToIntervalInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformYearToMonthInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static DatePartSpecifier TransformYearToMonth(PEGTransformer &transformer, const DatePartSpecifier &year_keyword,
	                                              const DatePartSpecifier &month_keyword);
	static unique_ptr<TransformResultValue> TransformDayToHourInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static DatePartSpecifier TransformDayToHour(PEGTransformer &transformer, const DatePartSpecifier &day_keyword,
	                                            const DatePartSpecifier &hour_keyword);
	static unique_ptr<TransformResultValue> TransformDayToMinuteInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static DatePartSpecifier TransformDayToMinute(PEGTransformer &transformer, const DatePartSpecifier &day_keyword,
	                                              const DatePartSpecifier &minute_keyword);
	static unique_ptr<TransformResultValue> TransformDayToSecondInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static DatePartSpecifier TransformDayToSecond(PEGTransformer &transformer, const DatePartSpecifier &day_keyword,
	                                              const DatePartSpecifier &second_keyword);
	static unique_ptr<TransformResultValue> TransformHourToMinuteInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static DatePartSpecifier TransformHourToMinute(PEGTransformer &transformer, const DatePartSpecifier &hour_keyword,
	                                               const DatePartSpecifier &minute_keyword);
	static unique_ptr<TransformResultValue> TransformHourToSecondInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static DatePartSpecifier TransformHourToSecond(PEGTransformer &transformer, const DatePartSpecifier &hour_keyword,
	                                               const DatePartSpecifier &second_keyword);
	static unique_ptr<TransformResultValue> TransformMinuteToSecondInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static DatePartSpecifier TransformMinuteToSecond(PEGTransformer &transformer,
	                                                 const DatePartSpecifier &minute_keyword,
	                                                 const DatePartSpecifier &second_keyword);
	static unique_ptr<TransformResultValue> TransformBitTypeInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformBitType(PEGTransformer &transformer, const bool &has_result,
	                                                     optional<vector<unique_ptr<ParsedExpression>>> expression);
	static unique_ptr<TransformResultValue> TransformGeometryTypeInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformGeometryType(PEGTransformer &transformer,
	                                                          optional<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformVariantTypeInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformVariantType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNumericTypeInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSimpleNumericTypeInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformSimpleNumericType(PEGTransformer &transformer, const string &child);
	static unique_ptr<TransformResultValue> TransformDecimalNumericTypeInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIntTypeInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static string TransformIntType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIntegerTypeInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static string TransformIntegerType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSmallintTypeInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static string TransformSmallintType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformBigintTypeInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static string TransformBigintType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRealTypeInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static string TransformRealType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformBooleanTypeInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static string TransformBooleanType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDoubleTypeInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static string TransformDoubleType(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFloatTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformFloatType(PEGTransformer &transformer,
	                                                       optional<unique_ptr<ParsedExpression>> number_literal);
	static unique_ptr<TransformResultValue> TransformDecimalTypeInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformDecimalType(PEGTransformer &transformer, optional<vector<unique_ptr<ParsedExpression>>> type_modifiers);
	static unique_ptr<TransformResultValue> TransformDecTypeInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDecType(PEGTransformer &transformer,
	                                                     optional<vector<unique_ptr<ParsedExpression>>> type_modifiers);
	static unique_ptr<TransformResultValue> TransformNumericModTypeInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformNumericModType(PEGTransformer &transformer, optional<vector<unique_ptr<ParsedExpression>>> type_modifiers);
	static unique_ptr<TransformResultValue> TransformQualifiedTypeNameInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTypeNameAsQualifiedNameInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static QualifiedName TransformTypeNameAsQualifiedName(PEGTransformer &transformer, const Identifier &type_name);
	static unique_ptr<TransformResultValue> TransformCatalogReservedSchemaTypeNameInternal(PEGTransformer &transformer,
	                                                                                       ParseResult &parse_result);
	static QualifiedName TransformCatalogReservedSchemaTypeName(PEGTransformer &transformer,
	                                                            const Identifier &catalog_qualification,
	                                                            const Identifier &reserved_schema_qualification,
	                                                            const Identifier &reserved_type_name);
	static unique_ptr<TransformResultValue> TransformSchemaReservedTypeNameInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static QualifiedName TransformSchemaReservedTypeName(PEGTransformer &transformer,
	                                                     const Identifier &schema_qualification,
	                                                     const Identifier &reserved_type_name);
	static unique_ptr<TransformResultValue> TransformTypeModifiersInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformTypeModifiers(PEGTransformer &transformer, optional<vector<unique_ptr<ParsedExpression>>> expression);
	static unique_ptr<TransformResultValue> TransformRowTypeInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformRowType(PEGTransformer &transformer,
	                                                     const optional<child_list_t<LogicalType>> &col_id_type_list);
	static unique_ptr<TransformResultValue> TransformSetofTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformSetofType(PEGTransformer &transformer, const LogicalType &type);
	static unique_ptr<TransformResultValue> TransformUnionTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformUnionType(PEGTransformer &transformer,
	                                                       const child_list_t<LogicalType> &col_id_type_list);
	static unique_ptr<TransformResultValue> TransformColIdTypeListInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static child_list_t<LogicalType> TransformColIdTypeList(PEGTransformer &transformer,
	                                                        const vector<pair<Identifier, LogicalType>> &col_id_type);
	static unique_ptr<TransformResultValue> TransformMapTypeInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformMapType(PEGTransformer &transformer, const vector<LogicalType> &type);
	static unique_ptr<TransformResultValue> TransformTupleTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformTupleType(PEGTransformer &transformer,
	                                                       const vector<LogicalType> &type);
	static unique_ptr<TransformResultValue> TransformColIdTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static pair<Identifier, LogicalType> TransformColIdType(PEGTransformer &transformer, const Identifier &col_id,
	                                                        const LogicalType &type);
	static unique_ptr<TransformResultValue> TransformArrayBoundsInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformArrayKeywordInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static int64_t TransformArrayKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSquareBracketsArrayInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static int64_t TransformSquareBracketsArray(PEGTransformer &transformer,
	                                            optional<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformTimeTypeInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformTimeType(PEGTransformer &transformer,
	                                                      const LogicalTypeId &time_or_timestamp,
	                                                      optional<vector<unique_ptr<ParsedExpression>>> type_modifiers,
	                                                      const optional<bool> &time_zone);
	static unique_ptr<TransformResultValue> TransformTimeOrTimestampInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTimeTypeIdInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static LogicalTypeId TransformTimeTypeId(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTimestampTypeIdInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static LogicalTypeId TransformTimestampTypeId(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTimeZoneInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static bool TransformTimeZone(PEGTransformer &transformer, const bool &with_or_without);
	static unique_ptr<TransformResultValue> TransformWithOrWithoutInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformWithRuleInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static bool TransformWithRule(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformWithoutRuleInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformWithoutRule(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformConnectStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformConnectStatement(PEGTransformer &transformer,
	                                                          optional<unique_ptr<ConnectInfo>> session_target);
	static unique_ptr<TransformResultValue> TransformDisconnectStatementInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformDisconnectStatement(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSessionTargetInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLocalSessionTargetInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ConnectInfo> TransformLocalSessionTarget(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformStringSessionTargetInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<ConnectInfo> TransformStringSessionTarget(PEGTransformer &transformer,
	                                                            const string &string_literal);
	static unique_ptr<TransformResultValue> TransformCatalogSessionTargetInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ConnectInfo> TransformCatalogSessionTarget(PEGTransformer &transformer,
	                                                             const Identifier &catalog_name);
	static unique_ptr<TransformResultValue> TransformCopyStatementInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCopyStatement(PEGTransformer &transformer,
	                                                       unique_ptr<SQLStatement> copy_variations);
	static unique_ptr<TransformResultValue> TransformCopyVariationsInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCopyTableInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCopyTable(PEGTransformer &transformer,
	                                                   unique_ptr<BaseTableRef> base_table_name,
	                                                   const optional<vector<string>> &insert_column_list,
	                                                   const bool &from_or_to,
	                                                   unique_ptr<ParsedExpression> copy_file_name,
	                                                   const optional<vector<GenericCopyOption>> &copy_options);
	static unique_ptr<TransformResultValue> TransformFromOrToInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCopyFromInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static bool TransformCopyFrom(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCopyToInternal(PEGTransformer &transformer,
	                                                                ParseResult &parse_result);
	static bool TransformCopyTo(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCopySelectInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCopySelect(PEGTransformer &transformer,
	                                                    unique_ptr<SelectStatement> select_statement_internal,
	                                                    unique_ptr<ParsedExpression> copy_file_name,
	                                                    const optional<vector<GenericCopyOption>> &copy_options);
	static unique_ptr<TransformResultValue> TransformCopyFileNameInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCopyFileNameExpressionInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCopyFileNameStringLiteralInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformCopyFileNameStringLiteral(PEGTransformer &transformer,
	                                                                       const string &string_literal);
	static unique_ptr<TransformResultValue> TransformCopyFileNameIdentifierInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformCopyFileNameIdentifier(PEGTransformer &transformer,
	                                                                    const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformCopyFileNameIdentifierColIdInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformCopyFileNameIdentifierColId(PEGTransformer &transformer,
	                                                                         const Identifier &identifier_col_id);
	static unique_ptr<TransformResultValue> TransformIdentifierColIdInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static Identifier TransformIdentifierColId(PEGTransformer &transformer, const Identifier &identifier,
	                                           const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformCopyOptionsInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static vector<GenericCopyOption> TransformCopyOptions(PEGTransformer &transformer, const bool &has_result,
	                                                      const vector<GenericCopyOption> &copy_option_list);
	static unique_ptr<TransformResultValue> TransformCopyOptionListInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSpecializedOptionListInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static vector<GenericCopyOption>
	TransformSpecializedOptionList(PEGTransformer &transformer,
	                               const optional<vector<GenericCopyOption>> &specialized_option);
	static unique_ptr<TransformResultValue> TransformSpecializedOptionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSingleOptionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformBinaryOptionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static GenericCopyOption TransformBinaryOption(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFreezeOptionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static GenericCopyOption TransformFreezeOption(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOidsOptionInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static GenericCopyOption TransformOidsOption(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCsvOptionInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static GenericCopyOption TransformCsvOption(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformHeaderOptionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static GenericCopyOption TransformHeaderOption(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNullAsOptionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static GenericCopyOption TransformNullAsOption(PEGTransformer &transformer, const bool &has_result,
	                                               const string &string_literal);
	static unique_ptr<TransformResultValue> TransformDelimiterAsOptionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static GenericCopyOption TransformDelimiterAsOption(PEGTransformer &transformer, const bool &has_result,
	                                                    const string &string_literal);
	static unique_ptr<TransformResultValue> TransformQuoteAsOptionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static GenericCopyOption TransformQuoteAsOption(PEGTransformer &transformer, const bool &has_result,
	                                                const string &string_literal);
	static unique_ptr<TransformResultValue> TransformEscapeAsOptionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static GenericCopyOption TransformEscapeAsOption(PEGTransformer &transformer, const bool &has_result,
	                                                 const string &string_literal);
	static unique_ptr<TransformResultValue> TransformEncodingOptionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static GenericCopyOption TransformEncodingOption(PEGTransformer &transformer, const string &string_literal);
	static unique_ptr<TransformResultValue> TransformForceQuoteOptionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static GenericCopyOption TransformForceQuoteOption(PEGTransformer &transformer, const optional<bool> &force_quote,
	                                                   const vector<string> &star_symbol_column_list);
	static unique_ptr<TransformResultValue> TransformStarSymbolColumnListInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformForceQuoteInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static bool TransformForceQuote(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformPartitionByOptionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static GenericCopyOption TransformPartitionByOption(PEGTransformer &transformer,
	                                                    const vector<string> &star_symbol_column_list);
	static unique_ptr<TransformResultValue> TransformForceNullOptionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static GenericCopyOption TransformForceNullOption(PEGTransformer &transformer, const optional<bool> &force_not_null,
	                                                  const vector<string> &column_list);
	static unique_ptr<TransformResultValue> TransformForceNotNullInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformForceNotNull(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGenericCopyOptionListInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static vector<GenericCopyOption>
	TransformGenericCopyOptionList(PEGTransformer &transformer, const vector<GenericCopyOption> &generic_copy_option);
	static unique_ptr<TransformResultValue> TransformGenericCopyOptionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static GenericCopyOption TransformGenericCopyOption(PEGTransformer &transformer, const Identifier &copy_option_name,
	                                                    optional<GenericCopyOptionValue> generic_copy_option_value);
	static unique_ptr<TransformResultValue> TransformGenericCopyOptionValueInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformGenericCopyOptionOrderListInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static GenericCopyOptionValue
	TransformGenericCopyOptionOrderList(PEGTransformer &transformer,
	                                    vector<OrderByNode> generic_copy_option_parenthesized_expression_list);
	static unique_ptr<TransformResultValue> TransformGenericCopyOptionExpressionInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static GenericCopyOptionValue TransformGenericCopyOptionExpression(PEGTransformer &transformer,
	                                                                   unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue>
	TransformGenericCopyOptionParenthesizedExpressionListInternal(PEGTransformer &transformer,
	                                                              ParseResult &parse_result);
	static vector<OrderByNode>
	TransformGenericCopyOptionParenthesizedExpressionList(PEGTransformer &transformer,
	                                                      vector<OrderByNode> order_by_expression_list);
	static unique_ptr<TransformResultValue> TransformCopyFromDatabaseInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCopyFromDatabaseWithFlagInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCopyFromDatabaseWithFlag(PEGTransformer &transformer,
	                                                                  const Identifier &col_id,
	                                                                  const Identifier &col_id_1,
	                                                                  const CopyDatabaseType &copy_database_flag);
	static unique_ptr<TransformResultValue> TransformCopyFromDatabaseWithoutFlagInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCopyFromDatabaseWithoutFlag(PEGTransformer &transformer,
	                                                                     const Identifier &col_id,
	                                                                     const Identifier &col_id_1);
	static unique_ptr<TransformResultValue> TransformCopyDatabaseFlagInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static CopyDatabaseType TransformCopyDatabaseFlag(PEGTransformer &transformer,
	                                                  const CopyDatabaseType &schema_or_data);
	static unique_ptr<TransformResultValue> TransformSchemaOrDataInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCopySchemaInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static CopyDatabaseType TransformCopySchema(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCopyDataInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static CopyDatabaseType TransformCopyData(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCreateIndexStmtInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<CreateStatement>
	TransformCreateIndexStmt(PEGTransformer &transformer, const optional<bool> &unique_index,
	                         const optional<bool> &if_not_exists, const optional<Identifier> &index_name,
	                         unique_ptr<BaseTableRef> base_table_name,
	                         const optional<vector<string>> &insert_column_list, const optional<Identifier> &index_type,
	                         optional<vector<unique_ptr<ParsedExpression>>> index_element,
	                         optional<case_insensitive_map_t<unique_ptr<ParsedExpression>>> with_list,
	                         optional<unique_ptr<ParsedExpression>> where_clause);
	static unique_ptr<TransformResultValue> TransformWithListInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static case_insensitive_map_t<unique_ptr<ParsedExpression>>
	TransformWithList(PEGTransformer &transformer,
	                  case_insensitive_map_t<unique_ptr<ParsedExpression>> rel_option_or_oids);
	static unique_ptr<TransformResultValue> TransformRelOptionOrOidsInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformRelOptionListInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static case_insensitive_map_t<unique_ptr<ParsedExpression>>
	TransformRelOptionList(PEGTransformer &transformer,
	                       vector<pair<Identifier, unique_ptr<ParsedExpression>>> rel_option);
	static unique_ptr<TransformResultValue> TransformOidsInternal(PEGTransformer &transformer,
	                                                              ParseResult &parse_result);
	static case_insensitive_map_t<unique_ptr<ParsedExpression>> TransformOids(PEGTransformer &transformer,
	                                                                          const bool &with_or_without_oids);
	static unique_ptr<TransformResultValue> TransformWithOrWithoutOidsInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformWithOidsInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static bool TransformWithOids(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformWithoutOidsInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformWithoutOids(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIndexElementInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIndexElement(PEGTransformer &transformer,
	                                                          unique_ptr<ParsedExpression> expression,
	                                                          const optional<OrderType> &desc_or_asc,
	                                                          const optional<OrderByNullType> &nulls_first_or_last);
	static unique_ptr<TransformResultValue> TransformUniqueIndexInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformUniqueIndex(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIndexTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static Identifier TransformIndexType(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformRelOptionInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static pair<Identifier, unique_ptr<ParsedExpression>>
	TransformRelOption(PEGTransformer &transformer, const Identifier &rel_option_name,
	                   optional<unique_ptr<ParsedExpression>> rel_option_argument_opt);
	static unique_ptr<TransformResultValue> TransformRelOptionNameInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static Identifier TransformRelOptionName(PEGTransformer &transformer, const string &child);
	static unique_ptr<TransformResultValue> TransformDottedIdentifierStringInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static string TransformDottedIdentifierString(PEGTransformer &transformer, const vector<string> &dotted_identifier);
	static unique_ptr<TransformResultValue> TransformRelOptionArgumentOptInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformRelOptionArgumentOpt(PEGTransformer &transformer,
	                                                                  unique_ptr<ParsedExpression> def_arg);
	static unique_ptr<TransformResultValue> TransformDefArgInternal(PEGTransformer &transformer,
	                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDefArgNullInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDefArgNull(PEGTransformer &transformer, const Value &null_literal);
	static unique_ptr<TransformResultValue> TransformDefArgKeywordInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDefArgKeyword(PEGTransformer &transformer,
	                                                           const string &reserved_keyword);
	static unique_ptr<TransformResultValue> TransformDefArgStringLiteralInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDefArgStringLiteral(PEGTransformer &transformer,
	                                                                 const string &string_literal);
	static unique_ptr<TransformResultValue> TransformNoneLiteralInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformNoneLiteral(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCreateMacroStmtInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<CreateStatement> TransformCreateMacroStmt(PEGTransformer &transformer,
	                                                            const bool &macro_or_function,
	                                                            const optional<bool> &if_not_exists,
	                                                            const QualifiedName &qualified_name,
	                                                            vector<unique_ptr<MacroFunction>> macro_definition);
	static unique_ptr<TransformResultValue> TransformMacroOrFunctionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformMacroKeywordInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformMacroKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFunctionKeywordInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static bool TransformFunctionKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformMacroDefinitionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<MacroFunction> TransformMacroDefinition(PEGTransformer &transformer,
	                                                          optional<vector<MacroParameter>> macro_parameters,
	                                                          unique_ptr<MacroFunction> macro_definition_body);
	static unique_ptr<TransformResultValue> TransformMacroDefinitionBodyInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformMacroParametersInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<MacroParameter> TransformMacroParameters(PEGTransformer &transformer,
	                                                       vector<MacroParameter> macro_parameter);
	static unique_ptr<TransformResultValue> TransformMacroParameterInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSimpleParameterInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static MacroParameter TransformSimpleParameter(PEGTransformer &transformer, const Identifier &type_func_name,
	                                               const optional<LogicalType> &type);
	static unique_ptr<TransformResultValue> TransformScalarMacroDefinitionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<MacroFunction> TransformScalarMacroDefinition(PEGTransformer &transformer,
	                                                                unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformTableMacroDefinitionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<MacroFunction>
	TransformTableMacroDefinition(PEGTransformer &transformer, unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformCreateSchemaStmtInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<CreateStatement> TransformCreateSchemaStmt(PEGTransformer &transformer,
	                                                             const optional<bool> &if_not_exists,
	                                                             const QualifiedName &qualified_name);
	static unique_ptr<TransformResultValue> TransformCreateSecretStmtInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<CreateStatement>
	TransformCreateSecretStmt(PEGTransformer &transformer, const optional<bool> &if_not_exists,
	                          const optional<Identifier> &secret_name,
	                          const optional<Identifier> &secret_storage_specifier,
	                          const vector<GenericCopyOption> &generic_copy_option_list);
	static unique_ptr<TransformResultValue> TransformSecretStorageSpecifierInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static Identifier TransformSecretStorageSpecifier(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformSecretNameInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static Identifier TransformSecretName(PEGTransformer &transformer, const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformCreateSequenceStmtInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<CreateStatement>
	TransformCreateSequenceStmt(PEGTransformer &transformer, const optional<bool> &if_not_exists,
	                            const QualifiedName &qualified_name,
	                            optional<vector<pair<string, unique_ptr<SequenceOption>>>> sequence_option);
	static unique_ptr<TransformResultValue> TransformSequenceOptionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSeqSetCycleInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSeqCycleInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqCycle(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSeqNoCycleInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqNoCycle(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSeqSetIncrementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqSetIncrement(PEGTransformer &transformer,
	                                                                         const bool &has_result,
	                                                                         unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformSeqSetMinMaxInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqSetMinMax(PEGTransformer &transformer,
	                                                                      const string &seq_min_or_max,
	                                                                      unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformSeqNoMinMaxInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqNoMinMax(PEGTransformer &transformer,
	                                                                     const string &seq_min_or_max);
	static unique_ptr<TransformResultValue> TransformSeqStartWithInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static pair<string, unique_ptr<SequenceOption>>
	TransformSeqStartWith(PEGTransformer &transformer, const bool &has_result, unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformSeqOwnedByInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqOwnedBy(PEGTransformer &transformer,
	                                                                    const QualifiedName &qualified_name);
	static unique_ptr<TransformResultValue> TransformSeqMinOrMaxInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformMinValueInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static string TransformMinValue(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformMaxValueInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static string TransformMaxValue(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCreateStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCreateStatement(PEGTransformer &transformer,
	                                                         const optional<bool> &or_replace,
	                                                         const optional<SecretPersistType> &temporary,
	                                                         unique_ptr<CreateStatement> create_statement_variation);
	static unique_ptr<TransformResultValue> TransformCreateStatementVariationInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOrReplaceInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static bool TransformOrReplace(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTemporaryInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPersistentInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static SecretPersistType TransformPersistent(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTempPersistentInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static SecretPersistType TransformTempPersistent(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTemporaryPersistentInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static SecretPersistType TransformTemporaryPersistent(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCreateTableStmtInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<CreateStatement> TransformCreateTableStmt(PEGTransformer &transformer,
	                                                            const optional<bool> &if_not_exists,
	                                                            const QualifiedName &qualified_name,
	                                                            CreateTableDefinition create_table_definition,
	                                                            const optional<bool> &commit_action);
	static unique_ptr<TransformResultValue> TransformCreateTableDefinitionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCreateTableAsInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static CreateTableDefinition
	TransformCreateTableAs(PEGTransformer &transformer, optional<ColumnList> identifier_list,
	                       optional<PartitionSortedOptions> partition_sorted_options,
	                       optional<case_insensitive_map_t<unique_ptr<ParsedExpression>>> with_list,
	                       unique_ptr<SQLStatement> statement, const optional<bool> &with_data);
	static unique_ptr<TransformResultValue> TransformPartitionSortedOptionsInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPartitionOptSortedOptionsInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static PartitionSortedOptions
	TransformPartitionOptSortedOptions(PEGTransformer &transformer,
	                                   vector<unique_ptr<ParsedExpression>> partition_options,
	                                   optional<vector<unique_ptr<ParsedExpression>>> sorted_options);
	static unique_ptr<TransformResultValue> TransformSortedOptPartitionOptionsInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static PartitionSortedOptions
	TransformSortedOptPartitionOptions(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> sorted_options,
	                                   optional<vector<unique_ptr<ParsedExpression>>> partition_options);
	static unique_ptr<TransformResultValue> TransformPartitionOptionsInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformPartitionOptions(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformSortedOptionsInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformSortedOptions(PEGTransformer &transformer,
	                                                                   vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformWithDataInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformWithDataOnlyInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformWithDataOnly(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformWithNoDataInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static bool TransformWithNoData(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIdentifierListInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static ColumnList TransformIdentifierList(PEGTransformer &transformer, const vector<Identifier> &identifier);
	static unique_ptr<TransformResultValue> TransformCreateColumnListInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static CreateTableDefinition
	TransformCreateColumnList(PEGTransformer &transformer, optional<ColumnElements> create_table_column_list,
	                          optional<PartitionSortedOptions> partition_sorted_options,
	                          optional<case_insensitive_map_t<unique_ptr<ParsedExpression>>> with_list);
	static unique_ptr<TransformResultValue> TransformIfNotExistsInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformIfNotExists(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformQualifiedNameInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue>
	TransformSchemaReservedIdentifierOrStringLiteralInternal(PEGTransformer &transformer, ParseResult &parse_result);
	static QualifiedName
	TransformSchemaReservedIdentifierOrStringLiteral(PEGTransformer &transformer,
	                                                 const Identifier &schema_qualification,
	                                                 const Identifier &reserved_identifier_or_string_literal);
	static unique_ptr<TransformResultValue>
	TransformCatalogReservedSchemaIdentifierInternal(PEGTransformer &transformer, ParseResult &parse_result);
	static QualifiedName
	TransformCatalogReservedSchemaIdentifier(PEGTransformer &transformer, const Identifier &catalog_qualification,
	                                         const Identifier &reserved_schema_qualification,
	                                         const Identifier &reserved_identifier_or_string_literal);
	static unique_ptr<TransformResultValue> TransformIdentifierOrStringLiteralInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static QualifiedName TransformIdentifierOrStringLiteral(PEGTransformer &transformer, const string &child);
	static unique_ptr<TransformResultValue>
	TransformReservedIdentifierOrStringLiteralInternal(PEGTransformer &transformer, ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCatalogQualificationInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static Identifier TransformCatalogQualification(PEGTransformer &transformer, const Identifier &catalog_name);
	static unique_ptr<TransformResultValue> TransformSchemaQualificationInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static Identifier TransformSchemaQualification(PEGTransformer &transformer, const Identifier &schema_name);
	static unique_ptr<TransformResultValue> TransformReservedSchemaQualificationInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTableQualificationInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static Identifier TransformTableQualification(PEGTransformer &transformer, const Identifier &table_name);
	static unique_ptr<TransformResultValue> TransformReservedTableQualificationInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static Identifier TransformReservedTableQualification(PEGTransformer &transformer,
	                                                      const Identifier &reserved_table_name);
	static unique_ptr<TransformResultValue> TransformCreateTableColumnListInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static ColumnElements TransformCreateTableColumnList(PEGTransformer &transformer,
	                                                     vector<CreateTableColumnElement> create_table_column_element);
	static unique_ptr<TransformResultValue> TransformCreateTableColumnElementInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCreateTableColumnDefinitionInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static CreateTableColumnElement TransformCreateTableColumnDefinition(PEGTransformer &transformer,
	                                                                     ConstraintColumnDefinition column_definition);
	static unique_ptr<TransformResultValue> TransformCreateTableConstraintInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static CreateTableColumnElement TransformCreateTableConstraint(PEGTransformer &transformer,
	                                                               unique_ptr<Constraint> top_level_constraint);
	static unique_ptr<TransformResultValue> TransformColumnDefinitionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static ConstraintColumnDefinition
	TransformColumnDefinition(PEGTransformer &transformer, const vector<string> &dotted_identifier,
	                          const optional<LogicalType> &type, optional<GeneratedColumnDefinition> generated_column,
	                          const bool &has_result, optional<vector<ColumnConstraintEntry>> column_constraint);
	static unique_ptr<TransformResultValue> TransformColumnConstraintInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNotNullConstraintInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static ColumnConstraintEntry TransformNotNullConstraint(PEGTransformer &transformer, const bool &child);
	static unique_ptr<TransformResultValue> TransformNullConstraintInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformNullConstraint(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNotNullColumnConstraintInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static bool TransformNotNullColumnConstraint(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformUniqueConstraintInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static ColumnConstraintEntry TransformUniqueConstraint(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformPrimaryKeyConstraintInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static ColumnConstraintEntry TransformPrimaryKeyConstraint(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDefaultValueInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static ColumnConstraintEntry TransformDefaultValue(PEGTransformer &transformer,
	                                                   unique_ptr<ParsedExpression> column_default_expr);
	static unique_ptr<TransformResultValue> TransformCheckConstraintInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static ColumnConstraintEntry TransformCheckConstraint(PEGTransformer &transformer,
	                                                      unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformForeignKeyConstraintInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static ColumnConstraintEntry TransformForeignKeyConstraint(PEGTransformer &transformer,
	                                                           unique_ptr<BaseTableRef> base_table_name,
	                                                           const optional<vector<string>> &column_list,
	                                                           const KeyActions &key_actions);
	static unique_ptr<TransformResultValue> TransformColumnCollationInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static ColumnConstraintEntry TransformColumnCollation(PEGTransformer &transformer,
	                                                      const vector<string> &dotted_identifier);
	static unique_ptr<TransformResultValue> TransformColumnCompressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static ColumnConstraintEntry TransformColumnCompression(PEGTransformer &transformer,
	                                                        const Identifier &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformKeyActionsInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static KeyActions TransformKeyActions(PEGTransformer &transformer, const optional<string> &update_action,
	                                      const optional<string> &delete_action);
	static unique_ptr<TransformResultValue> TransformUpdateActionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static string TransformUpdateAction(PEGTransformer &transformer, const string &key_action);
	static unique_ptr<TransformResultValue> TransformDeleteActionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static string TransformDeleteAction(PEGTransformer &transformer, const string &key_action);
	static unique_ptr<TransformResultValue> TransformKeyActionInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNoKeyActionInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static string TransformNoKeyAction(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRestrictKeyActionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static string TransformRestrictKeyAction(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCascadeKeyActionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static string TransformCascadeKeyAction(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSetNullKeyActionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static string TransformSetNullKeyAction(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSetDefaultKeyActionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static string TransformSetDefaultKeyAction(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTopLevelConstraintInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<Constraint> TransformTopLevelConstraint(PEGTransformer &transformer, const bool &has_result,
	                                                          unique_ptr<Constraint> top_level_constraint_list);
	static unique_ptr<TransformResultValue> TransformTopLevelConstraintListInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<Constraint> TransformTopLevelConstraintList(PEGTransformer &transformer,
	                                                              ParseResult &choice_result);
	static unique_ptr<TransformResultValue> TransformTopPrimaryKeyConstraintInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<Constraint> TransformTopPrimaryKeyConstraint(PEGTransformer &transformer,
	                                                               const vector<string> &column_id_list);
	static unique_ptr<TransformResultValue> TransformTopUniqueConstraintInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<Constraint> TransformTopUniqueConstraint(PEGTransformer &transformer,
	                                                           const vector<string> &column_id_list);
	static unique_ptr<TransformResultValue> TransformTopForeignKeyConstraintInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<Constraint> TransformTopForeignKeyConstraint(PEGTransformer &transformer,
	                                                               const vector<string> &column_id_list,
	                                                               ColumnConstraintEntry foreign_key_constraint);
	static unique_ptr<TransformResultValue> TransformColumnIdListInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static vector<string> TransformColumnIdList(PEGTransformer &transformer, const vector<Identifier> &col_id);
	static unique_ptr<TransformResultValue> TransformDottedIdentifierInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static vector<string> TransformDottedIdentifier(PEGTransformer &transformer, const Identifier &identifier,
	                                                const optional<vector<string>> &dot_col_label);
	static unique_ptr<TransformResultValue> TransformDotColLabelInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColIdInternal(PEGTransformer &transformer,
	                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColIdOrStringInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static Identifier TransformColIdOrString(PEGTransformer &transformer, ParseResult &choice_result);
	static unique_ptr<TransformResultValue> TransformTypeFuncNameInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformGeneratedColumnInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static GeneratedColumnDefinition TransformGeneratedColumn(PEGTransformer &transformer, const bool &has_result,
	                                                          unique_ptr<ParsedExpression> expression,
	                                                          const optional<bool> &generated_column_type);
	static unique_ptr<TransformResultValue> TransformGeneratedColumnTypeInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCommitActionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformCommitAction(PEGTransformer &transformer, const bool &preserve_or_delete);
	static unique_ptr<TransformResultValue> TransformPreserveOrDeleteInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPreserveRowsInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformPreserveRows(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDeleteRowsInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static bool TransformDeleteRows(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformVirtualGeneratedColumnInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static bool TransformVirtualGeneratedColumn(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformStoredGeneratedColumnInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static bool TransformStoredGeneratedColumn(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCreateTriggerStmtInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<CreateStatement>
	TransformCreateTriggerStmt(PEGTransformer &transformer, const optional<bool> &if_not_exists,
	                           const Identifier &trigger_name, const TriggerTiming &trigger_timing,
	                           const TriggerEventInfo &trigger_event, unique_ptr<BaseTableRef> base_table_name,
	                           const optional<TriggerTableReferencingInfo> &referencing_clause,
	                           const optional<TriggerForEach> &for_each_clause, unique_ptr<SQLStatement> trigger_body);
	static unique_ptr<TransformResultValue> TransformTriggerBodyInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTriggerNameInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static Identifier TransformTriggerName(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformReferencingClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static TriggerTableReferencingInfo
	TransformReferencingClause(PEGTransformer &transformer, const TriggerTableReferencingInfo &referencing_item,
	                           const optional<TriggerTableReferencingInfo> &referencing_item_1);
	static unique_ptr<TransformResultValue> TransformReferencingItemInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformReferencingNewTableAsInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static TriggerTableReferencingInfo TransformReferencingNewTableAs(PEGTransformer &transformer,
	                                                                  const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformReferencingOldTableAsInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static TriggerTableReferencingInfo TransformReferencingOldTableAs(PEGTransformer &transformer,
	                                                                  const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformTriggerTimingInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTriggerBeforeInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static TriggerTiming TransformTriggerBefore(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTriggerAfterInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static TriggerTiming TransformTriggerAfter(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTriggerInsteadOfInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static TriggerTiming TransformTriggerInsteadOf(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTriggerEventInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTriggerEventInsertInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static TriggerEventInfo TransformTriggerEventInsert(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTriggerEventDeleteInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static TriggerEventInfo TransformTriggerEventDelete(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTriggerEventUpdateInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static TriggerEventInfo TransformTriggerEventUpdate(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTriggerEventUpdateOfInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static TriggerEventInfo TransformTriggerEventUpdateOf(PEGTransformer &transformer,
	                                                      const vector<string> &trigger_column_list);
	static unique_ptr<TransformResultValue> TransformTriggerColumnListInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<string> TransformTriggerColumnList(PEGTransformer &transformer, const vector<Identifier> &col_id);
	static unique_ptr<TransformResultValue> TransformForEachClauseInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformForEachRowInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static TriggerForEach TransformForEachRow(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformForEachStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static TriggerForEach TransformForEachStatement(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCreateTypeStmtInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<CreateStatement> TransformCreateTypeStmt(PEGTransformer &transformer,
	                                                           const optional<bool> &if_not_exists,
	                                                           const QualifiedName &qualified_name,
	                                                           unique_ptr<CreateTypeInfo> create_type);
	static unique_ptr<TransformResultValue> TransformCreateTypeInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCreateTypeFromTypeInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<CreateTypeInfo> TransformCreateTypeFromType(PEGTransformer &transformer, const LogicalType &type);
	static unique_ptr<TransformResultValue> TransformEnumSelectTypeInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<CreateTypeInfo> TransformEnumSelectType(PEGTransformer &transformer,
	                                                          unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformEnumStringLiteralListInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<CreateTypeInfo> TransformEnumStringLiteralList(PEGTransformer &transformer,
	                                                                 const optional<vector<string>> &string_literal);
	static unique_ptr<TransformResultValue> TransformCreateViewStmtInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<CreateStatement>
	TransformCreateViewStmt(PEGTransformer &transformer, const optional<bool> &create_recursive,
	                        const optional<bool> &if_not_exists, const QualifiedName &qualified_name,
	                        const optional<vector<string>> &insert_column_list,
	                        optional<case_insensitive_map_t<unique_ptr<ParsedExpression>>> with_list,
	                        unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformCreateRecursiveInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static bool TransformCreateRecursive(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDeallocateStatementInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformDeallocateStatement(PEGTransformer &transformer,
	                                                             const optional<bool> &deallocate_prepare,
	                                                             const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformDeallocatePrepareInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static bool TransformDeallocatePrepare(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDeleteStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformDeleteStatement(PEGTransformer &transformer, optional<CommonTableExpressionMap> with_clause,
	                         unique_ptr<BaseTableRef> target_opt_alias,
	                         optional<vector<unique_ptr<TableRef>>> delete_using_clause,
	                         optional<unique_ptr<ParsedExpression>> where_clause,
	                         optional<vector<unique_ptr<ParsedExpression>>> returning_clause);
	static unique_ptr<TransformResultValue> TransformTruncateStatementInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformTruncateStatement(PEGTransformer &transformer, const bool &has_result,
	                                                           unique_ptr<BaseTableRef> base_table_name);
	static unique_ptr<TransformResultValue> TransformTargetOptAliasInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<BaseTableRef> TransformTargetOptAlias(PEGTransformer &transformer,
	                                                        unique_ptr<BaseTableRef> base_table_name,
	                                                        const bool &has_result, const optional<Identifier> &col_id);
	static unique_ptr<TransformResultValue> TransformDeleteUsingClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<unique_ptr<TableRef>> TransformDeleteUsingClause(PEGTransformer &transformer,
	                                                               vector<unique_ptr<TableRef>> table_ref);
	static unique_ptr<TransformResultValue> TransformDescribeStatementInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformDescribeStatement(PEGTransformer &transformer,
	                                                              unique_ptr<QueryNode> child);
	static unique_ptr<TransformResultValue> TransformShowSelectInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<QueryNode> TransformShowSelect(PEGTransformer &transformer,
	                                                 const ShowType &show_or_describe_or_summarize,
	                                                 unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformShowAllTablesInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<QueryNode> TransformShowAllTables(PEGTransformer &transformer, const ShowType &show_or_describe);
	static unique_ptr<TransformResultValue> TransformShowQualifiedNameInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<QueryNode> TransformShowQualifiedName(PEGTransformer &transformer,
	                                                        const ShowType &show_or_describe_or_summarize,
	                                                        optional<DescribeTarget> describe_target);
	static unique_ptr<TransformResultValue> TransformShowTablesInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<QueryNode> TransformShowTables(PEGTransformer &transformer, const ShowType &show_or_describe,
	                                                 const QualifiedName &qualified_name);
	static unique_ptr<TransformResultValue> TransformDescribeTargetInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDescribeBaseTableNameInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static DescribeTarget TransformDescribeBaseTableName(PEGTransformer &transformer,
	                                                     unique_ptr<BaseTableRef> base_table_name);
	static unique_ptr<TransformResultValue> TransformDescribeStringLiteralInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static DescribeTarget TransformDescribeStringLiteral(PEGTransformer &transformer, const string &string_literal);
	static unique_ptr<TransformResultValue> TransformShowOrDescribeOrSummarizeInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSummarizeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSummarizeRuleInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static ShowType TransformSummarizeRule(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformShowOrDescribeInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformShowRuleInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static ShowType TransformShowRule(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDescribeRuleInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDescribeLongRuleInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static ShowType TransformDescribeLongRule(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDescRuleInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static ShowType TransformDescRule(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDetachStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformDetachStatement(PEGTransformer &transformer, const bool &has_result,
	                                                         const optional<bool> &if_exists,
	                                                         const Identifier &catalog_name);
	static unique_ptr<TransformResultValue> TransformDropStatementInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformDropStatement(PEGTransformer &transformer,
	                                                       unique_ptr<DropStatement> drop_entries,
	                                                       const optional<bool> &drop_behavior);
	static unique_ptr<TransformResultValue> TransformDropEntriesInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDropTriggerInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropTrigger(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                      const Identifier &trigger_name,
	                                                      unique_ptr<BaseTableRef> base_table_name);
	static unique_ptr<TransformResultValue> TransformDropTableInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropTable(PEGTransformer &transformer, const CatalogType &table_or_view,
	                                                    const optional<bool> &if_exists,
	                                                    vector<unique_ptr<BaseTableRef>> base_table_name);
	static unique_ptr<TransformResultValue> TransformDropTableFunctionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropTableFunction(PEGTransformer &transformer,
	                                                            const CatalogType &comment_macro_table,
	                                                            const optional<bool> &if_exists,
	                                                            const vector<Identifier> &table_function_name);
	static unique_ptr<TransformResultValue> TransformDropFunctionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropFunction(PEGTransformer &transformer, const bool &function_type_macro,
	                                                       const optional<bool> &if_exists,
	                                                       const vector<QualifiedName> &function_identifier);
	static unique_ptr<TransformResultValue> TransformDropSchemaInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropSchema(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                     const vector<QualifiedName> &qualified_name);
	static unique_ptr<TransformResultValue> TransformDropIndexInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropIndex(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                    const vector<QualifiedName> &qualified_index_name);
	static unique_ptr<TransformResultValue> TransformQualifiedIndexNameInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformQualifiedIndexNameStringInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static QualifiedName TransformQualifiedIndexNameString(PEGTransformer &transformer, const Identifier &index_name);
	static unique_ptr<TransformResultValue> TransformSchemaReservedIndexInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static QualifiedName TransformSchemaReservedIndex(PEGTransformer &transformer,
	                                                  const Identifier &schema_qualification,
	                                                  const Identifier &reserved_index_name);
	static unique_ptr<TransformResultValue> TransformCatalogReservedSchemaIndexInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static QualifiedName TransformCatalogReservedSchemaIndex(PEGTransformer &transformer,
	                                                         const Identifier &catalog_qualification,
	                                                         const Identifier &reserved_schema_qualification,
	                                                         const Identifier &reserved_index_name);
	static unique_ptr<TransformResultValue> TransformDropSequenceInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropSequence(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                       const vector<QualifiedName> &qualified_sequence_name);
	static unique_ptr<TransformResultValue> TransformDropCollationInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropCollation(PEGTransformer &transformer,
	                                                        const optional<bool> &if_exists,
	                                                        const vector<Identifier> &collation_name);
	static unique_ptr<TransformResultValue> TransformDropTypeInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropType(PEGTransformer &transformer, const optional<bool> &if_exists,
	                                                   const vector<QualifiedName> &qualified_type_name);
	static unique_ptr<TransformResultValue> TransformDropSecretInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<DropStatement> TransformDropSecret(PEGTransformer &transformer,
	                                                     const optional<SecretPersistType> &temporary,
	                                                     const optional<bool> &if_exists, const Identifier &secret_name,
	                                                     const optional<Identifier> &drop_secret_storage);
	static unique_ptr<TransformResultValue> TransformTableOrViewInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformMaterializedViewEntryInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static CatalogType TransformMaterializedViewEntry(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFunctionTypeMacroInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformFunctionTypeMacroKeywordInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static bool TransformFunctionTypeMacroKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFunctionTypeFunctionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static bool TransformFunctionTypeFunction(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDropBehaviorInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCascadeDropBehaviorInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static bool TransformCascadeDropBehavior(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRestrictDropBehaviorInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static bool TransformRestrictDropBehavior(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIfExistsInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static bool TransformIfExists(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDropSecretStorageInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static Identifier TransformDropSecretStorage(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformExecuteStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformExecuteStatement(PEGTransformer &transformer, const Identifier &identifier,
	                          optional<vector<FunctionArgument>> table_function_arguments);
	static unique_ptr<TransformResultValue> TransformExplainStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformExplainStatement(PEGTransformer &transformer, const optional<bool> &explain_analyze,
	                          const optional<vector<GenericCopyOption>> &explain_option_list,
	                          unique_ptr<SQLStatement> explainable_statements);
	static unique_ptr<TransformResultValue> TransformExplainAnalyzeInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformExplainAnalyze(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformExplainOptionListInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<GenericCopyOption> TransformExplainOptionList(PEGTransformer &transformer,
	                                                            const vector<GenericCopyOption> &explain_option);
	static unique_ptr<TransformResultValue> TransformExplainOptionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static GenericCopyOption TransformExplainOption(PEGTransformer &transformer, const Identifier &explain_option_name,
	                                                optional<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformExplainSelectStatementInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformExplainSelectStatement(PEGTransformer &transformer, unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformExplainableStatementsInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExportStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformExportStatement(PEGTransformer &transformer, const optional<string> &export_source,
	                         const string &string_literal,
	                         const optional<vector<GenericCopyOption>> &generic_copy_option_list);
	static unique_ptr<TransformResultValue> TransformExportSourceInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static string TransformExportSource(PEGTransformer &transformer, const Identifier &catalog_name);
	static unique_ptr<TransformResultValue> TransformImportStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformImportStatement(PEGTransformer &transformer, const string &string_literal);
	static unique_ptr<TransformResultValue> TransformColumnReferenceInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformColumnReference(PEGTransformer &transformer,
	                                                             unique_ptr<ColumnRefExpression> child);
	static unique_ptr<TransformResultValue>
	TransformCatalogReservedSchemaTableColumnNameInternal(PEGTransformer &transformer, ParseResult &parse_result);
	static unique_ptr<ColumnRefExpression>
	TransformCatalogReservedSchemaTableColumnName(PEGTransformer &transformer, const Identifier &catalog_qualification,
	                                              const Identifier &reserved_schema_qualification,
	                                              const Identifier &reserved_table_qualification,
	                                              const Identifier &reserved_column_name);
	static unique_ptr<TransformResultValue> TransformSchemaReservedTableColumnNameInternal(PEGTransformer &transformer,
	                                                                                       ParseResult &parse_result);
	static unique_ptr<ColumnRefExpression>
	TransformSchemaReservedTableColumnName(PEGTransformer &transformer, const Identifier &schema_qualification,
	                                       const Identifier &reserved_table_qualification,
	                                       const Identifier &reserved_column_name);
	static unique_ptr<TransformResultValue> TransformTableReservedColumnNameInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ColumnRefExpression> TransformTableReservedColumnName(PEGTransformer &transformer,
	                                                                        const Identifier &table_qualification,
	                                                                        const Identifier &reserved_column_name);
	static unique_ptr<TransformResultValue> TransformFunctionExpressionInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformFunctionExpression(PEGTransformer &transformer, const QualifiedName &function_identifier,
	                            MethodArguments function_expression_arguments,
	                            optional<vector<OrderByNode>> within_group_clause,
	                            optional<unique_ptr<ParsedExpression>> filter_clause, const bool &has_result,
	                            optional<unique_ptr<WindowExpression>> over_clause);
	static unique_ptr<TransformResultValue> TransformFunctionExpressionArgumentsInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformFunctionExpressionArgumentListInternal(PEGTransformer &transformer,
	                                                                                        ParseResult &parse_result);
	static MethodArguments
	TransformFunctionExpressionArgumentList(PEGTransformer &transformer, const optional<bool> &distinct_or_all,
	                                        optional<vector<FunctionArgument>> function_argument_list,
	                                        optional<vector<OrderByNode>> order_by_clause,
	                                        const optional<bool> &ignore_or_respect_nulls);
	static unique_ptr<TransformResultValue> TransformFunctionArgumentListInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformFunctionIdentifierInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static QualifiedName TransformFunctionIdentifier(PEGTransformer &transformer, ParseResult &choice_result);
	static unique_ptr<TransformResultValue>
	TransformCatalogReservedSchemaFunctionNameInternal(PEGTransformer &transformer, ParseResult &parse_result);
	static QualifiedName
	TransformCatalogReservedSchemaFunctionName(PEGTransformer &transformer, const Identifier &catalog_qualification,
	                                           const optional<Identifier> &reserved_schema_qualification,
	                                           const Identifier &reserved_function_name);
	static unique_ptr<TransformResultValue> TransformSchemaReservedFunctionNameInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static QualifiedName TransformSchemaReservedFunctionName(PEGTransformer &transformer,
	                                                         const Identifier &schema_qualification,
	                                                         const Identifier &reserved_function_name);
	static unique_ptr<TransformResultValue> TransformDistinctOrAllInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDistinctKeywordInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static bool TransformDistinctKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformAllKeywordInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static bool TransformAllKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformWithinGroupClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<OrderByNode> TransformWithinGroupClause(PEGTransformer &transformer,
	                                                      vector<OrderByNode> order_by_clause);
	static unique_ptr<TransformResultValue> TransformFilterClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformFilterClause(PEGTransformer &transformer,
	                                                          unique_ptr<ParsedExpression> filter_clause_expression);
	static unique_ptr<TransformResultValue> TransformFilterClauseExpressionInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformFilterClauseExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> filter_clause_contents);
	static unique_ptr<TransformResultValue> TransformFilterClauseContentsInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformFilterClauseContents(PEGTransformer &transformer,
	                                                                  const bool &has_result,
	                                                                  unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformIgnoreOrRespectNullsInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIgnoreNullsInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformIgnoreNulls(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRespectNullsInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformRespectNulls(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformParenthesisExpressionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformParenthesisExpression(PEGTransformer &transformer,
	                               optional<vector<unique_ptr<ParsedExpression>>> expression);
	static unique_ptr<TransformResultValue> TransformLiteralExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformLiteralExpression(PEGTransformer &transformer,
	                                                               ParseResult &choice_result);
	static unique_ptr<TransformResultValue> TransformConstantLiteralInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformConstantLiteral(PEGTransformer &transformer, const Value &child);
	static unique_ptr<TransformResultValue> TransformNullLiteralInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static Value TransformNullLiteral(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTrueLiteralInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static Value TransformTrueLiteral(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFalseLiteralInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static Value TransformFalseLiteral(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCastExpressionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformCastExpression(PEGTransformer &transformer, const bool &cast_or_try_cast, CastArguments cast_arguments);
	static unique_ptr<TransformResultValue> TransformCastArgumentsInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static CastArguments TransformCastArguments(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                                            const LogicalType &type);
	static unique_ptr<TransformResultValue> TransformCastOrTryCastInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCastKeywordInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformCastKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTryCastKeywordInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformTryCastKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformColIdDotInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static string TransformColIdDot(PEGTransformer &transformer, const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformStarExpressionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformStarExpression(PEGTransformer &transformer, const optional<vector<string>> &star_qualifier_list,
	                        const optional<qualified_column_set_t> &exclude_list,
	                        optional<case_insensitive_map_t<unique_ptr<ParsedExpression>>> replace_list,
	                        const optional<qualified_column_map_t<string>> &rename_list);
	static unique_ptr<TransformResultValue> TransformStarQualifierListInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExcludeListInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static qualified_column_set_t TransformExcludeList(PEGTransformer &transformer,
	                                                   const qualified_column_set_t &exclude_names);
	static unique_ptr<TransformResultValue> TransformExcludeNamesInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExcludeNameListInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static qualified_column_set_t TransformExcludeNameList(PEGTransformer &transformer,
	                                                       const vector<QualifiedColumnName> &exclude_name);
	static unique_ptr<TransformResultValue> TransformExcludeNameSingleInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static qualified_column_set_t TransformExcludeNameSingle(PEGTransformer &transformer,
	                                                         const QualifiedColumnName &exclude_name);
	static unique_ptr<TransformResultValue> TransformExcludeNameInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExcludeDottedNameInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static QualifiedColumnName TransformExcludeDottedName(PEGTransformer &transformer,
	                                                      const vector<string> &dotted_identifier);
	static unique_ptr<TransformResultValue> TransformExcludeColumnNameInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static QualifiedColumnName TransformExcludeColumnName(PEGTransformer &transformer,
	                                                      const Identifier &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformReplaceListInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static case_insensitive_map_t<unique_ptr<ParsedExpression>>
	TransformReplaceList(PEGTransformer &transformer,
	                     case_insensitive_map_t<unique_ptr<ParsedExpression>> replace_entries);
	static unique_ptr<TransformResultValue> TransformReplaceEntriesInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformReplaceEntrySingleInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static case_insensitive_map_t<unique_ptr<ParsedExpression>>
	TransformReplaceEntrySingle(PEGTransformer &transformer, pair<string, unique_ptr<ParsedExpression>> replace_entry);
	static unique_ptr<TransformResultValue> TransformReplaceEntryListInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static case_insensitive_map_t<unique_ptr<ParsedExpression>>
	TransformReplaceEntryList(PEGTransformer &transformer,
	                          vector<pair<string, unique_ptr<ParsedExpression>>> replace_entry);
	static unique_ptr<TransformResultValue> TransformReplaceEntryInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static pair<string, unique_ptr<ParsedExpression>>
	TransformReplaceEntry(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                      unique_ptr<ParsedExpression> column_reference);
	static unique_ptr<TransformResultValue> TransformRenameListInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static qualified_column_map_t<string> TransformRenameList(PEGTransformer &transformer,
	                                                          const qualified_column_map_t<string> &rename_entries);
	static unique_ptr<TransformResultValue> TransformRenameEntriesInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformRenameEntryListInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static qualified_column_map_t<string>
	TransformRenameEntryList(PEGTransformer &transformer,
	                         const vector<pair<QualifiedColumnName, string>> &rename_entry);
	static unique_ptr<TransformResultValue> TransformSingleRenameEntryInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static qualified_column_map_t<string>
	TransformSingleRenameEntry(PEGTransformer &transformer, const pair<QualifiedColumnName, string> &rename_entry);
	static unique_ptr<TransformResultValue> TransformRenameEntryInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static pair<QualifiedColumnName, string> TransformRenameEntry(PEGTransformer &transformer,
	                                                              const QualifiedColumnName &exclude_name,
	                                                              const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformSubqueryExpressionInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformSubqueryExpression(PEGTransformer &transformer,
	                                                                const optional<bool> &subquery_not,
	                                                                const optional<bool> &subquery_exists,
	                                                                unique_ptr<TableRef> subquery_reference);
	static unique_ptr<TransformResultValue> TransformSubqueryNotInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformSubqueryNot(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSubqueryExistsInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformSubqueryExists(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCaseExpressionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformCaseExpression(PEGTransformer &transformer,
	                                                            optional<unique_ptr<ParsedExpression>> expression,
	                                                            vector<CaseCheck> case_when_then,
	                                                            optional<unique_ptr<ParsedExpression>> case_else);
	static unique_ptr<TransformResultValue> TransformCaseWhenThenInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static CaseCheck TransformCaseWhenThen(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                                       unique_ptr<ParsedExpression> expression_1);
	static unique_ptr<TransformResultValue> TransformCaseElseInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformCaseElse(PEGTransformer &transformer,
	                                                      unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformTypeLiteralInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformTypeLiteral(PEGTransformer &transformer, const Identifier &col_id,
	                                                         const string &string_literal);
	static unique_ptr<TransformResultValue> TransformIntervalLiteralInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIntervalLiteral(PEGTransformer &transformer,
	                                                             unique_ptr<ParsedExpression> interval_parameter,
	                                                             const optional<DatePartSpecifier> &interval);
	static unique_ptr<TransformResultValue> TransformIntervalParameterInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIntervalStringParameterInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIntervalStringParameter(PEGTransformer &transformer,
	                                                                     const string &string_literal);
	static unique_ptr<TransformResultValue> TransformFrameClauseInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static WindowFrame TransformFrameClause(PEGTransformer &transformer, const string &framing,
	                                        vector<WindowBoundaryExpression> frame_extent,
	                                        const optional<WindowExcludeMode> &window_exclude_clause);
	static unique_ptr<TransformResultValue> TransformFramingInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformRowsFramingInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static string TransformRowsFraming(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRangeFramingInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static string TransformRangeFraming(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGroupsFramingInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static string TransformGroupsFraming(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFrameExtentInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSingleFrameExtentInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<WindowBoundaryExpression> TransformSingleFrameExtent(PEGTransformer &transformer,
	                                                                   WindowBoundaryExpression frame_bound);
	static unique_ptr<TransformResultValue> TransformBetweenFrameExtentInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static vector<WindowBoundaryExpression> TransformBetweenFrameExtent(PEGTransformer &transformer,
	                                                                    WindowBoundaryExpression frame_bound,
	                                                                    WindowBoundaryExpression frame_bound_1);
	static unique_ptr<TransformResultValue> TransformFrameBoundInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformFrameUnboundedInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static WindowBoundaryExpression TransformFrameUnbounded(PEGTransformer &transformer,
	                                                        const bool &preceding_or_following);
	static unique_ptr<TransformResultValue> TransformFrameExpressionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static WindowBoundaryExpression TransformFrameExpression(PEGTransformer &transformer,
	                                                         unique_ptr<ParsedExpression> expression,
	                                                         const bool &preceding_or_following);
	static unique_ptr<TransformResultValue> TransformFrameCurrentRowInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static WindowBoundaryExpression TransformFrameCurrentRow(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformPrecedingOrFollowingInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPrecedingFrameInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformPrecedingFrame(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFollowingFrameInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformFollowingFrame(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformWindowExcludeClauseInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static WindowExcludeMode TransformWindowExcludeClause(PEGTransformer &transformer,
	                                                      const WindowExcludeMode &window_exclude_element);
	static unique_ptr<TransformResultValue> TransformWindowExcludeElementInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExcludeCurrentRowInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static WindowExcludeMode TransformExcludeCurrentRow(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformExcludeGroupInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static WindowExcludeMode TransformExcludeGroup(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformExcludeTiesInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static WindowExcludeMode TransformExcludeTies(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformExcludeNoOthersInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static WindowExcludeMode TransformExcludeNoOthers(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformWindowFrameInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<WindowExpression> TransformWindowFrame(PEGTransformer &transformer, ParseResult &choice_result);
	static unique_ptr<TransformResultValue> TransformParensIdentifierInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<WindowExpression> TransformParensIdentifier(PEGTransformer &transformer,
	                                                              const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformWindowFrameDefinitionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformWindowFrameNameContentsParensInternal(PEGTransformer &transformer,
	                                                                                       ParseResult &parse_result);
	static unique_ptr<WindowExpression>
	TransformWindowFrameNameContentsParens(PEGTransformer &transformer,
	                                       unique_ptr<WindowExpression> window_frame_name_contents);
	static unique_ptr<TransformResultValue> TransformWindowFrameNameContentsInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<WindowExpression>
	TransformWindowFrameNameContents(PEGTransformer &transformer, const optional<Identifier> &base_window_name,
	                                 unique_ptr<WindowExpression> window_frame_contents);
	static unique_ptr<TransformResultValue> TransformWindowFrameContentsParensInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<WindowExpression>
	TransformWindowFrameContentsParens(PEGTransformer &transformer, unique_ptr<WindowExpression> window_frame_contents);
	static unique_ptr<TransformResultValue> TransformWindowFrameContentsInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<WindowExpression>
	TransformWindowFrameContents(PEGTransformer &transformer,
	                             optional<vector<unique_ptr<ParsedExpression>>> window_partition,
	                             optional<vector<OrderByNode>> order_by_clause, optional<WindowFrame> frame_clause);
	static unique_ptr<TransformResultValue> TransformBaseWindowNameInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static Identifier TransformBaseWindowName(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformWindowPartitionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformWindowPartition(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformListExpressionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformArrayBoundedListExpressionInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformArrayBoundedListExpression(PEGTransformer &transformer, const bool &has_result,
	                                    vector<unique_ptr<ParsedExpression>> bounded_list_expression);
	static unique_ptr<TransformResultValue> TransformArrayParensSelectInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformArrayParensSelect(PEGTransformer &transformer, unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformBoundedListExpressionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformBoundedListExpression(PEGTransformer &transformer,
	                               optional<vector<unique_ptr<ParsedExpression>>> expression);
	static unique_ptr<TransformResultValue> TransformStructExpressionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformStructExpression(PEGTransformer &transformer,
	                                                              optional<vector<FunctionArgument>> struct_field);
	static unique_ptr<TransformResultValue> TransformStructFieldInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static FunctionArgument TransformStructField(PEGTransformer &transformer, const Identifier &col_id_or_string,
	                                             unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformMapExpressionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformMapExpression(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> map_struct_expression);
	static unique_ptr<TransformResultValue> TransformMapStructExpressionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformMapStructExpression(PEGTransformer &transformer,
	                             optional<vector<vector<unique_ptr<ParsedExpression>>>> map_struct_field);
	static unique_ptr<TransformResultValue> TransformMapStructFieldInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformMapStructField(PEGTransformer &transformer,
	                                                                    unique_ptr<ParsedExpression> expression,
	                                                                    unique_ptr<ParsedExpression> expression_1);
	static unique_ptr<TransformResultValue> TransformGroupingExpressionInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformGroupingExpression(PEGTransformer &transformer, const bool &grouping_or_grouping_id,
	                            optional<vector<unique_ptr<ParsedExpression>>> expression);
	static unique_ptr<TransformResultValue> TransformGroupingOrGroupingIdInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformGroupingKeywordInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static bool TransformGroupingKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGroupingIdKeywordInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static bool TransformGroupingIdKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformParameterInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformQuestionMarkNumberedParameterInternal(PEGTransformer &transformer,
	                                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformQuestionMarkNumberedParameter(PEGTransformer &transformer, unique_ptr<ParsedExpression> number_literal);
	static unique_ptr<TransformResultValue> TransformAnonymousParameterInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformAnonymousParameter(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNumberedParameterInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformNumberedParameter(PEGTransformer &transformer,
	                                                               unique_ptr<ParsedExpression> number_literal);
	static unique_ptr<TransformResultValue> TransformColLabelParameterInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformColLabelParameter(PEGTransformer &transformer,
	                                                               const string &col_label);
	static unique_ptr<TransformResultValue> TransformPositionalExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformPositionalExpression(PEGTransformer &transformer,
	                                                                  unique_ptr<ParsedExpression> number_literal);
	static unique_ptr<TransformResultValue> TransformDefaultExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDefaultExpression(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformListComprehensionExpressionInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformListComprehensionExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                                     const vector<Identifier> &col_id_or_string,
	                                     unique_ptr<ParsedExpression> expression_1,
	                                     optional<unique_ptr<ParsedExpression>> list_comprehension_filter);
	static unique_ptr<TransformResultValue> TransformListComprehensionFilterInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformListComprehensionFilter(PEGTransformer &transformer,
	                                                                     unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformParensExpressionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformParensExpression(PEGTransformer &transformer,
	                                                              unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformSingleExpressionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColumnDefaultExprInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLambdaArrowExpressionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformLambdaArrowExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> logical_or_expression,
	                               optional<vector<unique_ptr<ParsedExpression>>> single_arrow_pair);
	static unique_ptr<TransformResultValue> TransformSingleArrowPairInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLogicalOrExpressionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformLogicalOrExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> logical_and_expression,
	                             optional<vector<unique_ptr<ParsedExpression>>> logical_or_expression_tail);
	static unique_ptr<TransformResultValue> TransformLogicalOrExpressionTailInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColDefOrExprInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformColDefOrExpr(PEGTransformer &transformer, unique_ptr<ParsedExpression> col_def_and_expr,
	                      optional<vector<unique_ptr<ParsedExpression>>> col_def_or_expression_tail);
	static unique_ptr<TransformResultValue> TransformColDefOrExpressionTailInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLogicalAndExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformLogicalAndExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> logical_not_expression,
	                              optional<vector<unique_ptr<ParsedExpression>>> logical_and_expression_tail);
	static unique_ptr<TransformResultValue> TransformLogicalAndExpressionTailInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColDefAndExprInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformColDefAndExpr(PEGTransformer &transformer, unique_ptr<ParsedExpression> is_distinct_from_expression,
	                       optional<vector<unique_ptr<ParsedExpression>>> col_def_and_expression_tail);
	static unique_ptr<TransformResultValue> TransformColDefAndExpressionTailInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLogicalNotExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformLogicalNotExpression(PEGTransformer &transformer,
	                                                                  optional<vector<bool>> not_expression,
	                                                                  unique_ptr<ParsedExpression> is_expression);
	static unique_ptr<TransformResultValue> TransformNotExpressionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNotKeywordInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static bool TransformNotKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIsExpressionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIsExpression(PEGTransformer &transformer,
	                                                          unique_ptr<ParsedExpression> is_distinct_from_expression,
	                                                          optional<vector<unique_ptr<ParsedExpression>>> is_test);
	static unique_ptr<TransformResultValue> TransformIsTestInternal(PEGTransformer &transformer,
	                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIsLiteralInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIsLiteral(PEGTransformer &transformer, const bool &has_result,
	                                                       const Value &is_literal_value);
	static unique_ptr<TransformResultValue> TransformIsLiteralValueInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformUnknownLiteralInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static Value TransformUnknownLiteral(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNotNullInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNotNullKeywordInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformNotNullKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNotNullOperatorInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformNotNullOperator(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIsNullInternal(PEGTransformer &transformer,
	                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIsNullOperatorInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformIsNullOperator(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformIsDistinctFromExpressionInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformIsDistinctFromExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> comparison_expression,
	                                  optional<vector<IsDistinctFromTail>> is_distinct_from_tail);
	static unique_ptr<TransformResultValue> TransformIsDistinctFromTailInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static IsDistinctFromTail TransformIsDistinctFromTail(PEGTransformer &transformer,
	                                                      const ExpressionType &is_distinct_from_op,
	                                                      unique_ptr<ParsedExpression> comparison_expression);
	static unique_ptr<TransformResultValue> TransformIsDistinctFromOpInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static ExpressionType TransformIsDistinctFromOp(PEGTransformer &transformer, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformComparisonExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformComparisonExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> between_in_like_expression,
	                              optional<vector<ComparisonExpressionTail>> comparison_expression_tail);
	static unique_ptr<TransformResultValue> TransformComparisonExpressionTailInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static ComparisonExpressionTail
	TransformComparisonExpressionTail(PEGTransformer &transformer, const ExpressionType &comparison_operator,
	                                  optional<vector<bool>> not_expression,
	                                  unique_ptr<ParsedExpression> between_in_like_expression);
	static unique_ptr<TransformResultValue> TransformComparisonOperatorInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOperatorEqualInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static ExpressionType TransformOperatorEqual(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOperatorNotEqualInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static ExpressionType TransformOperatorNotEqual(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOperatorLessThanInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static ExpressionType TransformOperatorLessThan(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOperatorGreaterThanInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static ExpressionType TransformOperatorGreaterThan(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOperatorLessThanEqualsInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static ExpressionType TransformOperatorLessThanEquals(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOperatorGreaterThanEqualsInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static ExpressionType TransformOperatorGreaterThanEquals(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformBetweenInLikeExpressionInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformBetweenInLikeExpression(PEGTransformer &transformer,
	                                 unique_ptr<ParsedExpression> other_operator_expression,
	                                 optional<BetweenInLikeOperator> between_in_like_op);
	static unique_ptr<TransformResultValue> TransformBetweenInLikeOpInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static BetweenInLikeOperator TransformBetweenInLikeOp(PEGTransformer &transformer, const bool &has_result,
	                                                      unique_ptr<ParsedExpression> between_in_like_op_expression);
	static unique_ptr<TransformResultValue> TransformBetweenInLikeOpExpressionInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLikeClauseInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformLikeClause(PEGTransformer &transformer, const string &like_variations,
	                                                        unique_ptr<ParsedExpression> other_operator_expression,
	                                                        optional<unique_ptr<ParsedExpression>> escape_clause);
	static unique_ptr<TransformResultValue> TransformEscapeClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformEscapeClause(PEGTransformer &transformer,
	                                                          unique_ptr<ParsedExpression> comparison_expression);
	static unique_ptr<TransformResultValue> TransformLikeVariationsInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLikeTokenInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static string TransformLikeToken(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformILikeTokenInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static string TransformILikeToken(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGlobTokenInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static string TransformGlobToken(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSimilarToTokenInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static string TransformSimilarToToken(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRegexMatchTokenInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static string TransformRegexMatchToken(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRegexInsensitiveMatchTokenInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static string TransformRegexInsensitiveMatchToken(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNotILikeOpInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static string TransformNotILikeOp(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNotLikeOpInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static string TransformNotLikeOp(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNotRegexInsensitiveMatchOpInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static string TransformNotRegexInsensitiveMatchOp(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNotSimilarToOpInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static string TransformNotSimilarToOp(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInClauseInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformInClause(PEGTransformer &transformer,
	                                                      unique_ptr<ParsedExpression> in_expression);
	static unique_ptr<TransformResultValue> TransformInExpressionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformInContainsExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformInContainsExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> other_operator_expression);
	static unique_ptr<TransformResultValue> TransformInExpressionListInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformInExpressionList(PEGTransformer &transformer,
	                                                              vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformInSelectStatementInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformInSelectStatement(PEGTransformer &transformer, unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformBetweenClauseInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformBetweenClause(PEGTransformer &transformer, unique_ptr<ParsedExpression> other_operator_expression,
	                       unique_ptr<ParsedExpression> other_operator_expression_1);
	static unique_ptr<TransformResultValue> TransformOtherOperatorExpressionInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformOtherOperatorExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> bitwise_expression,
	                                 optional<vector<OtherOperatorTail>> other_operator_tail);
	static unique_ptr<TransformResultValue> TransformOtherOperatorTailInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static OtherOperatorTail TransformOtherOperatorTail(PEGTransformer &transformer, ParsedOperator other_operator,
	                                                    unique_ptr<ParsedExpression> bitwise_expression);
	static unique_ptr<TransformResultValue> TransformOtherOperatorInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static ParsedOperator TransformOtherOperator(PEGTransformer &transformer, ParseResult &choice_result);
	static unique_ptr<TransformResultValue> TransformAnyAllOperatorInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static pair<string, bool> TransformAnyAllOperator(PEGTransformer &transformer, const string &any_op,
	                                                  const bool &any_or_all);
	static unique_ptr<TransformResultValue> TransformAnyOrAllInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSubqueryAnyInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformSubqueryAny(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSubqueryAllInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static bool TransformSubqueryAll(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInetOperatorInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformJsonOperatorInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformListOperatorInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformStringOperatorInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformQualifiedOperatorInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static string TransformQualifiedOperator(PEGTransformer &transformer, const string &qualified_operator_contents);
	static unique_ptr<TransformResultValue> TransformQualifiedOperatorContentsInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static string TransformQualifiedOperatorContents(PEGTransformer &transformer,
	                                                 const optional<vector<string>> &col_id_dot, const string &any_op);
	static unique_ptr<TransformResultValue> TransformAnyOpInternal(PEGTransformer &transformer,
	                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformBitwiseExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformBitwiseExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> additive_expression,
	                           optional<vector<BinaryExpressionTail>> bitwise_expression_tail);
	static unique_ptr<TransformResultValue> TransformBitwiseExpressionTailInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static BinaryExpressionTail TransformBitwiseExpressionTail(PEGTransformer &transformer, const string &bit_operator,
	                                                           unique_ptr<ParsedExpression> additive_expression);
	static unique_ptr<TransformResultValue> TransformBitOperatorInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformAdditiveExpressionInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformAdditiveExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> multiplicative_expression,
	                            optional<vector<BinaryExpressionTail>> additive_expression_tail);
	static unique_ptr<TransformResultValue> TransformAdditiveExpressionTailInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static BinaryExpressionTail TransformAdditiveExpressionTail(PEGTransformer &transformer, const string &term,
	                                                            unique_ptr<ParsedExpression> multiplicative_expression,
	                                                            optional_idx query_location);
	static unique_ptr<TransformResultValue> TransformTermInternal(PEGTransformer &transformer,
	                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformMultiplicativeExpressionInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformMultiplicativeExpression(PEGTransformer &transformer,
	                                  unique_ptr<ParsedExpression> exponentiation_expression,
	                                  optional<vector<BinaryExpressionTail>> multiplicative_expression_tail);
	static unique_ptr<TransformResultValue> TransformMultiplicativeExpressionTailInternal(PEGTransformer &transformer,
	                                                                                      ParseResult &parse_result);
	static BinaryExpressionTail
	TransformMultiplicativeExpressionTail(PEGTransformer &transformer, const string &factor,
	                                      unique_ptr<ParsedExpression> exponentiation_expression);
	static unique_ptr<TransformResultValue> TransformFactorInternal(PEGTransformer &transformer,
	                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExponentiationExpressionInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformExponentiationExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> collate_expression,
	                                  optional<vector<BinaryExpressionTail>> exponentiation_expression_tail);
	static unique_ptr<TransformResultValue> TransformExponentiationExpressionTailInternal(PEGTransformer &transformer,
	                                                                                      ParseResult &parse_result);
	static BinaryExpressionTail TransformExponentiationExpressionTail(PEGTransformer &transformer,
	                                                                  const string &exponent_operator,
	                                                                  unique_ptr<ParsedExpression> collate_expression);
	static unique_ptr<TransformResultValue> TransformExponentOperatorInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCollateExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformCollateExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> at_time_zone_expression,
	                           optional<vector<unique_ptr<ParsedExpression>>> collate_expression_tail);
	static unique_ptr<TransformResultValue> TransformCollateExpressionTailInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformAtTimeZoneExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformAtTimeZoneExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> prefix_expression,
	                              optional<vector<unique_ptr<ParsedExpression>>> at_time_zone_expression_tail);
	static unique_ptr<TransformResultValue> TransformAtTimeZoneExpressionTailInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPrefixOperatorInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformMinusPrefixOperatorInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPlusPrefixOperatorInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTildePrefixOperatorInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformBaseExpressionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformBaseExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> single_expression,
	                        optional<vector<unique_ptr<ParsedExpression>>> indirection_list);
	static unique_ptr<TransformResultValue> TransformIndirectionListInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIndirectionInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCastOperatorInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformCastOperator(PEGTransformer &transformer, const LogicalType &type);
	static unique_ptr<TransformResultValue> TransformDotOperatorInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDotMethodOperatorInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDotMethodOperator(PEGTransformer &transformer,
	                                                               unique_ptr<ParsedExpression> method_expression);
	static unique_ptr<TransformResultValue> TransformDotColumnOperatorInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformDotColumnOperator(PEGTransformer &transformer,
	                                                               const string &col_label);
	static unique_ptr<TransformResultValue> TransformMethodExpressionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformMethodExpression(PEGTransformer &transformer, const string &col_label,
	                                                              MethodArguments method_expression_arguments);
	static unique_ptr<TransformResultValue> TransformMethodExpressionArgumentsInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static MethodArguments TransformMethodExpressionArguments(PEGTransformer &transformer,
	                                                          MethodArguments method_expression_argument_list);
	static unique_ptr<TransformResultValue> TransformMethodExpressionArgumentListInternal(PEGTransformer &transformer,
	                                                                                      ParseResult &parse_result);
	static MethodArguments
	TransformMethodExpressionArgumentList(PEGTransformer &transformer, const optional<bool> &distinct_or_all,
	                                      optional<vector<FunctionArgument>> method_function_arguments,
	                                      optional<vector<OrderByNode>> order_by_clause,
	                                      const optional<bool> &ignore_or_respect_nulls);
	static unique_ptr<TransformResultValue> TransformMethodFunctionArgumentsInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static vector<FunctionArgument> TransformMethodFunctionArguments(PEGTransformer &transformer,
	                                                                 vector<FunctionArgument> function_argument);
	static unique_ptr<TransformResultValue> TransformSliceExpressionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformSliceExpression(PEGTransformer &transformer,
	                                                             vector<unique_ptr<ParsedExpression>> slice_bound);
	static unique_ptr<TransformResultValue> TransformSliceBoundInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformSliceBound(PEGTransformer &transformer, optional<unique_ptr<ParsedExpression>> expression,
	                    optional<unique_ptr<ParsedExpression>> end_slice_bound,
	                    optional<unique_ptr<ParsedExpression>> step_slice_bound);
	static unique_ptr<TransformResultValue> TransformEndSliceBoundInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformEndSliceBound(PEGTransformer &transformer,
	                                                           optional<unique_ptr<ParsedExpression>> end_slice_value);
	static unique_ptr<TransformResultValue> TransformEndSliceValueInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformEndSliceMinusInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformEndSliceMinus(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformStepSliceBoundInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformStepSliceBound(PEGTransformer &transformer,
	                                                            optional<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformPostfixOperatorInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformPostfixOperator(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSpecialFunctionExpressionInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCoalesceExpressionInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformCoalesceExpression(PEGTransformer &transformer,
	                                                                vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformUnpackExpressionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformUnpackExpression(PEGTransformer &transformer,
	                                                              unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformTryExpressionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformTryExpression(PEGTransformer &transformer,
	                                                           unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformColumnsExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformColumnsExpression(PEGTransformer &transformer, const bool &has_result,
	                                                               unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformExtractExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformExtractExpression(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> extract_arguments);
	static unique_ptr<TransformResultValue> TransformExtractArgumentsInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformExtractArguments(PEGTransformer &transformer,
	                                                                      unique_ptr<ParsedExpression> extract_argument,
	                                                                      unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformLambdaExpressionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformLambdaExpression(PEGTransformer &transformer,
	                                                              const vector<Identifier> &col_id_or_string,
	                                                              unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformNullIfExpressionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformNullIfExpression(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> null_if_arguments);
	static unique_ptr<TransformResultValue> TransformNullIfArgumentsInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformNullIfArguments(PEGTransformer &transformer,
	                                                                     unique_ptr<ParsedExpression> expression,
	                                                                     unique_ptr<ParsedExpression> expression_1);
	static unique_ptr<TransformResultValue> TransformPositionExpressionInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformPositionExpression(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> position_arguments);
	static unique_ptr<TransformResultValue> TransformPositionArgumentsInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformPositionArguments(PEGTransformer &transformer, unique_ptr<ParsedExpression> other_operator_expression,
	                           unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformRowExpressionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformRowExpression(PEGTransformer &transformer, optional<vector<unique_ptr<ParsedExpression>>> expression);
	static unique_ptr<TransformResultValue> TransformSubstringExpressionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformSubstringExpression(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> substring_arguments);
	static unique_ptr<TransformResultValue> TransformSubstringArgumentsInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSubstringExpressionListInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformSubstringExpressionList(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformSubstringParametersInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformSubstringParameters(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                             vector<unique_ptr<ParsedExpression>> substring_from_for);
	static unique_ptr<TransformResultValue> TransformSubstringFromForInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSubstringFromOptionalForInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformSubstringFromOptionalFor(PEGTransformer &transformer, unique_ptr<ParsedExpression> from_expression,
	                                  optional<unique_ptr<ParsedExpression>> for_expression);
	static unique_ptr<TransformResultValue> TransformSubstringForInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformSubstringFor(PEGTransformer &transformer,
	                                                                  unique_ptr<ParsedExpression> for_expression);
	static unique_ptr<TransformResultValue> TransformTrimExpressionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformTrimExpression(PEGTransformer &transformer,
	                                                            TrimArguments trim_arguments);
	static unique_ptr<TransformResultValue> TransformTrimArgumentsInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static TrimArguments TransformTrimArguments(PEGTransformer &transformer, const optional<string> &trim_direction,
	                                            optional<unique_ptr<ParsedExpression>> trim_source,
	                                            vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformTrimDirectionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTrimBothInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static string TransformTrimBoth(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTrimLeadingInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static string TransformTrimLeading(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTrimTrailingInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static string TransformTrimTrailing(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTrimSourceInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformTrimSource(PEGTransformer &transformer,
	                                                        optional<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformOverlayExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformOverlayExpression(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> overlay_arguments);
	static unique_ptr<TransformResultValue> TransformOverlayArgumentsInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOverlayParametersInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformOverlayParameters(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                           unique_ptr<ParsedExpression> expression_1, unique_ptr<ParsedExpression> from_expression,
	                           optional<unique_ptr<ParsedExpression>> for_expression);
	static unique_ptr<TransformResultValue> TransformFromExpressionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformFromExpression(PEGTransformer &transformer,
	                                                            unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformForExpressionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformForExpression(PEGTransformer &transformer,
	                                                           unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformOverlayExpressionListInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformOverlayExpressionList(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformExtractArgumentInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformExtractDatePartArgumentInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformExtractDatePartArgument(PEGTransformer &transformer,
	                                                                     const DatePartSpecifier &extract_date_part);
	static unique_ptr<TransformResultValue> TransformExtractIdentifierArgumentInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformExtractIdentifierArgument(PEGTransformer &transformer,
	                                                                       const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformExtractStringArgumentInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformExtractStringArgument(PEGTransformer &transformer,
	                                                                   const string &string_literal);
	static unique_ptr<TransformResultValue> TransformExtractDatePartInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformInsertStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformInsertStatement(PEGTransformer &transformer, optional<CommonTableExpressionMap> with_clause,
	                         const optional<OnConflictAction> &or_action, unique_ptr<BaseTableRef> insert_target,
	                         const optional<InsertColumnOrder> &by_name_or_position,
	                         const optional<vector<string>> &insert_column_list, InsertValues insert_values,
	                         optional<unique_ptr<OnConflictInfo>> on_conflict_clause,
	                         optional<vector<unique_ptr<ParsedExpression>>> returning_clause);
	static unique_ptr<TransformResultValue> TransformOrActionInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformInsertOrReplaceInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static OnConflictAction TransformInsertOrReplace(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInsertOrIgnoreInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static OnConflictAction TransformInsertOrIgnore(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformByNameOrPositionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformInsertByNameOrderInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformInsertByPositionOrderInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformInsertByNameInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static InsertColumnOrder TransformInsertByName(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInsertByPositionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static InsertColumnOrder TransformInsertByPosition(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInsertTargetInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<BaseTableRef> TransformInsertTarget(PEGTransformer &transformer,
	                                                      unique_ptr<BaseTableRef> base_table_name,
	                                                      const optional<Identifier> &insert_alias);
	static unique_ptr<TransformResultValue> TransformInsertAliasInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static Identifier TransformInsertAlias(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformColumnListInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static vector<string> TransformColumnList(PEGTransformer &transformer, const vector<Identifier> &col_id);
	static unique_ptr<TransformResultValue> TransformInsertColumnListInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static vector<string> TransformInsertColumnList(PEGTransformer &transformer, const vector<string> &column_list);
	static unique_ptr<TransformResultValue> TransformInsertValuesInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSelectInsertValuesInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static InsertValues TransformSelectInsertValues(PEGTransformer &transformer,
	                                                unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformDefaultValuesInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static InsertValues TransformDefaultValues(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOnConflictClauseInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<OnConflictInfo> TransformOnConflictClause(PEGTransformer &transformer,
	                                                            optional<OnConflictExpressionTarget> on_conflict_target,
	                                                            unique_ptr<OnConflictInfo> on_conflict_action);
	static unique_ptr<TransformResultValue> TransformOnConflictTargetInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOnConflictExpressionTargetInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static OnConflictExpressionTarget
	TransformOnConflictExpressionTarget(PEGTransformer &transformer, const vector<string> &column_id_list,
	                                    optional<unique_ptr<ParsedExpression>> where_clause);
	static unique_ptr<TransformResultValue> TransformOnConflictIndexTargetInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static OnConflictExpressionTarget TransformOnConflictIndexTarget(PEGTransformer &transformer,
	                                                                 const Identifier &constraint_name);
	static unique_ptr<TransformResultValue> TransformOnConflictActionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOnConflictUpdateInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<OnConflictInfo> TransformOnConflictUpdate(PEGTransformer &transformer,
	                                                            unique_ptr<UpdateSetInfo> update_set_clause,
	                                                            optional<unique_ptr<ParsedExpression>> where_clause);
	static unique_ptr<TransformResultValue> TransformOnConflictNothingInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<OnConflictInfo> TransformOnConflictNothing(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformReturningClauseInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformReturningClause(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> target_list);
	static unique_ptr<TransformResultValue> TransformLoadStatementInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformLoadStatement(PEGTransformer &transformer,
	                                                       const Identifier &col_id_or_string,
	                                                       const optional<Identifier> &extension_alias);
	static unique_ptr<TransformResultValue> TransformExtensionAliasInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static Identifier TransformExtensionAlias(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformInstallStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformInstallStatement(PEGTransformer &transformer, const bool &has_result,
	                                                          const QualifiedName &identifier_or_string_literal,
	                                                          const optional<ExtensionRepositoryInfo> &from_source,
	                                                          const optional<string> &version_number);
	static unique_ptr<TransformResultValue> TransformUpdateExtensionsStatementInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformUpdateExtensionsStatement(PEGTransformer &transformer,
	                                                                   const optional<vector<Identifier>> &identifier);
	static unique_ptr<TransformResultValue> TransformFromSourceInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformFromSourceIdentifierInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static ExtensionRepositoryInfo TransformFromSourceIdentifier(PEGTransformer &transformer,
	                                                             const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformFromSourceStringInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static ExtensionRepositoryInfo TransformFromSourceString(PEGTransformer &transformer, const string &string_literal);
	static unique_ptr<TransformResultValue> TransformVersionNumberInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static string TransformVersionNumber(PEGTransformer &transformer,
	                                     const QualifiedName &identifier_or_string_literal);
	static unique_ptr<TransformResultValue> TransformMergeIntoStatementInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformMergeIntoStatement(PEGTransformer &transformer, optional<CommonTableExpressionMap> with_clause,
	                            unique_ptr<BaseTableRef> target_opt_alias, unique_ptr<TableRef> merge_into_using_clause,
	                            JoinQualifier join_qualifier,
	                            vector<pair<MergeActionCondition, unique_ptr<MergeIntoAction>>> merge_match,
	                            optional<vector<unique_ptr<ParsedExpression>>> returning_clause);
	static unique_ptr<TransformResultValue> TransformMergeIntoUsingClauseInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TableRef> TransformMergeIntoUsingClause(PEGTransformer &transformer,
	                                                          unique_ptr<TableRef> table_ref);
	static unique_ptr<TransformResultValue> TransformMergeMatchInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformMatchedClauseInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static pair<MergeActionCondition, unique_ptr<MergeIntoAction>>
	TransformMatchedClause(PEGTransformer &transformer, optional<unique_ptr<ParsedExpression>> and_expression,
	                       unique_ptr<MergeIntoAction> matched_clause_action);
	static unique_ptr<TransformResultValue> TransformMatchedClauseActionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformUpdateMatchClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<MergeIntoAction>
	TransformUpdateMatchClause(PEGTransformer &transformer, optional<unique_ptr<MergeIntoAction>> update_match_info);
	static unique_ptr<TransformResultValue> TransformUpdateMatchInfoInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformUpdateMatchSetActionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<MergeIntoAction> TransformUpdateMatchSetAction(PEGTransformer &transformer,
	                                                                 unique_ptr<UpdateSetInfo> update_match_set_clause);
	static unique_ptr<TransformResultValue> TransformUpdateByNameOrPositionInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<MergeIntoAction> TransformUpdateByNameOrPosition(PEGTransformer &transformer,
	                                                                   const InsertColumnOrder &by_name_or_position);
	static unique_ptr<TransformResultValue> TransformDeleteMatchClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<MergeIntoAction> TransformDeleteMatchClause(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInsertMatchClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<MergeIntoAction>
	TransformInsertMatchClause(PEGTransformer &transformer, optional<unique_ptr<MergeIntoAction>> insert_match_info);
	static unique_ptr<TransformResultValue> TransformInsertMatchInfoInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformInsertDefaultValuesInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<MergeIntoAction> TransformInsertDefaultValues(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInsertByNameOrPositionInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<MergeIntoAction>
	TransformInsertByNameOrPosition(PEGTransformer &transformer, const optional<InsertColumnOrder> &by_name_or_position,
	                                const bool &has_result);
	static unique_ptr<TransformResultValue> TransformInsertValuesListInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<MergeIntoAction> TransformInsertValuesList(PEGTransformer &transformer,
	                                                             const optional<vector<string>> &insert_column_list,
	                                                             vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformDoNothingMatchClauseInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<MergeIntoAction> TransformDoNothingMatchClause(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformErrorMatchClauseInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<MergeIntoAction> TransformErrorMatchClause(PEGTransformer &transformer,
	                                                             optional<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformUpdateMatchSetClauseInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformUpdateMatchSetInfoInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformAndExpressionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformAndExpression(PEGTransformer &transformer,
	                                                           unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformNotMatchedClauseInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static pair<MergeActionCondition, unique_ptr<MergeIntoAction>>
	TransformNotMatchedClause(PEGTransformer &transformer, const optional<MergeActionCondition> &by_source_or_target,
	                          optional<unique_ptr<ParsedExpression>> and_expression,
	                          unique_ptr<MergeIntoAction> matched_clause_action);
	static unique_ptr<TransformResultValue> TransformBySourceOrTargetInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformBySourceInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static MergeActionCondition TransformBySource(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformByTargetInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static MergeActionCondition TransformByTarget(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformPivotOnInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static vector<PivotColumn> TransformPivotOn(PEGTransformer &transformer, vector<PivotColumn> pivot_column_list);
	static unique_ptr<TransformResultValue> TransformPivotUsingInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformPivotUsing(PEGTransformer &transformer,
	                                                                vector<unique_ptr<ParsedExpression>> target_list);
	static unique_ptr<TransformResultValue> TransformPivotColumnListInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<PivotColumn> TransformPivotColumnList(PEGTransformer &transformer,
	                                                    vector<PivotColumn> pivot_column_entry);
	static unique_ptr<TransformResultValue> TransformPivotColumnEntryInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPivotColumnExpressionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static PivotColumn TransformPivotColumnExpression(PEGTransformer &transformer,
	                                                  unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformPivotColumnSubqueryInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static PivotColumn TransformPivotColumnSubquery(PEGTransformer &transformer,
	                                                unique_ptr<ParsedExpression> base_expression,
	                                                unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformIntoNameValuesInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static UnpivotNameValues TransformIntoNameValues(PEGTransformer &transformer, const Identifier &col_id_or_string,
	                                                 const vector<Identifier> &identifier);
	static unique_ptr<TransformResultValue> TransformIncludeOrExcludeNullsInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformIncludeNullsInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformIncludeNulls(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformExcludeNullsInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformExcludeNulls(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformUnpivotHeaderInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformUnpivotHeaderSingleInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static vector<string> TransformUnpivotHeaderSingle(PEGTransformer &transformer, const Identifier &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformUnpivotHeaderListInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<string> TransformUnpivotHeaderList(PEGTransformer &transformer,
	                                                 const vector<Identifier> &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformPragmaStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformPragmaStatement(PEGTransformer &transformer,
	                                                         unique_ptr<SQLStatement> pragma_assign_or_function);
	static unique_ptr<TransformResultValue> TransformPragmaAssignOrFunctionInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformPragmaAssignInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformPragmaAssign(PEGTransformer &transformer, const Identifier &setting_name,
	                                                      vector<unique_ptr<ParsedExpression>> variable_list);
	static unique_ptr<TransformResultValue> TransformPragmaFunctionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformPragmaFunction(PEGTransformer &transformer, const Identifier &pragma_name,
	                        optional<vector<unique_ptr<ParsedExpression>>> pragma_parameters);
	static unique_ptr<TransformResultValue> TransformPragmaParametersInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformPragmaParameters(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformPrepareStatementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformPrepareStatement(PEGTransformer &transformer, const Identifier &identifier,
	                                                          const optional<vector<LogicalType>> &type_list,
	                                                          unique_ptr<SQLStatement> statement);
	static unique_ptr<TransformResultValue> TransformTypeListInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static vector<LogicalType> TransformTypeList(PEGTransformer &transformer, const vector<LogicalType> &type);
	static unique_ptr<TransformResultValue> TransformSelectStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformSelectStatement(PEGTransformer &transformer,
	                                                         unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformSelectSetOpChainInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformSelectSetOpChain(
	    PEGTransformer &transformer, unique_ptr<SelectStatement> intersect_chain,
	    optional<vector<pair<unique_ptr<SetOperationNode>, unique_ptr<SelectStatement>>>> select_set_op_chain_tail);
	static unique_ptr<TransformResultValue> TransformSelectSetOpChainTailInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static pair<unique_ptr<SetOperationNode>, unique_ptr<SelectStatement>>
	TransformSelectSetOpChainTail(PEGTransformer &transformer, unique_ptr<SetOperationNode> setop_clause,
	                              unique_ptr<SelectStatement> intersect_chain);
	static unique_ptr<TransformResultValue> TransformIntersectChainInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformIntersectChain(
	    PEGTransformer &transformer, unique_ptr<SelectStatement> select_atom,
	    optional<vector<pair<unique_ptr<SetOperationNode>, unique_ptr<SelectStatement>>>> intersect_chain_tail);
	static unique_ptr<TransformResultValue> TransformIntersectChainTailInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static pair<unique_ptr<SetOperationNode>, unique_ptr<SelectStatement>>
	TransformIntersectChainTail(PEGTransformer &transformer, unique_ptr<SetOperationNode> set_intersect_clause,
	                            unique_ptr<SelectStatement> select_atom);
	static unique_ptr<TransformResultValue> TransformSetIntersectClauseInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<SetOperationNode> TransformSetIntersectClause(PEGTransformer &transformer,
	                                                                const optional<bool> &distinct_or_all);
	static unique_ptr<TransformResultValue> TransformSelectAtomInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSelectParensInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformSelectParens(PEGTransformer &transformer,
	                                                         unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformSetopClauseInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<SetOperationNode> TransformSetopClause(PEGTransformer &transformer,
	                                                         const SetOperationType &setop_type,
	                                                         const optional<bool> &distinct_or_all,
	                                                         const bool &has_result);
	static unique_ptr<TransformResultValue> TransformSetopTypeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSetopUnionInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static SetOperationType TransformSetopUnion(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSetopExceptInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static SetOperationType TransformSetopExcept(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSelectStatementTypeInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformResultModifiersInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<unique_ptr<ResultModifier>>
	TransformResultModifiers(PEGTransformer &transformer, optional<vector<OrderByNode>> order_by_clause,
	                         optional<unique_ptr<ResultModifier>> limit_offset);
	static unique_ptr<TransformResultValue> TransformLimitOffsetInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLimitOffsetClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ResultModifier> TransformLimitOffsetClause(PEGTransformer &transformer,
	                                                             LimitPercentResult limit_clause,
	                                                             optional<LimitPercentResult> offset_clause);
	static unique_ptr<TransformResultValue> TransformOffsetLimitClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ResultModifier> TransformOffsetLimitClause(PEGTransformer &transformer,
	                                                             LimitPercentResult offset_clause,
	                                                             optional<LimitPercentResult> limit_clause);
	static unique_ptr<TransformResultValue> TransformTableStatementInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformTableStatement(PEGTransformer &transformer,
	                                                           unique_ptr<BaseTableRef> base_table_name);
	static unique_ptr<TransformResultValue> TransformOptionalParensSimpleSelectInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSimpleSelectParensInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<SelectStatement> TransformSimpleSelectParens(PEGTransformer &transformer,
	                                                               unique_ptr<SelectStatement> simple_select);
	static unique_ptr<TransformResultValue> TransformSelectFromInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSelectFromClauseInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SelectNode> TransformSelectFromClause(PEGTransformer &transformer,
	                                                        unique_ptr<SelectNode> select_clause,
	                                                        optional<unique_ptr<TableRef>> from_clause);
	static unique_ptr<TransformResultValue> TransformFromSelectClauseInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SelectNode> TransformFromSelectClause(PEGTransformer &transformer,
	                                                        unique_ptr<TableRef> from_clause,
	                                                        optional<unique_ptr<SelectNode>> select_clause);
	static unique_ptr<TransformResultValue> TransformWithStatementInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static pair<Identifier, unique_ptr<CommonTableExpressionInfo>>
	TransformWithStatement(PEGTransformer &transformer, const Identifier &col_id_or_string,
	                       const optional<vector<string>> &insert_column_list,
	                       optional<vector<unique_ptr<ParsedExpression>>> using_key, const optional<bool> &materialized,
	                       unique_ptr<TableRef> cte_body);
	static unique_ptr<TransformResultValue> TransformCTEBodyInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCTESelectBodyInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TableRef> TransformCTESelectBody(PEGTransformer &transformer,
	                                                   unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformCTEDMLBodyInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TableRef> TransformCTEDMLBody(PEGTransformer &transformer, unique_ptr<SQLStatement> statement);
	static unique_ptr<TransformResultValue> TransformUsingKeyInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformUsingKey(PEGTransformer &transformer,
	                                                              vector<unique_ptr<ParsedExpression>> target_list);
	static unique_ptr<TransformResultValue> TransformMaterializedInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static bool TransformMaterialized(PEGTransformer &transformer, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformSelectClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<SelectNode> TransformSelectClause(PEGTransformer &transformer,
	                                                    optional<DistinctClause> distinct_clause,
	                                                    optional<vector<unique_ptr<ParsedExpression>>> target_list);
	static unique_ptr<TransformResultValue> TransformTargetListInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformTargetList(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> aliased_expression);
	static unique_ptr<TransformResultValue> TransformColumnAliasesInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static vector<string> TransformColumnAliases(PEGTransformer &transformer,
	                                             const vector<Identifier> &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformDistinctClauseInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDistinctAllInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static DistinctClause TransformDistinctAll(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformDistinctOnInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static DistinctClause TransformDistinctOn(PEGTransformer &transformer,
	                                          optional<vector<unique_ptr<ParsedExpression>>> distinct_on_targets);
	static unique_ptr<TransformResultValue> TransformDistinctOnTargetsInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformDistinctOnTargets(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformInnerTableRefInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTableSubqueryInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TableRef> TransformTableSubquery(PEGTransformer &transformer, const optional<bool> &lateral,
	                                                   unique_ptr<TableRef> subquery_reference,
	                                                   const optional<TableAlias> &table_alias);
	static unique_ptr<TransformResultValue> TransformBaseTableRefInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TableRef>
	TransformBaseTableRef(PEGTransformer &transformer, const optional<Identifier> &table_alias_colon,
	                      unique_ptr<BaseTableRef> base_table_name, const optional<TableAlias> &table_alias,
	                      optional<unique_ptr<AtClause>> at_clause, optional<unique_ptr<SampleOptions>> sample_clause);
	static unique_ptr<TransformResultValue> TransformTableAliasColonInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static Identifier TransformTableAliasColon(PEGTransformer &transformer, const Identifier &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformValuesRefInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TableRef> TransformValuesRef(PEGTransformer &transformer,
	                                               unique_ptr<SelectStatement> values_clause,
	                                               const optional<TableAlias> &table_alias);
	static unique_ptr<TransformResultValue> TransformParensTableRefInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<TableRef> TransformParensTableRef(PEGTransformer &transformer,
	                                                    const optional<Identifier> &table_alias_colon,
	                                                    unique_ptr<TableRef> table_ref,
	                                                    const optional<TableAlias> &table_alias,
	                                                    optional<unique_ptr<SampleOptions>> sample_clause);
	static unique_ptr<TransformResultValue> TransformJoinOrPivotInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTablePivotClauseInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TableRef> TransformTablePivotClause(PEGTransformer &transformer,
	                                                      unique_ptr<TableRef> table_pivot_clause_body,
	                                                      const optional<TableAlias> &table_alias);
	static unique_ptr<TransformResultValue> TransformTablePivotClauseBodyInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TableRef> TransformTablePivotClauseBody(PEGTransformer &transformer,
	                                                          vector<unique_ptr<ParsedExpression>> target_list,
	                                                          vector<PivotColumn> pivot_value_list,
	                                                          const optional<vector<string>> &pivot_group_by_list);
	static unique_ptr<TransformResultValue> TransformPivotGroupByListInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static vector<string> TransformPivotGroupByList(PEGTransformer &transformer,
	                                                const vector<Identifier> &col_id_or_string);
	static unique_ptr<TransformResultValue> TransformTableUnpivotClauseInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TableRef> TransformTableUnpivotClause(PEGTransformer &transformer,
	                                                        const optional<bool> &include_or_exclude_nulls,
	                                                        unique_ptr<TableRef> table_unpivot_clause_body,
	                                                        const optional<TableAlias> &table_alias);
	static unique_ptr<TransformResultValue> TransformTableUnpivotClauseBodyInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<TableRef> TransformTableUnpivotClauseBody(PEGTransformer &transformer,
	                                                            const vector<string> &unpivot_header,
	                                                            vector<PivotColumn> unpivot_value_list);
	static unique_ptr<TransformResultValue> TransformPivotHeaderInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformPivotHeader(PEGTransformer &transformer,
	                                                         unique_ptr<ParsedExpression> base_expression);
	static unique_ptr<TransformResultValue> TransformPivotValueListInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static PivotColumn TransformPivotValueList(PEGTransformer &transformer, unique_ptr<ParsedExpression> pivot_header,
	                                           PivotColumn pivot_value_target);
	static unique_ptr<TransformResultValue> TransformPivotValueTargetInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static PivotColumn TransformPivotValueTarget(PEGTransformer &transformer, ParseResult &choice_result);
	static unique_ptr<TransformResultValue> TransformUnpivotValueListInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static PivotColumn TransformUnpivotValueList(PEGTransformer &transformer, const vector<string> &unpivot_header,
	                                             vector<PivotColumnEntry> unpivot_target_list);
	static unique_ptr<TransformResultValue> TransformPivotTargetListInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static vector<PivotColumnEntry> TransformPivotTargetList(PEGTransformer &transformer,
	                                                         vector<unique_ptr<ParsedExpression>> target_list);
	static unique_ptr<TransformResultValue> TransformUnpivotTargetListInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<PivotColumnEntry> TransformUnpivotTargetList(PEGTransformer &transformer,
	                                                           vector<unique_ptr<ParsedExpression>> target_list);
	static unique_ptr<TransformResultValue> TransformLateralInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static bool TransformLateral(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformBaseTableNameInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformUnqualifiedBaseTableNameInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<BaseTableRef> TransformUnqualifiedBaseTableName(PEGTransformer &transformer,
	                                                                  const Identifier &table_name);
	static unique_ptr<TransformResultValue> TransformSchemaReservedTableInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<BaseTableRef> TransformSchemaReservedTable(PEGTransformer &transformer,
	                                                             const Identifier &schema_qualification,
	                                                             const Identifier &reserved_table_name);
	static unique_ptr<TransformResultValue> TransformCatalogReservedSchemaTableInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static unique_ptr<BaseTableRef> TransformCatalogReservedSchemaTable(PEGTransformer &transformer,
	                                                                    const Identifier &catalog_qualification,
	                                                                    const Identifier &reserved_schema_qualification,
	                                                                    const Identifier &reserved_table_name);
	static unique_ptr<TransformResultValue> TransformTableFunctionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTableFunctionLateralOptInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<TableRef> TransformTableFunctionLateralOpt(PEGTransformer &transformer,
	                                                             const optional<bool> &lateral,
	                                                             const QualifiedName &qualified_table_function,
	                                                             vector<FunctionArgument> table_function_arguments,
	                                                             const optional<bool> &with_ordinality,
	                                                             const optional<TableAlias> &table_alias);
	static unique_ptr<TransformResultValue> TransformTableFunctionAliasColonInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<TableRef> TransformTableFunctionAliasColon(PEGTransformer &transformer,
	                                                             const Identifier &table_alias_colon,
	                                                             const QualifiedName &qualified_table_function,
	                                                             vector<FunctionArgument> table_function_arguments,
	                                                             const optional<bool> &with_ordinality,
	                                                             optional<unique_ptr<SampleOptions>> sample_clause);
	static unique_ptr<TransformResultValue> TransformWithOrdinalityInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static bool TransformWithOrdinality(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformQualifiedTableFunctionInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static QualifiedName TransformQualifiedTableFunction(PEGTransformer &transformer,
	                                                     const optional<Identifier> &catalog_qualification,
	                                                     const optional<Identifier> &schema_qualification,
	                                                     const Identifier &table_function_name);
	static unique_ptr<TransformResultValue> TransformTableFunctionArgumentsInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static vector<FunctionArgument>
	TransformTableFunctionArguments(PEGTransformer &transformer, optional<vector<FunctionArgument>> function_argument);
	static unique_ptr<TransformResultValue> TransformFunctionArgumentInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNamedFunctionArgumentInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static FunctionArgument TransformNamedFunctionArgument(PEGTransformer &transformer, MacroParameter named_parameter);
	static unique_ptr<TransformResultValue> TransformPositionalFunctionArgumentInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static FunctionArgument TransformPositionalFunctionArgument(PEGTransformer &transformer,
	                                                            unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformNamedParameterInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static MacroParameter TransformNamedParameter(PEGTransformer &transformer, const Identifier &type_func_name,
	                                              const optional<LogicalType> &type,
	                                              unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformTableAliasInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTableAliasAsInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static TableAlias TransformTableAliasAs(PEGTransformer &transformer,
	                                        const QualifiedName &identifier_or_string_literal,
	                                        const optional<vector<string>> &column_aliases);
	static unique_ptr<TransformResultValue> TransformTableAliasWithoutAsInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static TableAlias TransformTableAliasWithoutAs(PEGTransformer &transformer, const Identifier &identifier,
	                                               const optional<vector<string>> &column_aliases);
	static unique_ptr<TransformResultValue> TransformAtClauseInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<AtClause> TransformAtClause(PEGTransformer &transformer, unique_ptr<AtClause> at_specifier);
	static unique_ptr<TransformResultValue> TransformAtSpecifierInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<AtClause> TransformAtSpecifier(PEGTransformer &transformer, const string &at_unit,
	                                                 unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformAtUnitInternal(PEGTransformer &transformer,
	                                                                ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformVersionAtUnitInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static string TransformVersionAtUnit(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformTimestampAtUnitInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static string TransformTimestampAtUnit(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformJoinClauseInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformRegularJoinClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TableRef> TransformRegularJoinClause(PEGTransformer &transformer, const optional<bool> &asof,
	                                                       const optional<JoinType> &join_type,
	                                                       unique_ptr<TableRef> table_ref,
	                                                       JoinQualifier join_qualifier);
	static unique_ptr<TransformResultValue> TransformJoinByClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TableRef> TransformJoinByClause(PEGTransformer &transformer, const string &col_label,
	                                                  unique_ptr<TableRef> table_ref, JoinQualifier join_qualifier);
	static unique_ptr<TransformResultValue> TransformAsofInternal(PEGTransformer &transformer,
	                                                              ParseResult &parse_result);
	static bool TransformAsof(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformJoinWithoutOnClauseInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TableRef> TransformJoinWithoutOnClause(PEGTransformer &transformer, const JoinPrefix &join_prefix,
	                                                         unique_ptr<TableRef> table_ref);
	static unique_ptr<TransformResultValue> TransformJoinQualifierInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOnClauseInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static JoinQualifier TransformOnClause(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformUsingClauseInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static JoinQualifier TransformUsingClause(PEGTransformer &transformer, const vector<Identifier> &column_name);
	static unique_ptr<TransformResultValue> TransformJoinTypeInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformJoinPrefixInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCrossJoinPrefixInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static JoinPrefix TransformCrossJoinPrefix(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNaturalJoinPrefixInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static JoinPrefix TransformNaturalJoinPrefix(PEGTransformer &transformer, const optional<JoinType> &join_type);
	static unique_ptr<TransformResultValue> TransformPositionalJoinPrefixInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static JoinPrefix TransformPositionalJoinPrefix(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFullJoinInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static JoinType TransformFullJoin(PEGTransformer &transformer, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformLeftJoinInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static JoinType TransformLeftJoin(PEGTransformer &transformer, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformRightJoinInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static JoinType TransformRightJoin(PEGTransformer &transformer, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformSemiJoinInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static JoinType TransformSemiJoin(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformAntiJoinInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static JoinType TransformAntiJoin(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformInnerJoinInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static JoinType TransformInnerJoin(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformFromClauseInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TableRef> TransformFromClause(PEGTransformer &transformer,
	                                                vector<unique_ptr<TableRef>> table_ref);
	static unique_ptr<TransformResultValue> TransformWhereClauseInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformWhereClause(PEGTransformer &transformer,
	                                                         unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformGroupByClauseInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static GroupByNode TransformGroupByClause(PEGTransformer &transformer, GroupByNode group_by_expressions);
	static unique_ptr<TransformResultValue> TransformHavingClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformHavingClause(PEGTransformer &transformer,
	                                                          unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformQualifyClauseInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformQualifyClause(PEGTransformer &transformer,
	                                                           unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformSampleClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<SampleOptions> TransformSampleClause(PEGTransformer &transformer,
	                                                       unique_ptr<SampleOptions> sample_entry);
	static unique_ptr<TransformResultValue> TransformWindowClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformWindowClause(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> window_definition);
	static unique_ptr<TransformResultValue> TransformSampleEntryInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSampleEntryCountInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SampleOptions>
	TransformSampleEntryCount(PEGTransformer &transformer, unique_ptr<SampleOptions> sample_count,
	                          const optional<pair<SampleMethod, optional_idx>> &sample_properties);
	static unique_ptr<TransformResultValue> TransformSampleEntryFunctionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<SampleOptions> TransformSampleEntryFunction(PEGTransformer &transformer,
	                                                              const optional<SampleMethod> &sample_function,
	                                                              unique_ptr<SampleOptions> sample_count,
	                                                              const optional<optional_idx> &repeatable_sample);
	static unique_ptr<TransformResultValue> TransformSampleFunctionInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static SampleMethod TransformSampleFunction(PEGTransformer &transformer, const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformSamplePropertiesInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static pair<SampleMethod, optional_idx> TransformSampleProperties(PEGTransformer &transformer,
	                                                                  const Identifier &col_id,
	                                                                  const optional<optional_idx> &sample_seed);
	static unique_ptr<TransformResultValue> TransformRepeatableSampleInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static optional_idx TransformRepeatableSample(PEGTransformer &transformer, const optional_idx &sample_seed);
	static unique_ptr<TransformResultValue> TransformSampleSeedInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static optional_idx TransformSampleSeed(PEGTransformer &transformer, unique_ptr<ParsedExpression> number_literal);
	static unique_ptr<TransformResultValue> TransformSampleCountInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<SampleOptions> TransformSampleCount(PEGTransformer &transformer,
	                                                      unique_ptr<ParsedExpression> sample_value,
	                                                      const optional<bool> &sample_unit);
	static unique_ptr<TransformResultValue> TransformSampleValueInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSampleUnitInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSamplePercentageInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static bool TransformSamplePercentage(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSampleRowsInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static bool TransformSampleRows(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGroupByExpressionsInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformGroupByAllInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static GroupByNode TransformGroupByAll(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGroupByListInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static GroupByNode TransformGroupByList(PEGTransformer &transformer,
	                                        vector<GroupByExpressionInfo> group_by_expression);
	static unique_ptr<TransformResultValue> TransformGroupByExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformGroupByBaseExpressionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static GroupByExpressionInfo TransformGroupByBaseExpression(PEGTransformer &transformer,
	                                                            unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformEmptyGroupingItemInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static GroupByExpressionInfo TransformEmptyGroupingItem(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformCubeOrRollupClauseInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static GroupByExpressionInfo TransformCubeOrRollupClause(PEGTransformer &transformer, const string &cube_or_rollup,
	                                                         optional<vector<unique_ptr<ParsedExpression>>> expression);
	static unique_ptr<TransformResultValue> TransformCubeOrRollupInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformCubeKeywordInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static string TransformCubeKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformRollupKeywordInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static string TransformRollupKeyword(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGroupingSetsClauseInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static GroupByExpressionInfo TransformGroupingSetsClause(PEGTransformer &transformer,
	                                                         vector<GroupByExpressionInfo> group_by_expression);
	static unique_ptr<TransformResultValue> TransformSubqueryReferenceInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TableRef> TransformSubqueryReference(PEGTransformer &transformer,
	                                                       unique_ptr<SelectStatement> select_statement_internal);
	static unique_ptr<TransformResultValue> TransformOrderByExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static OrderByNode TransformOrderByExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                                              const optional<OrderType> &desc_or_asc,
	                                              const optional<OrderByNullType> &nulls_first_or_last);
	static unique_ptr<TransformResultValue> TransformDescOrAscInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformDescendingOrderInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static OrderType TransformDescendingOrder(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformAscendingOrderInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static OrderType TransformAscendingOrder(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNullsFirstOrLastInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformNullsFirstInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static OrderByNullType TransformNullsFirst(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNullsLastInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static OrderByNullType TransformNullsLast(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOrderByClauseInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static vector<OrderByNode> TransformOrderByClause(PEGTransformer &transformer,
	                                                  vector<OrderByNode> order_by_expressions);
	static unique_ptr<TransformResultValue> TransformOrderByExpressionsInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOrderByExpressionListInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static vector<OrderByNode> TransformOrderByExpressionList(PEGTransformer &transformer,
	                                                          vector<OrderByNode> order_by_expression);
	static unique_ptr<TransformResultValue> TransformOrderByAllInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static vector<OrderByNode> TransformOrderByAll(PEGTransformer &transformer, const optional<OrderType> &desc_or_asc,
	                                               const optional<OrderByNullType> &nulls_first_or_last);
	static unique_ptr<TransformResultValue> TransformLimitClauseInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static LimitPercentResult TransformLimitClause(PEGTransformer &transformer, LimitPercentResult limit_value);
	static unique_ptr<TransformResultValue> TransformOffsetClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static LimitPercentResult TransformOffsetClause(PEGTransformer &transformer, LimitPercentResult offset_value);
	static unique_ptr<TransformResultValue> TransformOffsetValueInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static LimitPercentResult TransformOffsetValue(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
	                                               const bool &has_result);
	static unique_ptr<TransformResultValue> TransformLimitValueInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLimitAllInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static LimitPercentResult TransformLimitAll(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformLimitLiteralPercentInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static LimitPercentResult TransformLimitLiteralPercent(PEGTransformer &transformer,
	                                                       unique_ptr<ParsedExpression> number_literal);
	static unique_ptr<TransformResultValue> TransformLimitExpressionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static LimitPercentResult TransformLimitExpression(PEGTransformer &transformer,
	                                                   unique_ptr<ParsedExpression> expression, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformAliasedExpressionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColIdExpressionInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformColIdExpression(PEGTransformer &transformer, const Identifier &col_id,
	                                                             unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformExpressionAsCollabelInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformExpressionAsCollabel(PEGTransformer &transformer,
	                                                                  unique_ptr<ParsedExpression> expression,
	                                                                  const Identifier &col_label_or_string);
	static unique_ptr<TransformResultValue> TransformExpressionOptIdentifierInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformExpressionOptIdentifier(PEGTransformer &transformer,
	                                                                     unique_ptr<ParsedExpression> expression,
	                                                                     const optional<Identifier> &identifier);
	static unique_ptr<TransformResultValue> TransformValuesClauseInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<SelectStatement>
	TransformValuesClause(PEGTransformer &transformer, vector<vector<unique_ptr<ParsedExpression>>> values_expressions);
	static unique_ptr<TransformResultValue> TransformValuesExpressionsInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformValuesExpressions(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformSetStatementInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformSetStatement(PEGTransformer &transformer,
	                                                      unique_ptr<SetStatement> set_assignment_or_time_zone);
	static unique_ptr<TransformResultValue> TransformSetAssignmentOrTimeZoneInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformResetStatementInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformResetStatement(PEGTransformer &transformer,
	                                                        const SettingInfo &set_variable_or_setting);
	static unique_ptr<TransformResultValue> TransformStandardAssignmentInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<SetStatement> TransformStandardAssignment(PEGTransformer &transformer,
	                                                            const SettingInfo &set_variable_or_setting,
	                                                            vector<unique_ptr<ParsedExpression>> set_assignment);
	static unique_ptr<TransformResultValue> TransformSetVariableOrSettingInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSetTimeZoneInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<SetStatement> TransformSetTimeZone(PEGTransformer &transformer,
	                                                     unique_ptr<ParsedExpression> zone_value);
	static unique_ptr<TransformResultValue> TransformZoneValueInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformZoneLocalInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformZoneLocal(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformZoneDefaultInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformZoneDefault(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformZoneStringLiteralInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformZoneStringLiteral(PEGTransformer &transformer,
	                                                               const string &string_literal);
	static unique_ptr<TransformResultValue> TransformZoneIdentifierInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformZoneIdentifier(PEGTransformer &transformer,
	                                                            const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformZoneIntervalWithIntervalInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformZoneIntervalWithInterval(PEGTransformer &transformer,
	                                                                      const string &string_literal,
	                                                                      const optional<DatePartSpecifier> &interval);
	static unique_ptr<TransformResultValue> TransformZoneIntervalWithPrecisionInternal(PEGTransformer &transformer,
	                                                                                   ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformZoneIntervalWithPrecision(PEGTransformer &transformer,
	                                                                       unique_ptr<ParsedExpression> number_literal,
	                                                                       const string &string_literal);
	static unique_ptr<TransformResultValue> TransformSetSettingInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static SettingInfo TransformSetSetting(PEGTransformer &transformer, const optional<SetScope> &setting_scope,
	                                       const Identifier &setting_name);
	static unique_ptr<TransformResultValue> TransformSetVariableInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static SettingInfo TransformSetVariable(PEGTransformer &transformer, const SetScope &variable_scope,
	                                        const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformVariableScopeInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static SetScope TransformVariableScope(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSettingScopeInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformLocalScopeInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static SetScope TransformLocalScope(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSessionScopeInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static SetScope TransformSessionScope(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformGlobalScopeInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static SetScope TransformGlobalScope(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformSetAssignmentInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>>
	TransformSetAssignment(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> variable_list);
	static unique_ptr<TransformResultValue> TransformVariableListInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformVariableList(PEGTransformer &transformer,
	                                                                  vector<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformTransactionStatementInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformBeginTransactionInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformBeginTransaction(PEGTransformer &transformer, const bool &has_result,
	                                                          const optional<TransactionModifierType> &read_or_write);
	static unique_ptr<TransformResultValue> TransformRollbackTransactionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformRollbackTransaction(PEGTransformer &transformer, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformCommitTransactionInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformCommitTransaction(PEGTransformer &transformer, const bool &has_result);
	static unique_ptr<TransformResultValue> TransformReadOrWriteInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static TransactionModifierType TransformReadOrWrite(PEGTransformer &transformer,
	                                                    const TransactionModifierType &read_only_or_read_write);
	static unique_ptr<TransformResultValue> TransformReadOnlyOrReadWriteInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformReadOnlyInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static TransactionModifierType TransformReadOnly(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformReadWriteInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static TransactionModifierType TransformReadWrite(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformUpdateStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement>
	TransformUpdateStatement(PEGTransformer &transformer, optional<CommonTableExpressionMap> with_clause,
	                         unique_ptr<TableRef> update_target, unique_ptr<UpdateSetInfo> update_set_clause,
	                         optional<unique_ptr<TableRef>> from_clause,
	                         optional<unique_ptr<ParsedExpression>> where_clause,
	                         optional<vector<unique_ptr<ParsedExpression>>> returning_clause);
	static unique_ptr<TransformResultValue> TransformUpdateTargetInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformBaseTableSetInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TableRef> TransformBaseTableSet(PEGTransformer &transformer,
	                                                  unique_ptr<BaseTableRef> base_table_name);
	static unique_ptr<TransformResultValue> TransformBaseTableAliasSetInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<TableRef> TransformBaseTableAliasSet(PEGTransformer &transformer,
	                                                       unique_ptr<BaseTableRef> base_table_name,
	                                                       const optional<Identifier> &update_alias);
	static unique_ptr<TransformResultValue> TransformUpdateAliasInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static Identifier TransformUpdateAlias(PEGTransformer &transformer, const bool &has_result,
	                                       const Identifier &col_id);
	static unique_ptr<TransformResultValue> TransformUpdateSetClauseInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformUpdateSetTupleInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static unique_ptr<UpdateSetInfo> TransformUpdateSetTuple(PEGTransformer &transformer,
	                                                         const vector<Identifier> &column_name,
	                                                         unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformUpdateSetElementListInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<UpdateSetInfo>
	TransformUpdateSetElementList(PEGTransformer &transformer,
	                              vector<pair<string, unique_ptr<ParsedExpression>>> update_set_element);
	static unique_ptr<TransformResultValue> TransformUpdateSetElementInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static pair<string, unique_ptr<ParsedExpression>>
	TransformUpdateSetElement(PEGTransformer &transformer, const string &update_set_column_target,
	                          unique_ptr<ParsedExpression> expression);
	static unique_ptr<TransformResultValue> TransformUpdateSetColumnTargetInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static string TransformUpdateSetColumnTarget(PEGTransformer &transformer, const Identifier &column_name,
	                                             const optional<vector<Identifier>> &dot_identifier);
	static unique_ptr<TransformResultValue> TransformUseStatementInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformUseStatement(PEGTransformer &transformer, const QualifiedName &use_target);
	static unique_ptr<TransformResultValue> TransformUseTargetInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformSchemaNameAsUseTargetInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static QualifiedName TransformSchemaNameAsUseTarget(PEGTransformer &transformer, const Identifier &schema_name);
	static unique_ptr<TransformResultValue> TransformCatalogNameAsUseTargetInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static QualifiedName TransformCatalogNameAsUseTarget(PEGTransformer &transformer, const Identifier &catalog_name);
	static unique_ptr<TransformResultValue> TransformUseTargetCatalogSchemaInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static QualifiedName TransformUseTargetCatalogSchema(PEGTransformer &transformer, const Identifier &catalog_name,
	                                                     const Identifier &reserved_schema_name,
	                                                     const optional<vector<Identifier>> &dot_identifier);
	static unique_ptr<TransformResultValue> TransformDotIdentifierInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static Identifier TransformDotIdentifier(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformVacuumStatementInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<SQLStatement> TransformVacuumStatement(PEGTransformer &transformer,
	                                                         const optional<VacuumOptions> &vacuum_options,
	                                                         optional<AnalyzeTarget> analyze_target);
	static unique_ptr<TransformResultValue> TransformVacuumOptionsInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformVacuumParensOptionsInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static VacuumOptions TransformVacuumParensOptions(PEGTransformer &transformer, const vector<string> &vacuum_option);
	static unique_ptr<TransformResultValue> TransformVacuumLegacyOptionsInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static VacuumOptions TransformVacuumLegacyOptions(PEGTransformer &transformer, const optional<string> &opt_full,
	                                                  const optional<string> &opt_freeze,
	                                                  const optional<string> &opt_verbose,
	                                                  const optional<string> &opt_analyze);
	static unique_ptr<TransformResultValue> TransformVacuumOptionInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformOptAnalyzeInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static string TransformOptAnalyze(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOptFullInternal(PEGTransformer &transformer,
	                                                                 ParseResult &parse_result);
	static string TransformOptFull(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOptFreezeInternal(PEGTransformer &transformer,
	                                                                   ParseResult &parse_result);
	static string TransformOptFreeze(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformOptVerboseInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static string TransformOptVerbose(PEGTransformer &transformer);
	static unique_ptr<TransformResultValue> TransformNameListInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static vector<string> TransformNameList(PEGTransformer &transformer, const vector<Identifier> &col_id);
	//===--------------------------------------------------------------------===//
	// END GENERATED RULES
	//===--------------------------------------------------------------------===//

private:
	const case_insensitive_map_t<PEGTransformer::AnyTransformFunction> &GetTransformFunctions(ParserOptions &options);

	PEGParser parser;
	case_insensitive_map_t<PEGTransformer::AnyTransformFunction> sql_transform_functions;
	case_insensitive_map_t<PEGTransformer::AnyTransformFunction> trampoline_transform_functions;
};

} // namespace duckdb
