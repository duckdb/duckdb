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

template <typename T>
unique_ptr<TypedTransformResult<T>> TryBridgeTransformResultValue(TransformResultValue &base_result);

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
			auto bridged = TryBridgeTransformResultValue<T>(*child_results[slot]);
			if (bridged) {
				auto bridged_result = std::move(bridged->value);
				child_results[slot].reset();
				return bridged_result;
			}
			throw InternalException("Unexpected trampoline transformer result type for slot %llu in rule '%s'", slot,
			                        ops.name);
		}
		auto result = std::move(typed_result->value);
		child_results[slot].reset();
		return result;
	}

	template <class T>
	T &GetResult(idx_t slot) {
		if (slot >= child_results.size() || !child_results[slot]) {
			throw InternalException("Missing trampoline transformer result for slot %llu in rule '%s'", slot, ops.name);
		}
		auto *typed_result = dynamic_cast<TypedTransformResult<T> *>(child_results[slot].get());
		if (!typed_result) {
			throw InternalException("Unexpected trampoline transformer result type for slot %llu in rule '%s'", slot,
			                        ops.name);
		}
		return typed_result->value;
	}

	const transform_frame_index_t frame_index;
	ParseResult &parse_result;
	const TransformFrameOps &ops;
	const optional<TransformFrameResultTarget> result_target;
	TransformFrameState state = TransformFrameState::INITIALIZE;
	idx_t manual_state = 0;
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
	void SetResultLocation(ParseResult &parse_result, TransformResultValue &result);
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
		return TryBridgeTransformResultValue<T>(base_result);
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

template <typename T>
inline unique_ptr<TypedTransformResult<T>> TryBridgeTransformResultValue(TransformResultValue &base_result) {
	return nullptr;
}

//! Transparent bridging between string-typed and Identifier-typed transform results.
template <>
inline unique_ptr<TypedTransformResult<string>>
TryBridgeTransformResultValue<string>(TransformResultValue &base_result) {
	if (auto *ident = dynamic_cast<TypedTransformResult<Identifier> *>(&base_result)) {
		return make_uniq<TypedTransformResult<string>>(ident->value.GetIdentifierName());
	}
	return nullptr;
}

template <>
inline unique_ptr<TypedTransformResult<Identifier>>
TryBridgeTransformResultValue<Identifier>(TransformResultValue &base_result) {
	if (auto *str = dynamic_cast<TypedTransformResult<string> *>(&base_result)) {
		return make_uniq<TypedTransformResult<Identifier>>(Identifier(str->value));
	}
	return nullptr;
}

template <>
inline unique_ptr<TypedTransformResult<vector<string>>>
TryBridgeTransformResultValue<vector<string>>(TransformResultValue &base_result) {
	if (auto *idents = dynamic_cast<TypedTransformResult<vector<Identifier>> *>(&base_result)) {
		return make_uniq<TypedTransformResult<vector<string>>>(IdentifiersToStrings(idents->value));
	}
	return nullptr;
}

template <>
inline unique_ptr<TypedTransformResult<vector<Identifier>>>
TryBridgeTransformResultValue<vector<Identifier>>(TransformResultValue &base_result) {
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
	static void SplitGenericOptions(const vector<GenericCopyOption> &options_in,
	                                case_insensitive_map_t<unique_ptr<ParsedExpression>> &parsed_options,
	                                unordered_map<string, Value> &options, const char *statement_name);
	static void AddToMultiStatement(const unique_ptr<MultiStatement> &multi_statement,
	                                unique_ptr<AlterInfo> alter_info);
	static void AddUpdateToMultiStatement(const unique_ptr<MultiStatement> &multi_statement, const string &column_name,
	                                      const AlterEntryData &table_data,
	                                      const unique_ptr<ParsedExpression> &original_expression);
	static unique_ptr<MultiStatement> TransformAndMaterializeAlter(AlterEntryData &data,
	                                                               unique_ptr<AlterInfo> info_with_null_placeholder,
	                                                               const string &column_name,
	                                                               unique_ptr<ParsedExpression> expression);

	static void InitializePivotStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePivotStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUnpivotStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUnpivotStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLiteralExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLiteralExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePrefixExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePrefixExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOverClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOverClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSelectStatementInternalTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSelectStatementInternalTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeSimpleSelectTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSimpleSelectTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTableRefTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTableRefTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWithClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWithClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWindowDefinitionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWindowDefinitionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);

	//===--------------------------------------------------------------------===//
	// START GENERATED TRAMPOLINE RULES
	//===--------------------------------------------------------------------===//
	static void InitializeStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAlterStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAlterStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAlterOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAlterOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAlterTableStmtTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAlterTableStmtTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAlterSchemaStmtTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAlterSchemaStmtTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAlterTableOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAlterTableOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAddConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAddConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAddColumnTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAddColumnTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAddColumnEntryTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAddColumnEntryTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropColumnTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropColumnTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAlterColumnTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAlterColumnTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRenameColumnTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRenameColumnTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNestedColumnNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNestedColumnNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIdentifierDotTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIdentifierDotTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRenameAlterTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRenameAlterTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSetPartitionedByTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetPartitionedByTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeResetPartitionedByTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeResetPartitionedByTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeSetSortedByTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetSortedByTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeResetSortedByTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeResetSortedByTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSetOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeResetOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeResetOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAlterColumnEntryTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAlterColumnEntryTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAddOrDropDefaultTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAddOrDropDefaultTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAddDefaultTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAddDefaultTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropDefaultTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropDefaultTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeChangeNullabilityTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeChangeNullabilityTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropOrSetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropOrSetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropNullabilityTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropNullabilityTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSetNullabilityTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetNullabilityTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAlterTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAlterTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUsingExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUsingExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAlterViewStmtTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAlterViewStmtTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAlterSequenceStmtTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAlterSequenceStmtTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeQualifiedSequenceNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeQualifiedSequenceNameTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeAlterSequenceOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeAlterSequenceOptionsTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeRenameAlterSequenceOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeRenameAlterSequenceOptionsTrampoline(PEGTransformer &transformer,
	                                                                                     TransformStack &stack,
	                                                                                     TransformStackFrame &frame);
	static void InitializeSetSequenceOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetSequenceOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAlterDatabaseStmtTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAlterDatabaseStmtTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAnalyzeStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAnalyzeStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAnalyzeTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAnalyzeTargetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAnalyzeVerboseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAnalyzeVerboseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAttachStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAttachStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDatabasePathTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDatabasePathTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAttachAliasTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAttachAliasTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAttachOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAttachOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCallStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCallStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCheckpointStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCheckpointStatementTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeCheckpointForceTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCheckpointForceTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCommentStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommentStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCommentOnTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommentOnTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCommentTableTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommentTableTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCommentSequenceTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommentSequenceTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCommentFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommentFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCommentMacroTableTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommentMacroTableTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCommentMacroTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommentMacroTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCommentViewTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommentViewTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCommentDatabaseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommentDatabaseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCommentIndexTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommentIndexTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCommentSchemaTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommentSchemaTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCommentTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommentTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCommentColumnTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommentColumnTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCommentValueTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommentValueTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeStringLiteralValueTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeStringLiteralValueTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeAnalyzeKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAnalyzeKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExpressionStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeExpressionStatementTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeExpressionAliasTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExpressionAliasTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIndexNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIndexNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeConstraintNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeConstraintNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSequenceNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSequenceNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCollationNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCollationNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNumberLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNumberLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeStringLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeStringLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                               TransformStackFrame &frame);
	static void InitializeTypeVariationsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTypeVariationsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSimpleTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSimpleTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCharacterSimpleTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCharacterSimpleTypeTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeQualifiedSimpleTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeQualifiedSimpleTypeTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeIntervalTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIntervalTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIntervalIntervalTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIntervalIntervalTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIntervalWithSpecifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIntervalWithSpecifierTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeIntervalWithRangeSpecifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIntervalWithRangeSpecifierTrampoline(PEGTransformer &transformer,
	                                                                                     TransformStack &stack,
	                                                                                     TransformStackFrame &frame);
	static void InitializeIntervalWithSimpleSpecifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIntervalWithSimpleSpecifierTrampoline(PEGTransformer &transformer,
	                                                                                      TransformStack &stack,
	                                                                                      TransformStackFrame &frame);
	static void InitializeIntervalWithoutSpecifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIntervalWithoutSpecifierTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeIntervalToIntervalAsTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIntervalToIntervalAsTypeTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeYearKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeYearKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMonthKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMonthKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDayKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDayKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeHourKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeHourKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMinuteKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMinuteKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSecondKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSecondKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMillisecondKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeMillisecondKeywordTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeMicrosecondKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeMicrosecondKeywordTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeWeekKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWeekKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeQuarterKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeQuarterKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDecadeKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDecadeKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCenturyKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCenturyKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMillenniumKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMillenniumKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIntervalTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIntervalTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIntervalToIntervalTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIntervalToIntervalTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeYearToMonthTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeYearToMonthTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDayToHourTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDayToHourTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDayToMinuteTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDayToMinuteTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDayToSecondTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDayToSecondTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeHourToMinuteTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeHourToMinuteTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeHourToSecondTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeHourToSecondTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMinuteToSecondTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMinuteToSecondTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBitTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBitTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGeometryTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeGeometryTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeVariantTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeVariantTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNumericTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNumericTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSimpleNumericTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSimpleNumericTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDecimalNumericTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeDecimalNumericTypeTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeIntTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIntTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIntegerTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIntegerTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSmallintTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSmallintTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBigintTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBigintTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRealTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRealTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBooleanTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBooleanTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDoubleTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDoubleTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFloatTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFloatTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDecimalTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDecimalTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDecTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDecTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNumericModTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNumericModTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeQualifiedTypeNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeQualifiedTypeNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTypeNameAsQualifiedNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTypeNameAsQualifiedNameTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeCatalogReservedSchemaTypeNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCatalogReservedSchemaTypeNameTrampoline(PEGTransformer &transformer,
	                                                                                        TransformStack &stack,
	                                                                                        TransformStackFrame &frame);
	static void InitializeSchemaReservedTypeNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSchemaReservedTypeNameTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeTypeModifiersTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTypeModifiersTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRowTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRowTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSetofTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetofTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUnionTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUnionTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColIdTypeListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColIdTypeListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMapTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMapTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTupleTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTupleTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColIdTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColIdTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeArrayBoundsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeArrayBoundsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeArrayKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeArrayKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSquareBracketsArrayTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSquareBracketsArrayTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeTimeTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTimeTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTimeOrTimestampTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTimeOrTimestampTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTimeTypeIdTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTimeTypeIdTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTimestampTypeIdTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTimestampTypeIdTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTimeZoneTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTimeZoneTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWithOrWithoutTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWithOrWithoutTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWithRuleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWithRuleTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWithoutRuleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWithoutRuleTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeConnectStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeConnectStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDisconnectStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeDisconnectStatementTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeSessionTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSessionTargetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLocalSessionTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeLocalSessionTargetTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeStringSessionTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeStringSessionTargetTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeCatalogSessionTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCatalogSessionTargetTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeCopyStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCopyStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCopyVariationsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCopyVariationsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCopyTableTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCopyTableTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFromOrToTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFromOrToTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCopyFromTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCopyFromTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCopyToTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCopyToTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                                 TransformStackFrame &frame);
	static void InitializeCopySelectTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCopySelectTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCopyFileNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCopyFileNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCopyFileNameExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCopyFileNameExpressionTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeCopyFileNameStringLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCopyFileNameStringLiteralTrampoline(PEGTransformer &transformer,
	                                                                                    TransformStack &stack,
	                                                                                    TransformStackFrame &frame);
	static void InitializeCopyFileNameIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCopyFileNameIdentifierTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeCopyFileNameIdentifierColIdTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCopyFileNameIdentifierColIdTrampoline(PEGTransformer &transformer,
	                                                                                      TransformStack &stack,
	                                                                                      TransformStackFrame &frame);
	static void InitializeIdentifierColIdTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIdentifierColIdTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCopyOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCopyOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCopyOptionListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCopyOptionListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSpecializedOptionListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSpecializedOptionListTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeSpecializedOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSpecializedOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSingleOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSingleOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBinaryOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBinaryOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFreezeOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFreezeOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOidsOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOidsOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCsvOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCsvOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeHeaderOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeHeaderOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNullAsOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNullAsOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDelimiterAsOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDelimiterAsOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeQuoteAsOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeQuoteAsOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeEscapeAsOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeEscapeAsOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeEncodingOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeEncodingOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeForceQuoteOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeForceQuoteOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeStarSymbolColumnListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeStarSymbolColumnListTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeForceQuoteTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeForceQuoteTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePartitionByOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePartitionByOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeForceNullOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeForceNullOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeForceNotNullTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeForceNotNullTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGenericCopyOptionListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeGenericCopyOptionListTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeGenericCopyOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeGenericCopyOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGenericCopyOptionValueTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeGenericCopyOptionValueTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeGenericCopyOptionOrderListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeGenericCopyOptionOrderListTrampoline(PEGTransformer &transformer,
	                                                                                     TransformStack &stack,
	                                                                                     TransformStackFrame &frame);
	static void InitializeGenericCopyOptionExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeGenericCopyOptionExpressionTrampoline(PEGTransformer &transformer,
	                                                                                      TransformStack &stack,
	                                                                                      TransformStackFrame &frame);
	static void InitializeGenericCopyOptionParenthesizedExpressionListTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeGenericCopyOptionParenthesizedExpressionListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                               TransformStackFrame &frame);
	static void InitializeCopyFromDatabaseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCopyFromDatabaseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCopyFromDatabaseWithFlagTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCopyFromDatabaseWithFlagTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeCopyFromDatabaseWithoutFlagTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCopyFromDatabaseWithoutFlagTrampoline(PEGTransformer &transformer,
	                                                                                      TransformStack &stack,
	                                                                                      TransformStackFrame &frame);
	static void InitializeCopyDatabaseFlagTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCopyDatabaseFlagTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSchemaOrDataTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSchemaOrDataTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCopySchemaTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCopySchemaTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCopyDataTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCopyDataTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCreateIndexStmtTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCreateIndexStmtTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWithListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWithListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRelOptionOrOidsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRelOptionOrOidsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRelOptionListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRelOptionListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOidsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeOidsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                               TransformStackFrame &frame);
	static void InitializeWithOrWithoutOidsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWithOrWithoutOidsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWithOidsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWithOidsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWithoutOidsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWithoutOidsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIndexElementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIndexElementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUniqueIndexTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUniqueIndexTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIndexTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIndexTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRelOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRelOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRelOptionNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRelOptionNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDottedIdentifierStringTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeDottedIdentifierStringTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeRelOptionArgumentOptTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeRelOptionArgumentOptTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeDefArgTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeDefArgTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                                 TransformStackFrame &frame);
	static void InitializeDefArgNullTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDefArgNullTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDefArgKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDefArgKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDefArgStringLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeDefArgStringLiteralTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeNoneLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNoneLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCreateMacroStmtTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCreateMacroStmtTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMacroOrFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMacroOrFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMacroKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMacroKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFunctionKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFunctionKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMacroDefinitionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMacroDefinitionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMacroDefinitionBodyTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeMacroDefinitionBodyTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeMacroParametersTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMacroParametersTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMacroParameterTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMacroParameterTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSimpleParameterTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSimpleParameterTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeScalarMacroDefinitionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeScalarMacroDefinitionTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeTableMacroDefinitionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTableMacroDefinitionTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeCreateSchemaStmtTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCreateSchemaStmtTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCreateSecretStmtTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCreateSecretStmtTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSecretStorageSpecifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSecretStorageSpecifierTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeSecretNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSecretNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCreateSequenceStmtTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCreateSequenceStmtTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeSequenceOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSequenceOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSeqSetCycleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSeqSetCycleTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSeqCycleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSeqCycleTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSeqNoCycleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSeqNoCycleTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSeqSetIncrementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSeqSetIncrementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSeqSetMinMaxTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSeqSetMinMaxTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSeqNoMinMaxTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSeqNoMinMaxTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSeqStartWithTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSeqStartWithTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSeqOwnedByTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSeqOwnedByTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSeqMinOrMaxTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSeqMinOrMaxTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMinValueTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMinValueTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMaxValueTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMaxValueTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCreateStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCreateStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCreateStatementVariationTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCreateStatementVariationTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeOrReplaceTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOrReplaceTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTemporaryTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTemporaryTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePersistentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePersistentTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTempPersistentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTempPersistentTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTemporaryPersistentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTemporaryPersistentTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeCreateTableStmtTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCreateTableStmtTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCreateTableDefinitionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCreateTableDefinitionTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeCreateTableAsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCreateTableAsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePartitionSortedOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizePartitionSortedOptionsTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializePartitionOptSortedOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizePartitionOptSortedOptionsTrampoline(PEGTransformer &transformer,
	                                                                                    TransformStack &stack,
	                                                                                    TransformStackFrame &frame);
	static void InitializeSortedOptPartitionOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSortedOptPartitionOptionsTrampoline(PEGTransformer &transformer,
	                                                                                    TransformStack &stack,
	                                                                                    TransformStackFrame &frame);
	static void InitializePartitionOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePartitionOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSortedOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSortedOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWithDataTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWithDataTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWithDataOnlyTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWithDataOnlyTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWithNoDataTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWithNoDataTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIdentifierListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIdentifierListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCreateColumnListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCreateColumnListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIfNotExistsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIfNotExistsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeQualifiedNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeQualifiedNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSchemaReservedIdentifierOrStringLiteralTrampoline(PEGTransformer &transformer,
	                                                                        TransformStack &stack,
	                                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSchemaReservedIdentifierOrStringLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static void InitializeCatalogReservedSchemaIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCatalogReservedSchemaIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static void InitializeIdentifierOrStringLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIdentifierOrStringLiteralTrampoline(PEGTransformer &transformer,
	                                                                                    TransformStack &stack,
	                                                                                    TransformStackFrame &frame);
	static void InitializeReservedIdentifierOrStringLiteralTrampoline(PEGTransformer &transformer,
	                                                                  TransformStack &stack,
	                                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeReservedIdentifierOrStringLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static void InitializeCatalogQualificationTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCatalogQualificationTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeSchemaQualificationTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSchemaQualificationTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeReservedSchemaQualificationTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeReservedSchemaQualificationTrampoline(PEGTransformer &transformer,
	                                                                                      TransformStack &stack,
	                                                                                      TransformStackFrame &frame);
	static void InitializeTableQualificationTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTableQualificationTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeReservedTableQualificationTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeReservedTableQualificationTrampoline(PEGTransformer &transformer,
	                                                                                     TransformStack &stack,
	                                                                                     TransformStackFrame &frame);
	static void InitializeCreateTableColumnListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCreateTableColumnListTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeCreateTableColumnElementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCreateTableColumnElementTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeCreateTableColumnDefinitionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCreateTableColumnDefinitionTrampoline(PEGTransformer &transformer,
	                                                                                      TransformStack &stack,
	                                                                                      TransformStackFrame &frame);
	static void InitializeCreateTableConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCreateTableConstraintTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeColumnDefinitionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColumnDefinitionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColumnConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColumnConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNotNullConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNotNullConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNullConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNullConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNotNullColumnConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeNotNullColumnConstraintTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeUniqueConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUniqueConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePrimaryKeyConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizePrimaryKeyConstraintTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeDefaultValueTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDefaultValueTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCheckConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCheckConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeForeignKeyConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeForeignKeyConstraintTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeColumnCollationTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColumnCollationTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColumnCompressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColumnCompressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeKeyActionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeKeyActionsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUpdateActionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUpdateActionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDeleteActionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDeleteActionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeKeyActionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeKeyActionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNoKeyActionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNoKeyActionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRestrictKeyActionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRestrictKeyActionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCascadeKeyActionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCascadeKeyActionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSetNullKeyActionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetNullKeyActionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSetDefaultKeyActionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSetDefaultKeyActionTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeTopLevelConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTopLevelConstraintTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeTopLevelConstraintListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTopLevelConstraintListTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeTopCheckConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTopCheckConstraintTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeTopPrimaryKeyConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTopPrimaryKeyConstraintTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeTopUniqueConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTopUniqueConstraintTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeTopForeignKeyConstraintTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTopForeignKeyConstraintTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeColumnIdListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColumnIdListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDottedIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDottedIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDotColLabelTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDotColLabelTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColIdTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeColIdTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                                TransformStackFrame &frame);
	static void InitializeColIdOrStringTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColIdOrStringTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTypeFuncNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTypeFuncNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColLabelTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColLabelTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColLabelOrStringTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColLabelOrStringTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColLabelIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeColLabelIdentifierTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeStringLiteralIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeStringLiteralIdentifierTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeGeneratedColumnTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeGeneratedColumnTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGeneratedColumnTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeGeneratedColumnTypeTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeCommitActionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommitActionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePreserveOrDeleteTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePreserveOrDeleteTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePreserveRowsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePreserveRowsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDeleteRowsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDeleteRowsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeVirtualGeneratedColumnTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeVirtualGeneratedColumnTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeStoredGeneratedColumnTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeStoredGeneratedColumnTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeCreateTriggerStmtTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCreateTriggerStmtTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTriggerBodyTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTriggerBodyTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTriggerNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTriggerNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeReferencingClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeReferencingClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeReferencingItemTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeReferencingItemTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeReferencingNewTableAsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeReferencingNewTableAsTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeReferencingOldTableAsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeReferencingOldTableAsTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeTriggerTimingTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTriggerTimingTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTriggerBeforeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTriggerBeforeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTriggerAfterTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTriggerAfterTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTriggerInsteadOfTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTriggerInsteadOfTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTriggerEventTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTriggerEventTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTriggerEventInsertTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTriggerEventInsertTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeTriggerEventDeleteTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTriggerEventDeleteTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeTriggerEventUpdateTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTriggerEventUpdateTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeTriggerEventUpdateOfTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTriggerEventUpdateOfTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeTriggerColumnListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTriggerColumnListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeForEachClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeForEachClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeForEachRowTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeForEachRowTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeForEachStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeForEachStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCreateTypeStmtTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCreateTypeStmtTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCreateTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCreateTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCreateTypeFromTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCreateTypeFromTypeTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeEnumSelectTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeEnumSelectTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeEnumStringLiteralListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeEnumStringLiteralListTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeCreateViewStmtTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCreateViewStmtTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCreateRecursiveTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCreateRecursiveTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDeallocateStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeDeallocateStatementTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeDeallocatePrepareTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDeallocatePrepareTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDeleteStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDeleteStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTruncateStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTruncateStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTargetOptAliasTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTargetOptAliasTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDeleteUsingClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDeleteUsingClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDescribeStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDescribeStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeShowSelectTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeShowSelectTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeShowAllTablesTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeShowAllTablesTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeShowQualifiedNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeShowQualifiedNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeShowTablesTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeShowTablesTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDescribeTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDescribeTargetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDescribeBaseTableNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeDescribeBaseTableNameTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeDescribeStringLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeDescribeStringLiteralTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeShowOrDescribeOrSummarizeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeShowOrDescribeOrSummarizeTrampoline(PEGTransformer &transformer,
	                                                                                    TransformStack &stack,
	                                                                                    TransformStackFrame &frame);
	static void InitializeSummarizeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSummarizeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSummarizeRuleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSummarizeRuleTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeShowOrDescribeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeShowOrDescribeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeShowRuleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeShowRuleTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDescribeRuleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDescribeRuleTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDescribeLongRuleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDescribeLongRuleTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDescRuleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDescRuleTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDetachStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDetachStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropEntriesTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropEntriesTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropTriggerTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropTriggerTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropTableTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropTableTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropTableFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropTableFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropSchemaTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropSchemaTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropIndexTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropIndexTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeQualifiedIndexNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeQualifiedIndexNameTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeQualifiedIndexNameStringTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeQualifiedIndexNameStringTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeSchemaReservedIndexTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSchemaReservedIndexTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeCatalogReservedSchemaIndexTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCatalogReservedSchemaIndexTrampoline(PEGTransformer &transformer,
	                                                                                     TransformStack &stack,
	                                                                                     TransformStackFrame &frame);
	static void InitializeDropSequenceTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropSequenceTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropCollationTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropCollationTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropSecretTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropSecretTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTableOrViewTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTableOrViewTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMaterializedViewEntryTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeMaterializedViewEntryTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeFunctionTypeMacroTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFunctionTypeMacroTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFunctionTypeMacroKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeFunctionTypeMacroKeywordTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeFunctionTypeFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeFunctionTypeFunctionTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeDropBehaviorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropBehaviorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCascadeDropBehaviorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCascadeDropBehaviorTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeRestrictDropBehaviorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeRestrictDropBehaviorTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeIfExistsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIfExistsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDropSecretStorageTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDropSecretStorageTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExecuteStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExecuteStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExplainStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExplainStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExplainOptionListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExplainOptionListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExplainOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExplainOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExplainOptionNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExplainOptionNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExplainSelectStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeExplainSelectStatementTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeExplainableStatementsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeExplainableStatementsTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeExportStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExportStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExportSourceTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExportSourceTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeImportStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeImportStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColumnReferenceTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColumnReferenceTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCatalogReservedSchemaTableColumnNameTrampoline(PEGTransformer &transformer,
	                                                                     TransformStack &stack,
	                                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCatalogReservedSchemaTableColumnNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static void InitializeSchemaReservedTableColumnNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSchemaReservedTableColumnNameTrampoline(PEGTransformer &transformer,
	                                                                                        TransformStack &stack,
	                                                                                        TransformStackFrame &frame);
	static void InitializeTableReservedColumnNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTableReservedColumnNameTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeFunctionExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeFunctionExpressionTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeFunctionExpressionArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeFunctionExpressionArgumentsTrampoline(PEGTransformer &transformer,
	                                                                                      TransformStack &stack,
	                                                                                      TransformStackFrame &frame);
	static void InitializeFunctionExpressionArgumentListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFunctionExpressionArgumentListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static void InitializeFunctionArgumentListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeFunctionArgumentListTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeFunctionIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeFunctionIdentifierTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeFunctionNameAsQualifiedNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeFunctionNameAsQualifiedNameTrampoline(PEGTransformer &transformer,
	                                                                                      TransformStack &stack,
	                                                                                      TransformStackFrame &frame);
	static void InitializeCatalogReservedSchemaFunctionNameTrampoline(PEGTransformer &transformer,
	                                                                  TransformStack &stack,
	                                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCatalogReservedSchemaFunctionNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static void InitializeSchemaReservedFunctionNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSchemaReservedFunctionNameTrampoline(PEGTransformer &transformer,
	                                                                                     TransformStack &stack,
	                                                                                     TransformStackFrame &frame);
	static void InitializeDistinctOrAllTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDistinctOrAllTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDistinctKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDistinctKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAllKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAllKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWithinGroupClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWithinGroupClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFilterClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFilterClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFilterClauseExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeFilterClauseExpressionTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeFilterClauseContentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeFilterClauseContentsTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeIgnoreOrRespectNullsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIgnoreOrRespectNullsTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeIgnoreNullsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIgnoreNullsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRespectNullsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRespectNullsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeParenthesisExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeParenthesisExpressionTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeConstantLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeConstantLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNullLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNullLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTrueLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTrueLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFalseLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFalseLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCastExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCastExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCastArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCastArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCastOrTryCastTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCastOrTryCastTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCastKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCastKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTryCastKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTryCastKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColIdDotTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColIdDotTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeStarExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeStarExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeStarQualifierListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeStarQualifierListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExcludeListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExcludeListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExcludeNamesTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExcludeNamesTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExcludeNameListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExcludeNameListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExcludeNameSingleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExcludeNameSingleTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExcludeNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExcludeNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExcludeDottedNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExcludeDottedNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExcludeColumnNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExcludeColumnNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeReplaceListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeReplaceListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeReplaceEntriesTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeReplaceEntriesTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeReplaceEntrySingleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeReplaceEntrySingleTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeReplaceEntryListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeReplaceEntryListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeReplaceEntryTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeReplaceEntryTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRenameListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRenameListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRenameEntriesTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRenameEntriesTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRenameEntryListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRenameEntryListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSingleRenameEntryTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSingleRenameEntryTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRenameEntryTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRenameEntryTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSubqueryExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSubqueryExpressionTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeSubqueryNotTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSubqueryNotTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSubqueryExistsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSubqueryExistsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCaseExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCaseExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCaseWhenThenTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCaseWhenThenTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCaseElseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCaseElseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTypeLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTypeLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIntervalLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIntervalLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIntervalParameterTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIntervalParameterTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIntervalStringParameterTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIntervalStringParameterTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeFrameClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFrameClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFramingTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFramingTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRowsFramingTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRowsFramingTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRangeFramingTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRangeFramingTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGroupsFramingTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeGroupsFramingTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFrameExtentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFrameExtentTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSingleFrameExtentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSingleFrameExtentTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBetweenFrameExtentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeBetweenFrameExtentTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeFrameBoundTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFrameBoundTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFrameUnboundedTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFrameUnboundedTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFrameExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFrameExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFrameCurrentRowTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFrameCurrentRowTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePrecedingOrFollowingTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizePrecedingOrFollowingTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializePrecedingFrameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePrecedingFrameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFollowingFrameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFollowingFrameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWindowExcludeClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeWindowExcludeClauseTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeWindowExcludeElementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeWindowExcludeElementTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeExcludeCurrentRowTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExcludeCurrentRowTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExcludeGroupTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExcludeGroupTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExcludeTiesTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExcludeTiesTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExcludeNoOthersTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExcludeNoOthersTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWindowFrameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWindowFrameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIdentifierWindowFrameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIdentifierWindowFrameTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeParensIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeParensIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWindowFrameDefinitionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeWindowFrameDefinitionTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeWindowFrameNameContentsParensTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeWindowFrameNameContentsParensTrampoline(PEGTransformer &transformer,
	                                                                                        TransformStack &stack,
	                                                                                        TransformStackFrame &frame);
	static void InitializeWindowFrameNameContentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeWindowFrameNameContentsTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeWindowFrameContentsParensTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeWindowFrameContentsParensTrampoline(PEGTransformer &transformer,
	                                                                                    TransformStack &stack,
	                                                                                    TransformStackFrame &frame);
	static void InitializeWindowFrameContentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeWindowFrameContentsTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeBaseWindowNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBaseWindowNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWindowPartitionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWindowPartitionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeListExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeListExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeArrayBoundedListExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeArrayBoundedListExpressionTrampoline(PEGTransformer &transformer,
	                                                                                     TransformStack &stack,
	                                                                                     TransformStackFrame &frame);
	static void InitializeArrayParensSelectTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeArrayParensSelectTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBoundedListExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeBoundedListExpressionTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeStructExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeStructExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeStructFieldTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeStructFieldTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMapExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMapExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMapStructExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeMapStructExpressionTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeMapStructFieldTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMapStructFieldTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGroupingExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeGroupingExpressionTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeGroupingOrGroupingIdTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeGroupingOrGroupingIdTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeGroupingKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeGroupingKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGroupingIdKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeGroupingIdKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeParameterTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeParameterTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeQuestionMarkNumberedParameterTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeQuestionMarkNumberedParameterTrampoline(PEGTransformer &transformer,
	                                                                                        TransformStack &stack,
	                                                                                        TransformStackFrame &frame);
	static void InitializeAnonymousParameterTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeAnonymousParameterTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeNumberedParameterTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNumberedParameterTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColLabelParameterTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColLabelParameterTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePositionalExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizePositionalExpressionTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeDefaultExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDefaultExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeListComprehensionExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeListComprehensionExpressionTrampoline(PEGTransformer &transformer,
	                                                                                      TransformStack &stack,
	                                                                                      TransformStackFrame &frame);
	static void InitializeListComprehensionFilterTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeListComprehensionFilterTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeParensExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeParensExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSingleExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSingleExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColumnDefaultExprTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColumnDefaultExprTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLambdaArrowExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeLambdaArrowExpressionTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeSingleArrowPairTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSingleArrowPairTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLogicalOrExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeLogicalOrExpressionTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeLogicalOrExpressionTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeLogicalOrExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeColDefOrExprTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColDefOrExprTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColDefOrExpressionTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeColDefOrExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeLogicalAndExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeLogicalAndExpressionTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeLogicalAndExpressionTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeLogicalAndExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeColDefAndExprTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColDefAndExprTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColDefAndExpressionTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeColDefAndExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeLogicalNotExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeLogicalNotExpressionTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeNotExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNotExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNotKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNotKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIsExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIsExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIsTestTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIsTestTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                                 TransformStackFrame &frame);
	static void InitializeIsLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIsLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIsLiteralValueTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIsLiteralValueTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUnknownLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUnknownLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNotNullTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNotNullTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNotNullKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNotNullKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNotNullOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNotNullOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIsNullTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIsNullTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                                 TransformStackFrame &frame);
	static void InitializeIsNullOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIsNullOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIsDistinctFromExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIsDistinctFromExpressionTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeIsDistinctFromTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIsDistinctFromTailTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeIsDistinctFromOpTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIsDistinctFromOpTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeComparisonExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeComparisonExpressionTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeComparisonExpressionTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeComparisonExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeComparisonOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeComparisonOperatorTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeOperatorEqualTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOperatorEqualTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOperatorNotEqualTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOperatorNotEqualTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOperatorLessThanTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOperatorLessThanTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOperatorGreaterThanTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeOperatorGreaterThanTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeOperatorLessThanEqualsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeOperatorLessThanEqualsTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeOperatorGreaterThanEqualsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeOperatorGreaterThanEqualsTrampoline(PEGTransformer &transformer,
	                                                                                    TransformStack &stack,
	                                                                                    TransformStackFrame &frame);
	static void InitializeBetweenInLikeExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeBetweenInLikeExpressionTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeBetweenInLikeOpTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBetweenInLikeOpTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBetweenInLikeOpExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeBetweenInLikeOpExpressionTrampoline(PEGTransformer &transformer,
	                                                                                    TransformStack &stack,
	                                                                                    TransformStackFrame &frame);
	static void InitializeLikeClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLikeClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeEscapeClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeEscapeClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLikeVariationsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLikeVariationsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLikeTokenTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLikeTokenTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeILikeTokenTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeILikeTokenTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGlobTokenTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeGlobTokenTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSimilarToTokenTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSimilarToTokenTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRegexMatchTokenTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRegexMatchTokenTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRegexInsensitiveMatchTokenTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeRegexInsensitiveMatchTokenTrampoline(PEGTransformer &transformer,
	                                                                                     TransformStack &stack,
	                                                                                     TransformStackFrame &frame);
	static void InitializeNotILikeOpTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNotILikeOpTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNotLikeOpTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNotLikeOpTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNotRegexInsensitiveMatchOpTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeNotRegexInsensitiveMatchOpTrampoline(PEGTransformer &transformer,
	                                                                                     TransformStack &stack,
	                                                                                     TransformStackFrame &frame);
	static void InitializeNotSimilarToOpTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNotSimilarToOpTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInContainsExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeInContainsExpressionTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeInExpressionListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInExpressionListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInSelectStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInSelectStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBetweenClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBetweenClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOtherOperatorExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeOtherOperatorExpressionTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeOtherOperatorTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOtherOperatorTailTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOtherOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOtherOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAnyAllParsedOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeAnyAllParsedOperatorTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeNamedOtherOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeNamedOtherOperatorTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeOperatorLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOperatorLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAnyAllOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAnyAllOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAnyOrAllTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAnyOrAllTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSubqueryAnyTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSubqueryAnyTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSubqueryAllTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSubqueryAllTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInetOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInetOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeJsonOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeJsonOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeListOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeListOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeStringOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeStringOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeQualifiedOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeQualifiedOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeQualifiedOperatorContentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeQualifiedOperatorContentsTrampoline(PEGTransformer &transformer,
	                                                                                    TransformStack &stack,
	                                                                                    TransformStackFrame &frame);
	static void InitializeAnyOpTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeAnyOpTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                                TransformStackFrame &frame);
	static void InitializeBitwiseExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBitwiseExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBitwiseExpressionTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeBitwiseExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeBitOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBitOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAdditiveExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeAdditiveExpressionTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeAdditiveExpressionTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeAdditiveExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeTermTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTermTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                               TransformStackFrame &frame);
	static void InitializeMultiplicativeExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeMultiplicativeExpressionTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeMultiplicativeExpressionTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeMultiplicativeExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                       TransformStack &stack,
	                                                                                       TransformStackFrame &frame);
	static void InitializeFactorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeFactorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                                 TransformStackFrame &frame);
	static void InitializeExponentiationExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeExponentiationExpressionTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeExponentiationExpressionTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeExponentiationExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                       TransformStack &stack,
	                                                                                       TransformStackFrame &frame);
	static void InitializeExponentOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExponentOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCollateExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCollateExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCollateExpressionTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCollateExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeAtTimeZoneExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeAtTimeZoneExpressionTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeAtTimeZoneExpressionTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeAtTimeZoneExpressionTailTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializePrefixOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePrefixOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMinusPrefixOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeMinusPrefixOperatorTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializePlusPrefixOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizePlusPrefixOperatorTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeTildePrefixOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTildePrefixOperatorTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeBaseExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBaseExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIndirectionListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIndirectionListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIndirectionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIndirectionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCastOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCastOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDotOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDotOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDotMethodOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDotMethodOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDotColumnOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDotColumnOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMethodExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMethodExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMethodExpressionArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeMethodExpressionArgumentsTrampoline(PEGTransformer &transformer,
	                                                                                    TransformStack &stack,
	                                                                                    TransformStackFrame &frame);
	static void InitializeMethodExpressionArgumentListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeMethodExpressionArgumentListTrampoline(PEGTransformer &transformer,
	                                                                                       TransformStack &stack,
	                                                                                       TransformStackFrame &frame);
	static void InitializeMethodFunctionArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeMethodFunctionArgumentsTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeSliceExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSliceExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSliceBoundTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSliceBoundTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeEndSliceBoundTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeEndSliceBoundTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeEndSliceValueTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeEndSliceValueTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeEndSliceMinusTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeEndSliceMinusTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeStepSliceBoundTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeStepSliceBoundTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePostfixOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePostfixOperatorTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSpecialFunctionExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSpecialFunctionExpressionTrampoline(PEGTransformer &transformer,
	                                                                                    TransformStack &stack,
	                                                                                    TransformStackFrame &frame);
	static void InitializeCoalesceExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCoalesceExpressionTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeUnpackExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUnpackExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTryExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTryExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColumnsExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColumnsExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExtractExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExtractExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExtractArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExtractArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLambdaExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLambdaExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNullIfExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNullIfExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNullIfArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNullIfArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePositionExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizePositionExpressionTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializePositionArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePositionArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRowExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRowExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSubstringExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSubstringExpressionTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeSubstringArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSubstringArgumentsTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeSubstringExpressionListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSubstringExpressionListTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeSubstringParametersTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSubstringParametersTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeSubstringFromForTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSubstringFromForTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSubstringFromOptionalForTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSubstringFromOptionalForTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeSubstringForTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSubstringForTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTrimExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTrimExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTrimArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTrimArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTrimDirectionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTrimDirectionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTrimBothTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTrimBothTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTrimLeadingTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTrimLeadingTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTrimTrailingTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTrimTrailingTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTrimSourceTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTrimSourceTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOverlayExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOverlayExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOverlayArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOverlayArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOverlayParametersTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOverlayParametersTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFromExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFromExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeForExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeForExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOverlayExpressionListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeOverlayExpressionListTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeExtractArgumentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExtractArgumentTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExtractDatePartArgumentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeExtractDatePartArgumentTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeExtractIdentifierArgumentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeExtractIdentifierArgumentTrampoline(PEGTransformer &transformer,
	                                                                                    TransformStack &stack,
	                                                                                    TransformStackFrame &frame);
	static void InitializeExtractStringArgumentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeExtractStringArgumentTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeExtractDatePartTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExtractDatePartTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInsertStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInsertStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOrActionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOrActionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInsertOrReplaceTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInsertOrReplaceTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInsertOrIgnoreTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInsertOrIgnoreTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeByNameOrPositionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeByNameOrPositionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInsertByNameOrderTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInsertByNameOrderTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInsertByPositionOrderTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeInsertByPositionOrderTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeInsertByNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInsertByNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInsertByPositionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInsertByPositionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInsertTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInsertTargetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInsertAliasTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInsertAliasTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColumnListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColumnListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInsertColumnListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInsertColumnListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInsertValuesTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInsertValuesTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSelectInsertValuesTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSelectInsertValuesTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeDefaultValuesTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDefaultValuesTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOnConflictClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOnConflictClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOnConflictTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOnConflictTargetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOnConflictExpressionTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeOnConflictExpressionTargetTrampoline(PEGTransformer &transformer,
	                                                                                     TransformStack &stack,
	                                                                                     TransformStackFrame &frame);
	static void InitializeOnConflictIndexTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeOnConflictIndexTargetTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeOnConflictActionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOnConflictActionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOnConflictUpdateTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOnConflictUpdateTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOnConflictNothingTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOnConflictNothingTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeReturningClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeReturningClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLoadStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLoadStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExtensionAliasTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExtensionAliasTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInstallStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInstallStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUpdateExtensionsStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeUpdateExtensionsStatementTrampoline(PEGTransformer &transformer,
	                                                                                    TransformStack &stack,
	                                                                                    TransformStackFrame &frame);
	static void InitializeFromSourceTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFromSourceTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFromSourceIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeFromSourceIdentifierTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeFromSourceStringTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFromSourceStringTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeVersionNumberTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeVersionNumberTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMergeIntoStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeMergeIntoStatementTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeMergeIntoUsingClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeMergeIntoUsingClauseTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeMergeMatchTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMergeMatchTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMatchedClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMatchedClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMatchedClauseActionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeMatchedClauseActionTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeUpdateMatchClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUpdateMatchClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUpdateMatchInfoTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUpdateMatchInfoTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUpdateMatchSetActionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeUpdateMatchSetActionTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeUpdateByNameOrPositionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeUpdateByNameOrPositionTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeDeleteMatchClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDeleteMatchClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInsertMatchClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInsertMatchClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInsertMatchInfoTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInsertMatchInfoTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInsertDefaultValuesTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeInsertDefaultValuesTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeInsertByNameOrPositionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeInsertByNameOrPositionTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeInsertValuesListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInsertValuesListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDoNothingMatchClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeDoNothingMatchClauseTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeErrorMatchClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeErrorMatchClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUpdateMatchSetClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeUpdateMatchSetClauseTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeUpdateMatchSetInfoTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeUpdateMatchSetInfoTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeAndExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAndExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNotMatchedClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNotMatchedClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBySourceOrTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBySourceOrTargetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBySourceTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBySourceTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeByTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeByTargetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePivotOnTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePivotOnTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePivotUsingTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePivotUsingTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePivotColumnListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePivotColumnListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePivotColumnEntryTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePivotColumnEntryTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePivotColumnExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizePivotColumnExpressionTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializePivotColumnSubqueryTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizePivotColumnSubqueryTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeIntoNameValuesTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIntoNameValuesTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIncludeOrExcludeNullsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIncludeOrExcludeNullsTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeIncludeNullsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIncludeNullsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExcludeNullsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeExcludeNullsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUnpivotHeaderTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUnpivotHeaderTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUnpivotHeaderSingleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeUnpivotHeaderSingleTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeUnpivotHeaderListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUnpivotHeaderListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePragmaStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePragmaStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePragmaAssignOrFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizePragmaAssignOrFunctionTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializePragmaAssignTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePragmaAssignTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePragmaFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePragmaFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePragmaParametersTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePragmaParametersTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePrepareStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePrepareStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTypeListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTypeListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSelectStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSelectStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSelectSetOpChainTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSelectSetOpChainTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSelectSetOpChainTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSelectSetOpChainTailTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeIntersectChainTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeIntersectChainTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeIntersectChainTailTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeIntersectChainTailTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeSetIntersectClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSetIntersectClauseTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeSelectAtomTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSelectAtomTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSelectParensTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSelectParensTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSetopClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetopClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSetopTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetopTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSetopUnionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetopUnionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSetopExceptTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetopExceptTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSelectStatementTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSelectStatementTypeTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeResultModifiersTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeResultModifiersTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLimitOffsetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLimitOffsetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLimitOffsetClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLimitOffsetClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOffsetLimitClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOffsetLimitClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOffsetFetchClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOffsetFetchClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFetchOnlyClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFetchOnlyClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTableStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTableStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOptionalParensSimpleSelectTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeOptionalParensSimpleSelectTrampoline(PEGTransformer &transformer,
	                                                                                     TransformStack &stack,
	                                                                                     TransformStackFrame &frame);
	static void InitializeSimpleSelectParensTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSimpleSelectParensTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeSelectFromTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSelectFromTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSelectFromClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSelectFromClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFromSelectClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFromSelectClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWithStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWithStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCTEBodyTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCTEBodyTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCTESelectBodyTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCTESelectBodyTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCTEDMLBodyTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCTEDMLBodyTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUsingKeyTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUsingKeyTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeMaterializedTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeMaterializedTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSelectClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSelectClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTargetListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTargetListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColumnAliasesTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColumnAliasesTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDistinctClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDistinctClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDistinctAllTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDistinctAllTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDistinctOnTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDistinctOnTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDistinctOnTargetsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDistinctOnTargetsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInnerTableRefTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInnerTableRefTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTableSubqueryTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTableSubqueryTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBaseTableRefTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBaseTableRefTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTableAliasColonTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTableAliasColonTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeValuesRefTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeValuesRefTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeParensTableRefTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeParensTableRefTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeJoinOrPivotTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeJoinOrPivotTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTablePivotClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTablePivotClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTablePivotClauseBodyTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTablePivotClauseBodyTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializePivotGroupByListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePivotGroupByListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTableUnpivotClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTableUnpivotClauseTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeTableUnpivotClauseBodyTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTableUnpivotClauseBodyTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializePivotHeaderTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePivotHeaderTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePivotValueListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePivotValueListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePivotValueTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePivotValueTargetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePivotEnumTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePivotEnumTargetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePivotListTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePivotListTargetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUnpivotValueListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUnpivotValueListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePivotTargetListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizePivotTargetListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUnpivotTargetListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUnpivotTargetListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLateralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLateralTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBaseTableNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBaseTableNameTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUnqualifiedBaseTableNameTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeUnqualifiedBaseTableNameTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeSchemaReservedTableTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSchemaReservedTableTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeCatalogReservedSchemaTableTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCatalogReservedSchemaTableTrampoline(PEGTransformer &transformer,
	                                                                                     TransformStack &stack,
	                                                                                     TransformStackFrame &frame);
	static void InitializeTableFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTableFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTableFunctionLateralOptTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTableFunctionLateralOptTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeTableFunctionAliasColonTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTableFunctionAliasColonTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeWithOrdinalityTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWithOrdinalityTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeQualifiedTableFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeQualifiedTableFunctionTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeTableFunctionArgumentsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTableFunctionArgumentsTrampoline(PEGTransformer &transformer,
	                                                                                 TransformStack &stack,
	                                                                                 TransformStackFrame &frame);
	static void InitializeFunctionArgumentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFunctionArgumentTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNamedFunctionArgumentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeNamedFunctionArgumentTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializePositionalFunctionArgumentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizePositionalFunctionArgumentTrampoline(PEGTransformer &transformer,
	                                                                                     TransformStack &stack,
	                                                                                     TransformStackFrame &frame);
	static void InitializeNamedParameterTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNamedParameterTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTableAliasTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTableAliasTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTableAliasAsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTableAliasAsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTableAliasWithoutAsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTableAliasWithoutAsTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeAtClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAtClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAtSpecifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAtSpecifierTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAtUnitTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                       TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeAtUnitTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                                 TransformStackFrame &frame);
	static void InitializeVersionAtUnitTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeVersionAtUnitTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTimestampAtUnitTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeTimestampAtUnitTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeJoinClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeJoinClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRegularJoinClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRegularJoinClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeJoinByClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeJoinByClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAsofTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeAsofTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                               TransformStackFrame &frame);
	static void InitializeJoinWithoutOnClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeJoinWithoutOnClauseTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeJoinQualifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeJoinQualifierTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOnClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOnClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUsingClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUsingClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeJoinTypeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeJoinTypeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeJoinPrefixTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeJoinPrefixTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCrossJoinPrefixTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCrossJoinPrefixTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNaturalJoinPrefixTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNaturalJoinPrefixTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializePositionalJoinPrefixTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizePositionalJoinPrefixTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeFullJoinTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFullJoinTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLeftJoinTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLeftJoinTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRightJoinTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRightJoinTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSemiJoinTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSemiJoinTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAntiJoinTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAntiJoinTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeInnerJoinTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeInnerJoinTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFromClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFromClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWhereClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWhereClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGroupByClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeGroupByClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeHavingClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeHavingClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeQualifyClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeQualifyClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSampleClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSampleClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeWindowClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeWindowClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSampleEntryTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSampleEntryTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSampleEntryCountTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSampleEntryCountTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSampleEntryFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSampleEntryFunctionTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeSampleFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSampleFunctionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSamplePropertiesTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSamplePropertiesTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRepeatableSampleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRepeatableSampleTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSampleSeedTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSampleSeedTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSampleCountTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSampleCountTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSampleValueTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSampleValueTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSampleUnitTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSampleUnitTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSamplePercentageTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSamplePercentageTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSampleRowsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSampleRowsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGroupByExpressionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeGroupByExpressionsTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeGroupByAllTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeGroupByAllTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGroupByListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeGroupByListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGroupByExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeGroupByExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGroupByBaseExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeGroupByBaseExpressionTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeEmptyGroupingItemTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeEmptyGroupingItemTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCubeOrRollupClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeCubeOrRollupClauseTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeCubeOrRollupTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCubeOrRollupTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeCubeKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCubeKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRollupKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeRollupKeywordTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGroupingSetsClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeGroupingSetsClauseTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeSubqueryReferenceTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSubqueryReferenceTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOrderByExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOrderByExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDescOrAscTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDescOrAscTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeDescendingOrderTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeDescendingOrderTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAscendingOrderTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAscendingOrderTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNullsFirstOrLastTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNullsFirstOrLastTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNullsFirstTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNullsFirstTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNullsLastTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNullsLastTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOrderByClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOrderByClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOrderByExpressionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeOrderByExpressionsTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeOrderByExpressionListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeOrderByExpressionListTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
	static void InitializeOrderByAllTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOrderByAllTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLimitClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLimitClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOffsetClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOffsetClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOffsetValueTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOffsetValueTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLimitValueTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLimitValueTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLimitAllTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLimitAllTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLimitLiteralPercentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeLimitLiteralPercentTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeLimitExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLimitExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFetchClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFetchClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeFetchValueTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeFetchValueTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeAliasedExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeAliasedExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeColIdExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeColIdExpressionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeExpressionAsCollabelTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeExpressionAsCollabelTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeExpressionOptIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeExpressionOptIdentifierTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeValuesClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeValuesClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeValuesExpressionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeValuesExpressionsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSetStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSetAssignmentOrTimeZoneTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSetAssignmentOrTimeZoneTrampoline(PEGTransformer &transformer,
	                                                                                  TransformStack &stack,
	                                                                                  TransformStackFrame &frame);
	static void InitializeResetStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeResetStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeStandardAssignmentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                   TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeStandardAssignmentTrampoline(PEGTransformer &transformer,
	                                                                             TransformStack &stack,
	                                                                             TransformStackFrame &frame);
	static void InitializeSetVariableOrSettingTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeSetVariableOrSettingTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeSetTimeZoneTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetTimeZoneTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeZoneValueTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeZoneValueTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeZoneLocalTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeZoneLocalTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeZoneDefaultTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeZoneDefaultTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeZoneStringLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeZoneStringLiteralTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeZoneIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeZoneIdentifierTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeZoneIntervalWithIntervalTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeZoneIntervalWithIntervalTrampoline(PEGTransformer &transformer,
	                                                                                   TransformStack &stack,
	                                                                                   TransformStackFrame &frame);
	static void InitializeZoneIntervalWithPrecisionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeZoneIntervalWithPrecisionTrampoline(PEGTransformer &transformer,
	                                                                                    TransformStack &stack,
	                                                                                    TransformStackFrame &frame);
	static void InitializeSetSettingTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetSettingTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSetVariableTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetVariableTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeVariableScopeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeVariableScopeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSettingScopeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSettingScopeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeLocalScopeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeLocalScopeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSessionScopeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSessionScopeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeGlobalScopeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeGlobalScopeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeSetAssignmentTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeSetAssignmentTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeVariableListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeVariableListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeTransactionStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeTransactionStatementTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeBeginTransactionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBeginTransactionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeRollbackTransactionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeRollbackTransactionTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeCommitTransactionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeCommitTransactionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeReadOrWriteTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeReadOrWriteTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeReadOnlyOrReadWriteTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeReadOnlyOrReadWriteTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeReadOnlyTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeReadOnlyTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeReadWriteTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeReadWriteTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUpdateStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUpdateStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUpdateTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUpdateTargetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBaseTableSetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBaseTableSetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeBaseTableAliasSetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                  TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeBaseTableAliasSetTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUpdateAliasTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                            TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUpdateAliasTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUpdateSetClauseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUpdateSetClauseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUpdateSetTupleTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                               TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUpdateSetTupleTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUpdateSetElementListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                     TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeUpdateSetElementListTrampoline(PEGTransformer &transformer,
	                                                                               TransformStack &stack,
	                                                                               TransformStackFrame &frame);
	static void InitializeUpdateSetElementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                 TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeUpdateSetElementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeUpdateSetColumnTargetTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                      TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeUpdateSetColumnTargetTrampoline(PEGTransformer &transformer,
	                                                                                TransformStack &stack,
	                                                                                TransformStackFrame &frame);
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
	static void InitializeVacuumStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeVacuumStatementTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeVacuumOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                              TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeVacuumOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeVacuumParensOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeVacuumParensOptionsTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeVacuumLegacyOptionsTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                                    TransformStackFrame &frame);
	static unique_ptr<TransformResultValue> FinalizeVacuumLegacyOptionsTrampoline(PEGTransformer &transformer,
	                                                                              TransformStack &stack,
	                                                                              TransformStackFrame &frame);
	static void InitializeVacuumOptionTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                             TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeVacuumOptionTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOptAnalyzeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOptAnalyzeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOptFullTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                        TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOptFullTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOptFreezeTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                          TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOptFreezeTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeOptVerboseTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                           TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeOptVerboseTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	static void InitializeNameListTrampoline(PEGTransformer &transformer, TransformStack &stack,
	                                         TransformStackFrame &frame);
	static unique_ptr<TransformResultValue>
	FinalizeNameListTrampoline(PEGTransformer &transformer, TransformStack &stack, TransformStackFrame &frame);
	//===--------------------------------------------------------------------===//
	// END GENERATED TRAMPOLINE RULES
	//===--------------------------------------------------------------------===//

	// Registration methods
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
	static const TransformFrameOps &GetTrampolineOps(const string &rule_name);

	// common.gram
	static unique_ptr<ParsedExpression> TransformNumberLiteral(PEGTransformer &transformer, ParseResult &parse_result);
	static string TransformStringLiteral(PEGTransformer &transformer, ParseResult &parse_result);
	static DatePartSpecifier TransformIntervalToIntervalAsType(PEGTransformer &transformer, ParseResult &parse_result);

	static string ExtractFormat(const string &file_path);

	// create_table.gram
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
	static unique_ptr<TransformResultValue> TransformRenameAlterSequenceOptionsInternal(PEGTransformer &transformer,
	                                                                                    ParseResult &parse_result);
	static unique_ptr<AlterInfo> TransformRenameAlterSequenceOptions(PEGTransformer &transformer,
	                                                                 unique_ptr<AlterTableInfo> rename_alter);
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
	                                                          const Identifier &analyze_keyword,
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
	static unique_ptr<TransformResultValue> TransformCommentValueInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformStringLiteralValueInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static Value TransformStringLiteralValue(PEGTransformer &transformer, const string &string_literal);
	static unique_ptr<TransformResultValue> TransformAnalyzeKeywordInternal(PEGTransformer &transformer,
	                                                                        ParseResult &parse_result);
	static Identifier TransformAnalyzeKeyword(PEGTransformer &transformer);
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
	static unique_ptr<ConnectInfo>
	TransformStringSessionTarget(PEGTransformer &transformer, const string &string_literal,
	                             const optional<vector<GenericCopyOption>> &generic_copy_option_list);
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
	static Identifier TransformReservedSchemaQualification(PEGTransformer &transformer,
	                                                       const Identifier &reserved_schema_name);
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
	static unique_ptr<TransformResultValue> TransformTopCheckConstraintInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<Constraint> TransformTopCheckConstraint(PEGTransformer &transformer,
	                                                          ColumnConstraintEntry check_constraint);
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
	static string TransformDotColLabel(PEGTransformer &transformer, const string &col_label);
	static unique_ptr<TransformResultValue> TransformColIdInternal(PEGTransformer &transformer,
	                                                               ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColIdOrStringInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformTypeFuncNameInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColLabelInternal(PEGTransformer &transformer,
	                                                                  ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColLabelOrStringInternal(PEGTransformer &transformer,
	                                                                          ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformColLabelIdentifierInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static Identifier TransformColLabelIdentifier(PEGTransformer &transformer, const string &col_label);
	static unique_ptr<TransformResultValue> TransformStringLiteralIdentifierInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static Identifier TransformStringLiteralIdentifier(PEGTransformer &transformer, const string &string_literal);
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
	static ShowType TransformSummarize(PEGTransformer &transformer, const ShowType &summarize_rule);
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
	TransformExplainStatement(PEGTransformer &transformer, const optional<Identifier> &analyze_keyword,
	                          const optional<vector<GenericCopyOption>> &explain_option_list,
	                          unique_ptr<SQLStatement> explainable_statements);
	static unique_ptr<TransformResultValue> TransformExplainOptionListInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static vector<GenericCopyOption> TransformExplainOptionList(PEGTransformer &transformer,
	                                                            const vector<GenericCopyOption> &explain_option);
	static unique_ptr<TransformResultValue> TransformExplainOptionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static GenericCopyOption TransformExplainOption(PEGTransformer &transformer, const Identifier &explain_option_name,
	                                                optional<unique_ptr<ParsedExpression>> expression);
	static unique_ptr<TransformResultValue> TransformExplainOptionNameInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static Identifier TransformExplainOptionName(PEGTransformer &transformer, ParseResult &choice_result);
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
	static MethodArguments TransformFunctionExpressionArguments(PEGTransformer &transformer,
	                                                            MethodArguments function_expression_argument_list);
	static unique_ptr<TransformResultValue> TransformFunctionExpressionArgumentListInternal(PEGTransformer &transformer,
	                                                                                        ParseResult &parse_result);
	static MethodArguments
	TransformFunctionExpressionArgumentList(PEGTransformer &transformer, const optional<bool> &distinct_or_all,
	                                        optional<vector<FunctionArgument>> function_argument_list,
	                                        optional<vector<OrderByNode>> order_by_clause,
	                                        const optional<bool> &ignore_or_respect_nulls);
	static unique_ptr<TransformResultValue> TransformFunctionArgumentListInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static vector<FunctionArgument> TransformFunctionArgumentList(PEGTransformer &transformer,
	                                                              vector<FunctionArgument> function_argument);
	static unique_ptr<TransformResultValue> TransformFunctionIdentifierInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformFunctionNameAsQualifiedNameInternal(PEGTransformer &transformer,
	                                                                                     ParseResult &parse_result);
	static QualifiedName TransformFunctionNameAsQualifiedName(PEGTransformer &transformer,
	                                                          const Identifier &function_name);
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
	static vector<string> TransformStarQualifierList(PEGTransformer &transformer, const vector<string> &col_id_dot);
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
	static unique_ptr<TransformResultValue> TransformIdentifierWindowFrameInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<WindowExpression> TransformIdentifierWindowFrame(PEGTransformer &transformer,
	                                                                   const Identifier &identifier);
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
	static unique_ptr<TransformResultValue> TransformExpressionInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformExpression(PEGTransformer &transformer,
	                                                        unique_ptr<ParsedExpression> lambda_arrow_expression);
	static unique_ptr<TransformResultValue> TransformColumnDefaultExprInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformColumnDefaultExpr(PEGTransformer &transformer,
	                                                               unique_ptr<ParsedExpression> col_def_or_expr);
	static unique_ptr<TransformResultValue> TransformLambdaArrowExpressionInternal(PEGTransformer &transformer,
	                                                                               ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformLambdaArrowExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> logical_or_expression,
	                               optional<vector<unique_ptr<ParsedExpression>>> single_arrow_pair);
	static unique_ptr<TransformResultValue> TransformSingleArrowPairInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformSingleArrowPair(PEGTransformer &transformer,
	                                                             unique_ptr<ParsedExpression> logical_or_expression);
	static unique_ptr<TransformResultValue> TransformLogicalOrExpressionInternal(PEGTransformer &transformer,
	                                                                             ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformLogicalOrExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> logical_and_expression,
	                             optional<vector<unique_ptr<ParsedExpression>>> logical_or_expression_tail);
	static unique_ptr<TransformResultValue> TransformLogicalOrExpressionTailInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformLogicalOrExpressionTail(PEGTransformer &transformer, unique_ptr<ParsedExpression> logical_and_expression);
	static unique_ptr<TransformResultValue> TransformColDefOrExprInternal(PEGTransformer &transformer,
	                                                                      ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformColDefOrExpr(PEGTransformer &transformer, unique_ptr<ParsedExpression> col_def_and_expr,
	                      optional<vector<unique_ptr<ParsedExpression>>> col_def_or_expression_tail);
	static unique_ptr<TransformResultValue> TransformColDefOrExpressionTailInternal(PEGTransformer &transformer,
	                                                                                ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformColDefOrExpressionTail(PEGTransformer &transformer,
	                                                                    unique_ptr<ParsedExpression> col_def_and_expr);
	static unique_ptr<TransformResultValue> TransformLogicalAndExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformLogicalAndExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> logical_not_expression,
	                              optional<vector<unique_ptr<ParsedExpression>>> logical_and_expression_tail);
	static unique_ptr<TransformResultValue> TransformLogicalAndExpressionTailInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformLogicalAndExpressionTail(PEGTransformer &transformer, unique_ptr<ParsedExpression> logical_not_expression);
	static unique_ptr<TransformResultValue> TransformColDefAndExprInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformColDefAndExpr(PEGTransformer &transformer, unique_ptr<ParsedExpression> is_distinct_from_expression,
	                       optional<vector<unique_ptr<ParsedExpression>>> col_def_and_expression_tail);
	static unique_ptr<TransformResultValue> TransformColDefAndExpressionTailInternal(PEGTransformer &transformer,
	                                                                                 ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformColDefAndExpressionTail(PEGTransformer &transformer,
	                                 unique_ptr<ParsedExpression> is_distinct_from_expression);
	static unique_ptr<TransformResultValue> TransformLogicalNotExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression> TransformLogicalNotExpression(PEGTransformer &transformer,
	                                                                  optional<vector<bool>> not_expression,
	                                                                  unique_ptr<ParsedExpression> is_expression);
	static unique_ptr<TransformResultValue> TransformNotExpressionInternal(PEGTransformer &transformer,
	                                                                       ParseResult &parse_result);
	static vector<bool> TransformNotExpression(PEGTransformer &transformer, const vector<bool> &not_keyword);
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
	static unique_ptr<ParsedExpression> TransformIsNull(PEGTransformer &transformer,
	                                                    unique_ptr<ParsedExpression> is_null_operator);
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
	static unique_ptr<TransformResultValue> TransformAnyAllParsedOperatorInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static ParsedOperator TransformAnyAllParsedOperator(PEGTransformer &transformer,
	                                                    const pair<string, bool> &any_all_operator);
	static unique_ptr<TransformResultValue> TransformNamedOtherOperatorInternal(PEGTransformer &transformer,
	                                                                            ParseResult &parse_result);
	static ParsedOperator TransformNamedOtherOperator(PEGTransformer &transformer, const string &child);
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
	static unique_ptr<ParsedExpression>
	TransformCollateExpressionTail(PEGTransformer &transformer, unique_ptr<ParsedExpression> at_time_zone_expression);
	static unique_ptr<TransformResultValue> TransformAtTimeZoneExpressionInternal(PEGTransformer &transformer,
	                                                                              ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformAtTimeZoneExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> prefix_expression,
	                              optional<vector<unique_ptr<ParsedExpression>>> at_time_zone_expression_tail);
	static unique_ptr<TransformResultValue> TransformAtTimeZoneExpressionTailInternal(PEGTransformer &transformer,
	                                                                                  ParseResult &parse_result);
	static unique_ptr<ParsedExpression>
	TransformAtTimeZoneExpressionTail(PEGTransformer &transformer, unique_ptr<ParsedExpression> prefix_expression);
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
	static vector<unique_ptr<ParsedExpression>>
	TransformIndirectionList(PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> indirection);
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
	static unique_ptr<TransformResultValue> TransformOffsetFetchClauseInternal(PEGTransformer &transformer,
	                                                                           ParseResult &parse_result);
	static unique_ptr<ResultModifier> TransformOffsetFetchClause(PEGTransformer &transformer,
	                                                             LimitPercentResult offset_clause,
	                                                             LimitPercentResult fetch_clause);
	static unique_ptr<TransformResultValue> TransformFetchOnlyClauseInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static unique_ptr<ResultModifier> TransformFetchOnlyClause(PEGTransformer &transformer,
	                                                           LimitPercentResult fetch_clause);
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
	static unique_ptr<TransformResultValue> TransformPivotEnumTargetInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static PivotColumn TransformPivotEnumTarget(PEGTransformer &transformer, const Identifier &identifier);
	static unique_ptr<TransformResultValue> TransformPivotListTargetInternal(PEGTransformer &transformer,
	                                                                         ParseResult &parse_result);
	static PivotColumn TransformPivotListTarget(PEGTransformer &transformer,
	                                            vector<PivotColumnEntry> pivot_target_list);
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
	static unique_ptr<TransformResultValue> TransformFetchClauseInternal(PEGTransformer &transformer,
	                                                                     ParseResult &parse_result);
	static unique_ptr<TransformResultValue> TransformFetchValueInternal(PEGTransformer &transformer,
	                                                                    ParseResult &parse_result);
	static LimitPercentResult TransformFetchValue(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression);
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
	static string TransformOptAnalyze(PEGTransformer &transformer, const Identifier &analyze_keyword);
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
