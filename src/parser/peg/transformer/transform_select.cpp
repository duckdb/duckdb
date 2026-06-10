#include "duckdb/parser/peg/ast/distinct_clause.hpp"
#include "duckdb/parser/peg/ast/join_prefix.hpp"
#include "duckdb/parser/peg/ast/join_qualifier.hpp"
#include "duckdb/parser/peg/ast/limit_percent_result.hpp"
#include "duckdb/parser/peg/ast/table_alias.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/tableref/at_clause.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformSelectStatement(PEGTransformer &transformer,
                                                                         ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(0));
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformSelectStatementInternal(PEGTransformer &transformer,
                                                                                    ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	CommonTableExpressionMap cte_map;
	transformer.TransformOptional<CommonTableExpressionMap>(list_pr, 0, cte_map);
	if (!cte_map.map.empty()) {
		transformer.stored_cte_map.push_back(cte_map);
	}
	auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ListParseResult>(1));

	if (!cte_map.map.empty()) {
		select_statement->node->cte_map = std::move(cte_map);
	}
	vector<unique_ptr<ResultModifier>> result_modifiers;
	transformer.TransformOptional<vector<unique_ptr<ResultModifier>>>(list_pr, 2, result_modifiers);
	for (auto &result_modifier : result_modifiers) {
		select_statement->node->modifiers.push_back(std::move(result_modifier));
	}
	if (select_statement->node->type != QueryNodeType::SELECT_NODE) {
		return select_statement;
	}
	auto &select_node = select_statement->node->Cast<SelectNode>();
	if (select_node.from_table->type != TableReferenceType::SHOW_REF) {
		return select_statement;
	}
	auto &show_ref = select_node.from_table->Cast<ShowRef>();
	if (!select_statement->node->cte_map.map.empty()) {
		throw ParserException("%s with CTE not allowed - wrap the statement in a subquery instead",
		                      EnumUtil::ToString(show_ref.show_type));
	}
	if (!select_statement->node->modifiers.empty()) {
		throw ParserException("%s with ORDER BY not allowed - wrap the statement in a subquery instead",
		                      EnumUtil::ToString(show_ref.show_type));
	}
	return select_statement;
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformSelectSetOpChain(
    PEGTransformer &transformer, unique_ptr<SelectStatement> intersect_chain,
    optional<vector<pair<unique_ptr<SetOperationNode>, unique_ptr<SelectStatement>>>> select_set_op_chain_tail) {
	auto select = std::move(intersect_chain);
	if (!select_set_op_chain_tail) {
		return select;
	}
	for (auto &tail : *select_set_op_chain_tail) {
		auto setop_result = std::move(tail.first);
		auto right_select = std::move(tail.second);
		if (select->node->type == QueryNodeType::SET_OPERATION_NODE && select->node->modifiers.empty() &&
		    select->node->cte_map.map.empty()) {
			auto &existing = select->node->Cast<SetOperationNode>();
			if (existing.setop_type == setop_result->setop_type && existing.setop_all == setop_result->setop_all &&
			    (existing.setop_type == SetOperationType::UNION ||
			     existing.setop_type == SetOperationType::UNION_BY_NAME)) {
				existing.children.push_back(std::move(right_select->node));
				continue;
			}
		}
		setop_result->children.push_back(std::move(select->node));
		setop_result->children.push_back(std::move(right_select->node));
		select->node = std::move(setop_result);
	}
	return select;
}

pair<unique_ptr<SetOperationNode>, unique_ptr<SelectStatement>>
PEGTransformerFactory::TransformSelectSetOpChainTail(PEGTransformer &transformer,
                                                     unique_ptr<SetOperationNode> setop_clause,
                                                     unique_ptr<SelectStatement> intersect_chain) {
	return make_pair(std::move(setop_clause), std::move(intersect_chain));
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformIntersectChain(
    PEGTransformer &transformer, unique_ptr<SelectStatement> select_atom,
    optional<vector<pair<unique_ptr<SetOperationNode>, unique_ptr<SelectStatement>>>> intersect_chain_tail) {
	auto select = std::move(select_atom);
	if (!intersect_chain_tail) {
		return select;
	}
	for (auto &tail : *intersect_chain_tail) {
		auto intersect_node = std::move(tail.first);
		auto right_select = std::move(tail.second);
		intersect_node->children.push_back(std::move(select->node));
		intersect_node->children.push_back(std::move(right_select->node));
		select->node = std::move(intersect_node);
	}
	return select;
}

pair<unique_ptr<SetOperationNode>, unique_ptr<SelectStatement>>
PEGTransformerFactory::TransformIntersectChainTail(PEGTransformer &transformer,
                                                   unique_ptr<SetOperationNode> set_intersect_clause,
                                                   unique_ptr<SelectStatement> select_atom) {
	return make_pair(std::move(set_intersect_clause), std::move(select_atom));
}

unique_ptr<SetOperationNode> PEGTransformerFactory::TransformSetIntersectClause(PEGTransformer &transformer,
                                                                                ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = make_uniq<SetOperationNode>();
	result->setop_type = SetOperationType::INTERSECT;
	auto &is_distinct_opt = list_pr.Child<OptionalParseResult>(1);
	if (is_distinct_opt.HasResult()) {
		result->setop_all = !transformer.Transform<bool>(is_distinct_opt.GetResult());
	}
	return result;
}

unique_ptr<SetOperationNode> PEGTransformerFactory::TransformSetopClause(PEGTransformer &transformer,
                                                                         ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = make_uniq<SetOperationNode>();
	result->setop_type = transformer.Transform<SetOperationType>(list_pr.Child<ListParseResult>(0));
	auto &is_distinct_opt = list_pr.Child<OptionalParseResult>(1);
	if (is_distinct_opt.HasResult()) {
		result->setop_all = !transformer.Transform<bool>(is_distinct_opt.GetResult());
	}
	auto &by_name = list_pr.Child<OptionalParseResult>(2);
	if (by_name.HasResult()) {
		if (result->setop_type == SetOperationType::UNION) {
			result->setop_type = SetOperationType::UNION_BY_NAME;
		} else {
			throw ParserException("Invalid combination of %s and BY NAME", EnumUtil::ToString(result->setop_type));
		}
	}
	return result;
}

SetOperationType PEGTransformerFactory::TransformSetopUnion(PEGTransformer &transformer) {
	return SetOperationType::UNION;
}

SetOperationType PEGTransformerFactory::TransformSetopExcept(PEGTransformer &transformer) {
	return SetOperationType::EXCEPT;
}

bool PEGTransformerFactory::TransformDistinctOrAll(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return transformer.Transform<bool>(choice_pr.GetResult());
}

bool PEGTransformerFactory::TransformDistinctKeyword(PEGTransformer &transformer, ParseResult &parse_result) {
	return true;
}

bool PEGTransformerFactory::TransformAllKeyword(PEGTransformer &transformer, ParseResult &parse_result) {
	return false;
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformSimpleSelect(PEGTransformer &transformer,
                                                                         ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &opt_window_clause = list_pr.Child<OptionalParseResult>(4);
	if (opt_window_clause.HasResult()) {
		auto window_functions =
		    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(opt_window_clause.GetResult());
		for (auto &window_func : window_functions) {
			D_ASSERT(!window_func->GetAlias().empty());
			string window_name(window_func->GetAlias());
			window_func->ClearAlias();
			auto it = transformer.window_clauses.find(window_name);
			if (it != transformer.window_clauses.end()) {
				throw ParserException("window \"%s\" is already defined", window_name);
			}
			auto window_function = unique_ptr_cast<ParsedExpression, WindowExpression>(std::move(window_func));
			transformer.window_clauses[window_name] = std::move(window_function);
		}
	}
	auto select_node = transformer.Transform<unique_ptr<SelectNode>>(list_pr.Child<ListParseResult>(0));
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 1, select_node->where_clause);
	auto &group_opt = list_pr.Child<OptionalParseResult>(2);
	if (group_opt.HasResult()) {
		auto group_by_node = transformer.Transform<GroupByNode>(group_opt.GetResult());
		if (group_by_node.group_expressions.size() == 1 && ExpressionIsEmptyStar(*group_by_node.group_expressions[0])) {
			select_node->aggregate_handling = AggregateHandling::FORCE_AGGREGATES;
			group_by_node.group_expressions.clear();
			group_by_node.grouping_sets.clear();
		}
		select_node->groups = std::move(group_by_node);
	}
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 3, select_node->having);
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 5, select_node->qualify);
	transformer.TransformOptional<unique_ptr<SampleOptions>>(list_pr, 6, select_node->sample);
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(select_node);
	transformer.window_clauses.clear();
	return select_statement;
}

FunctionArgument PEGTransformerFactory::TransformFunctionArgument(PEGTransformer &transformer,
                                                                  ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	if (choice_pr.name == "NamedParameter") {
		auto parameter = transformer.Transform<MacroParameter>(choice_pr);
		parameter.expression->SetAlias(parameter.name);
		return FunctionArgument(parameter.name, std::move(parameter.expression));
	}

	return FunctionArgument(transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr));
}

MacroParameter PEGTransformerFactory::TransformNamedParameter(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	MacroParameter parameter;
	parameter.expression = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(3));
	parameter.name = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	parameter.is_default = true;
	transformer.TransformOptional<LogicalType>(list_pr, 1, parameter.type);
	return parameter;
}

unique_ptr<BaseTableRef> PEGTransformerFactory::TransformBaseTableName(PEGTransformer &transformer,
                                                                       ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.GetResult().type == ParseResultType::IDENTIFIER) {
		auto table_name = choice_pr.GetResult().Cast<IdentifierParseResult>().identifier;
		const auto description = TableDescription(INVALID_CATALOG, INVALID_SCHEMA, table_name);
		return make_uniq<BaseTableRef>(description);
	}
	return transformer.Transform<unique_ptr<BaseTableRef>>(choice_pr.GetResult());
}

string PEGTransformerFactory::TransformSchemaQualification(PEGTransformer &transformer, const string &schema_name) {
	return schema_name;
}

string PEGTransformerFactory::TransformCatalogQualification(PEGTransformer &transformer, const string &catalog_name) {
	return catalog_name;
}

QualifiedName PEGTransformerFactory::TransformCatalogReservedSchemaIdentifier(
    PEGTransformer &transformer, const string &catalog_qualification, const string &reserved_schema_qualification,
    const string &reserved_identifier_or_string_literal) {
	QualifiedName result;
	result.catalog = catalog_qualification;
	result.schema = reserved_schema_qualification;
	result.name = reserved_identifier_or_string_literal;
	return result;
}

QualifiedName PEGTransformerFactory::TransformSchemaReservedIdentifierOrStringLiteral(
    PEGTransformer &transformer, const string &schema_qualification,
    const string &reserved_identifier_or_string_literal) {
	QualifiedName result;
	result.catalog = INVALID_CATALOG;
	result.schema = schema_qualification;
	result.name = reserved_identifier_or_string_literal;
	return result;
}

static bool IsConditionlessJoin(const JoinRef &join) {
	if (join.condition || !join.using_columns.empty()) {
		return false;
	}
	if (join.ref_type != JoinRefType::CROSS && join.ref_type != JoinRefType::POSITIONAL &&
	    join.ref_type != JoinRefType::NATURAL) {
		return false;
	}
	return true;
}

static unique_ptr<TableRef> ReassociateJoins(unique_ptr<TableRef> root) {
	// Left-rotate while the current node is a conditionless join and its right child is a join.
	// This converts right-associative join trees (from PEG grammar) to left-associative.
	while (root->type == TableReferenceType::JOIN) {
		auto &current = root->Cast<JoinRef>();
		if (!IsConditionlessJoin(current) || !current.right || current.right->type != TableReferenceType::JOIN) {
			break;
		}
		// Left rotation:
		//   current(left=A, right=inner(left=B, right=C))
		//   => inner(left=current(left=A, right=B), right=C)
		auto inner = std::move(current.right);
		auto &inner_join = inner->Cast<JoinRef>();
		current.right = std::move(inner_join.left);
		inner_join.left = std::move(root);
		root = std::move(inner);
	}
	return root;
}

//! Check whether the RHS TableRef of a JoinOrPivot parse result has its own JoinOrPivot* entries.
//! This distinguishes PEG right-recursion (has entries) from parenthesized joins (no entries).
//! Navigation: JoinOrPivot → Choice → JoinClause → Choice → JoinWithoutOnClause → child(2)=TableRef → child(1)=Optional
static bool RHSTableRefHasJoinOrPivot(ParseResult &join_or_pivot_pr) {
	auto &jop_list = join_or_pivot_pr.Cast<ListParseResult>();
	auto &jop_choice = jop_list.Child<ChoiceParseResult>(0);
	auto &join_clause = jop_choice.GetResult().Cast<ListParseResult>();
	auto &jc_choice = join_clause.Child<ChoiceParseResult>(0);
	auto &join_impl = jc_choice.GetResult().Cast<ListParseResult>();
	// For JoinWithoutOnClause the TableRef is at index 2
	auto &table_ref = join_impl.Child<ListParseResult>(2);
	return table_ref.Child<OptionalParseResult>(1).HasResult();
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTableRef(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto inner_table_ref = transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(0));
	auto &join_or_pivot_opt = list_pr.Child<OptionalParseResult>(1);
	if (!join_or_pivot_opt.HasResult()) {
		return inner_table_ref;
	}
	auto &repeat_join_or_pivot = join_or_pivot_opt.GetResult().Cast<RepeatParseResult>();
	for (auto join_or_pivot : repeat_join_or_pivot.GetChildren()) {
		auto transform_join_or_pivot = transformer.Transform<unique_ptr<TableRef>>(join_or_pivot);
		if (transform_join_or_pivot->type == TableReferenceType::JOIN) {
			auto &join_ref = transform_join_or_pivot->Cast<JoinRef>();
			join_ref.left = std::move(inner_table_ref);
			if (IsConditionlessJoin(join_ref) && RHSTableRefHasJoinOrPivot(join_or_pivot)) {
				inner_table_ref = ReassociateJoins(std::move(transform_join_or_pivot));
			} else {
				inner_table_ref = std::move(transform_join_or_pivot);
			}
		} else if (transform_join_or_pivot->type == TableReferenceType::PIVOT) {
			auto &pivot_ref = transform_join_or_pivot->Cast<PivotRef>();
			pivot_ref.source = std::move(inner_table_ref);
			inner_table_ref = std::move(transform_join_or_pivot);
		} else {
			throw NotImplementedException("Unsupported TableRef type encountered: %s",
			                              EnumUtil::ToString(transform_join_or_pivot->type));
		}
	}
	return inner_table_ref;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTableUnpivotClauseBody(PEGTransformer &transformer,
                                                                            const vector<string> &unpivot_header,
                                                                            vector<PivotColumn> unpivot_value_list) {
	auto result = make_uniq<PivotRef>();
	result->unpivot_names = unpivot_header;
	result->pivots = std::move(unpivot_value_list);
	if (result->pivots.size() > 1) {
		throw ParserException("UNPIVOT requires a single pivot element");
	}
	return std::move(result);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTableUnpivotClause(PEGTransformer &transformer,
                                                                        const optional<bool> &include_or_exclude_nulls,
                                                                        unique_ptr<TableRef> table_unpivot_clause_body,
                                                                        const optional<TableAlias> &table_alias) {
	auto &result = table_unpivot_clause_body->Cast<PivotRef>();
	result.include_nulls = include_or_exclude_nulls.value_or(false);
	if (table_alias) {
		result.alias = table_alias->name;
		result.column_name_alias = table_alias->column_name_alias;
	}
	return table_unpivot_clause_body;
}

void PEGTransformerFactory::GetValueFromExpression(unique_ptr<ParsedExpression> &expr, vector<Value> &result) {
	if (expr->GetExpressionClass() == ExpressionClass::CONSTANT) {
		auto &const_expr = expr->Cast<ConstantExpression>();
		result.push_back(const_expr.GetValue());
	} else if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref_expr = expr->Cast<ColumnRefExpression>();
		for (auto &col : col_ref_expr.ColumnNames()) {
			result.push_back(Value(col));
		}
	} else if (expr->GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &func_expr = expr->Cast<FunctionExpression>();
		if (func_expr.FunctionName() == "row") {
			for (auto &col : func_expr.GetArgumentsMutable()) {
				GetValueFromExpression(col.GetExpressionMutable(), result);
			}
		}
	}
}

bool PEGTransformerFactory::TransformPivotInList(unique_ptr<ParsedExpression> &expr, PivotColumnEntry &entry) {
	auto initial_size = entry.values.size();
	switch (expr->GetExpressionType()) {
	case ExpressionType::COLUMN_REF: {
		auto &colref = expr->Cast<ColumnRefExpression>();
		if (colref.IsQualified()) {
			throw ParserException(expr->GetQueryLocation(), "PIVOT IN list cannot contain qualified column references");
		}
		entry.values.emplace_back(colref.GetColumnName());
		return true;
	}
	case ExpressionType::FUNCTION: {
		auto &function = expr->Cast<FunctionExpression>();
		if (function.FunctionName() != "row") {
			return false;
		}
		for (auto &child : function.GetArgumentsMutable()) {
			if (!TransformPivotInList(child.GetExpressionMutable(), entry)) {
				entry.values.resize(initial_size);
				return false;
			}
		}
		return true;
	}
	default: {
		Value val;
		if (!ConstructConstantFromExpression(*expr, val)) {
			return false;
		}
		entry.values.push_back(std::move(val));
		return true;
	}
	}
}

static bool PivotEntryIsTuple(const PivotColumnEntry &entry) {
	if (entry.values.size() > 1) {
		return true;
	}
	if (!entry.expr || entry.expr->GetExpressionType() != ExpressionType::FUNCTION) {
		return false;
	}
	auto &function = entry.expr->Cast<FunctionExpression>();
	return function.FunctionName() == "row";
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTablePivotClauseBody(
    PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> target_list, vector<PivotColumn> pivot_value_list,
    const optional<vector<string>> &pivot_group_by_list) {
	auto result = make_uniq<PivotRef>();
	result->aggregates = std::move(target_list);
	result->pivots = std::move(pivot_value_list);
	if (pivot_group_by_list) {
		result->groups = *pivot_group_by_list;
	}
	return std::move(result);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTablePivotClause(PEGTransformer &transformer,
                                                                      unique_ptr<TableRef> table_pivot_clause_body,
                                                                      const optional<TableAlias> &table_alias) {
	auto &result = table_pivot_clause_body->Cast<PivotRef>();
	if (table_alias) {
		result.alias = table_alias->name;
		result.column_name_alias = table_alias->column_name_alias;
	}
	return table_pivot_clause_body;
}

PivotColumn PEGTransformerFactory::TransformPivotValueTarget(PEGTransformer &transformer, ParseResult &choice_result) {
	PivotColumn result;
	if (choice_result.type == ParseResultType::IDENTIFIER) {
		result.pivot_enum = choice_result.Cast<IdentifierParseResult>().identifier;
	} else {
		result.entries = transformer.Transform<vector<PivotColumnEntry>>(choice_result);
	}
	return result;
}

PivotColumn PEGTransformerFactory::TransformPivotValueList(PEGTransformer &transformer,
                                                           unique_ptr<ParsedExpression> pivot_header,
                                                           PivotColumn pivot_value_target) {
	auto result = std::move(pivot_value_target);
	auto pivot_expression = std::move(pivot_header);
	if (pivot_expression->GetExpressionClass() != ExpressionClass::FUNCTION) {
		result.pivot_expressions.push_back(std::move(pivot_expression));
		return result;
	}
	auto &func_expr = pivot_expression->Cast<FunctionExpression>();
	if (func_expr.FunctionName() != "row") {
		result.pivot_expressions.push_back(std::move(pivot_expression));
		return result;
	}
	// Unpack row() only when IN list entries are tuples (multi-value).
	// For scalar IN entries like IN ('xx'), keep row() as a single compound expression
	// so pivot_expressions.size() matches entry.values.size() (both 1).
	bool has_tuple_entries = false;
	for (auto &entry : result.entries) {
		if (PivotEntryIsTuple(entry)) {
			has_tuple_entries = true;
			break;
		}
	}
	if (has_tuple_entries) {
		for (auto &child : func_expr.GetArgumentsMutable()) {
			result.pivot_expressions.emplace_back(std::move(child.GetExpressionMutable()));
		}
	} else {
		result.pivot_expressions.push_back(std::move(pivot_expression));
	}
	return result;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformRegularJoinClause(PEGTransformer &transformer,
                                                                       ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = make_uniq<JoinRef>();
	auto asof = list_pr.Child<OptionalParseResult>(0).HasResult();
	if (asof) {
		result->ref_type = JoinRefType::ASOF;
	}
	auto join_type = JoinType::INNER;
	transformer.TransformOptional<JoinType>(list_pr, 1, join_type);
	result->type = join_type;
	result->right = transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(3));
	auto join_qualifier = transformer.Transform<JoinQualifier>(list_pr.Child<ListParseResult>(4));
	if (join_qualifier.on_clause) {
		result->condition = std::move(join_qualifier.on_clause);
	} else if (!join_qualifier.using_columns.empty()) {
		result->using_columns = std::move(join_qualifier.using_columns);
	} else {
		throw InternalException("Invalid join qualifier found.");
	}
	return std::move(result);
}

JoinType PEGTransformerFactory::TransformFullJoin(PEGTransformer &transformer, const bool &has_result) {
	return JoinType::OUTER;
}

JoinType PEGTransformerFactory::TransformLeftJoin(PEGTransformer &transformer, const bool &has_result) {
	return JoinType::LEFT;
}

JoinType PEGTransformerFactory::TransformRightJoin(PEGTransformer &transformer, const bool &has_result) {
	return JoinType::RIGHT;
}

JoinType PEGTransformerFactory::TransformSemiJoin(PEGTransformer &transformer) {
	return JoinType::SEMI;
}

JoinType PEGTransformerFactory::TransformAntiJoin(PEGTransformer &transformer) {
	return JoinType::ANTI;
}

JoinType PEGTransformerFactory::TransformInnerJoin(PEGTransformer &transformer) {
	return JoinType::INNER;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTableFunctionLateralOpt(PEGTransformer &transformer,
                                                                             ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();

	auto result = make_uniq<TableFunctionRef>();

	auto qualified_table_function = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(1));
	auto table_function_arguments = transformer.Transform<vector<FunctionArgument>>(list_pr.Child<ListParseResult>(2));
	result->with_ordinality = list_pr.Child<OptionalParseResult>(3).HasResult() ? OrdinalityType::WITH_ORDINALITY
	                                                                            : OrdinalityType::WITHOUT_ORDINALITY;
	result->function =
	    make_uniq<FunctionExpression>(qualified_table_function.catalog, qualified_table_function.schema,
	                                  qualified_table_function.name, std::move(table_function_arguments));
	auto &table_alias_opt = list_pr.Child<OptionalParseResult>(4);
	if (table_alias_opt.HasResult()) {
		auto table_alias = transformer.Transform<TableAlias>(table_alias_opt.GetResult());
		result->alias = table_alias.name;
		result->column_name_alias = table_alias.column_name_alias;
	}
	return std::move(result);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTableFunctionAliasColon(
    PEGTransformer &transformer, const string &table_alias_colon, const QualifiedName &qualified_table_function,
    vector<FunctionArgument> table_function_arguments, const bool &has_result,
    optional<unique_ptr<SampleOptions>> sample_clause) {
	auto result = make_uniq<TableFunctionRef>();
	result->with_ordinality = has_result ? OrdinalityType::WITH_ORDINALITY : OrdinalityType::WITHOUT_ORDINALITY;
	result->function =
	    make_uniq<FunctionExpression>(qualified_table_function.catalog, qualified_table_function.schema,
	                                  qualified_table_function.name, std::move(table_function_arguments));
	result->alias = table_alias_colon;
	if (sample_clause) {
		result->sample = std::move(*sample_clause);
	}
	return std::move(result);
}

string PEGTransformerFactory::TransformVersionAtUnit(PEGTransformer &transformer) {
	return "VERSION";
}

string PEGTransformerFactory::TransformTimestampAtUnit(PEGTransformer &transformer) {
	return "TIMESTAMP";
}

OrderType PEGTransformerFactory::TransformDescendingOrder(PEGTransformer &transformer) {
	return OrderType::DESCENDING;
}

OrderType PEGTransformerFactory::TransformAscendingOrder(PEGTransformer &transformer) {
	return OrderType::ASCENDING;
}

OrderByNullType PEGTransformerFactory::TransformNullsFirst(PEGTransformer &transformer) {
	return OrderByNullType::NULLS_FIRST;
}

OrderByNullType PEGTransformerFactory::TransformNullsLast(PEGTransformer &transformer) {
	return OrderByNullType::NULLS_LAST;
}

unique_ptr<ResultModifier> PEGTransformerFactory::VerifyLimitOffset(LimitPercentResult &limit,
                                                                    LimitPercentResult &offset) {
	if (offset.is_percent) {
		throw ParserException("Percentage for offsets are not supported.");
	}
	auto result = make_uniq<LimitModifier>();
	result->limit_type = limit.is_percent ? LimitValueType::PERCENTAGE : LimitValueType::ROW_COUNT;
	if (limit.expression) {
		result->limit = std::move(limit.expression);
	}
	if (offset.expression) {
		result->offset = std::move(offset.expression);
	}
	if (!result->limit && !result->offset) {
		return nullptr;
	}
	return std::move(result);
}

LimitPercentResult PEGTransformerFactory::TransformLimitExpression(PEGTransformer &transformer,
                                                                   unique_ptr<ParsedExpression> expression,
                                                                   const bool &has_result) {
	LimitPercentResult result;
	result.expression = std::move(expression);
	result.is_percent = has_result;
	return result;
}

struct GroupingExpressionMap {
	parsed_expression_map_t<ProjectionIndex> map;
};

static void CheckGroupingSetMax(idx_t count) {
	static constexpr const idx_t MAX_GROUPING_SETS = 65535;
	if (count > MAX_GROUPING_SETS) {
		throw ParserException("Maximum grouping set count of %d exceeded", MAX_GROUPING_SETS);
	}
}

static void CheckGroupingSetCubes(idx_t current_count, idx_t cube_count) {
	idx_t combinations = 1;
	for (idx_t i = 0; i < cube_count; i++) {
		combinations *= 2;
		CheckGroupingSetMax(current_count + combinations);
	}
}

static GroupingSet VectorToGroupingSet(vector<ProjectionIndex> &indexes) {
	GroupingSet result;
	for (idx_t i = 0; i < indexes.size(); i++) {
		result.insert(indexes[i]);
	}
	return result;
}

void PEGTransformerFactory::AddGroupByExpression(unique_ptr<ParsedExpression> expression, GroupingExpressionMap &map,
                                                 GroupByNode &result, vector<ProjectionIndex> &result_set) {
	if (expression->GetExpressionType() == ExpressionType::FUNCTION) {
		auto &func = expression->Cast<FunctionExpression>();
		if (func.FunctionName() == "row") {
			for (auto &child : func.GetArgumentsMutable()) {
				AddGroupByExpression(std::move(child.GetExpressionMutable()), map, result, result_set);
			}
			return;
		}
	}
	auto entry = map.map.find(*expression);
	ProjectionIndex result_idx;
	if (entry == map.map.end()) {
		result_idx = ProjectionIndex(result.group_expressions.size());
		map.map[*expression] = result_idx;
		result.group_expressions.push_back(std::move(expression));
	} else {
		result_idx = entry->second;
	}
	result_set.push_back(result_idx);
}

static void AddCubeSets(const GroupingSet &current_set, vector<GroupingSet> &cube_sets,
                        vector<GroupingSet> &result_sets, idx_t start_idx = 0) {
	CheckGroupingSetMax(result_sets.size());
	result_sets.push_back(current_set);
	for (idx_t k = start_idx; k < cube_sets.size(); k++) {
		auto child_set = current_set;
		child_set.insert(cube_sets[k].begin(), cube_sets[k].end());
		AddCubeSets(child_set, cube_sets, result_sets, k + 1);
	}
}

vector<GroupingSet> PEGTransformerFactory::GroupByExpressionUnfolding(PEGTransformer &transformer,
                                                                      ParseResult &group_by_expr,
                                                                      GroupingExpressionMap &map, GroupByNode &result) {
	vector<GroupingSet> result_sets;
	if (StringUtil::CIEquals(group_by_expr.name, "EmptyGroupingItem")) {
		result_sets.emplace_back();

	} else if (StringUtil::CIEquals(group_by_expr.name, "Expression")) {
		vector<ProjectionIndex> indexes;
		auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(group_by_expr);
		AddGroupByExpression(std::move(expr), map, result, indexes);
		result_sets.push_back(VectorToGroupingSet(indexes));
	} else if (StringUtil::CIEquals(group_by_expr.name, "GroupingSetsClause")) {
		auto &grouping_set_list = group_by_expr.Cast<ListParseResult>();
		auto &inner_group_by_list = ExtractResultFromParens(grouping_set_list.Child<ListParseResult>(2));
		auto &list_pr = inner_group_by_list.Cast<ListParseResult>();
		auto group_by_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
		for (auto &child_wrapper : group_by_list) {
			auto &child_list_pr = child_wrapper.get().Cast<ListParseResult>();
			auto &child_expr = child_list_pr.Child<ChoiceParseResult>(0).GetResult();
			auto child_sets = GroupByExpressionUnfolding(transformer, child_expr, map, result);
			result_sets.insert(result_sets.end(), child_sets.begin(), child_sets.end());
		}
	} else if (StringUtil::CIEquals(group_by_expr.name, "CubeOrRollupClause")) {
		auto &group_by_list = group_by_expr.Cast<ListParseResult>();
		auto type_str = transformer.Transform<string>(group_by_list.Child<ListParseResult>(0));
		auto &extract_parens = ExtractResultFromParens(group_by_list.Child<ListParseResult>(1));
		if (!extract_parens.Cast<OptionalParseResult>().HasResult()) {
			throw ParserException("CUBE or ROLLUP column list cannot be empty");
		}
		auto expr_list = ExtractParseResultsFromList(extract_parens.Cast<OptionalParseResult>().GetResult());

		vector<GroupingSet> unfolding_sets;
		for (auto &expr_node : expr_list) {
			vector<ProjectionIndex> indexes;
			auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(expr_node);
			AddGroupByExpression(std::move(expr), map, result, indexes);

			GroupingSet s;
			for (auto idx : indexes) {
				s.insert(idx);
			}
			unfolding_sets.push_back(std::move(s));
		}

		if (StringUtil::CIEquals(type_str, "CUBE")) {
			CheckGroupingSetCubes(result_sets.size(), unfolding_sets.size());
			GroupingSet current_set;
			AddCubeSets(current_set, unfolding_sets, result_sets, 0);
		} else if (StringUtil::CIEquals(type_str, "ROLLUP")) {
			GroupingSet current_set;
			result_sets.push_back(current_set);
			for (idx_t i = 0; i < unfolding_sets.size(); i++) {
				current_set.insert(unfolding_sets[i].begin(), unfolding_sets[i].end());
				result_sets.push_back(current_set);
			}
		}
	}
	return result_sets;
}

string PEGTransformerFactory::TransformCubeKeyword(PEGTransformer &transformer) {
	return "CUBE";
}

string PEGTransformerFactory::TransformRollupKeyword(PEGTransformer &transformer) {
	return "ROLLUP";
}

GroupByNode PEGTransformerFactory::TransformGroupByList(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto group_by_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));

	GroupByNode result;
	GroupingExpressionMap map;

	for (auto group_by_child : group_by_list) {
		auto &group_by_expr_child_list = group_by_child.get().Cast<ListParseResult>();
		auto &group_by_expr = group_by_expr_child_list.Child<ChoiceParseResult>(0).GetResult();

		vector<GroupingSet> next_sets = GroupByExpressionUnfolding(transformer, group_by_expr, map, result);

		if (result.grouping_sets.empty()) {
			result.grouping_sets = std::move(next_sets);
		} else {
			vector<GroupingSet> new_sets;
			idx_t grouping_set_count = result.grouping_sets.size() * next_sets.size();
			CheckGroupingSetMax(grouping_set_count);
			new_sets.reserve(grouping_set_count);

			for (auto &current_set : result.grouping_sets) {
				for (auto &next_set : next_sets) {
					GroupingSet combined_set;
					combined_set.insert(current_set.begin(), current_set.end());
					combined_set.insert(next_set.begin(), next_set.end());
					new_sets.push_back(std::move(combined_set));
				}
			}
			result.grouping_sets = std::move(new_sets);
		}
	}
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformGroupByExpression(PEGTransformer &transformer,
                                                                               ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformEmptyGroupingItem(PEGTransformer &transformer,
                                                                               ParseResult &parse_result) {
	throw NotImplementedException("Rule 'EmptyGroupingItem' has not been implemented yet");
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCubeOrRollupClause(PEGTransformer &transformer,
                                                                                ParseResult &parse_result) {
	throw NotImplementedException("Rule 'CubeOrRollupClause' has not been implemented yet");
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformGroupingSetsClause(PEGTransformer &transformer,
                                                                                ParseResult &parse_result) {
	throw NotImplementedException("Rule 'GroupingSetsClause' has not been implemented yet");
}

CommonTableExpressionMap PEGTransformerFactory::TransformWithClause(PEGTransformer &transformer,
                                                                    ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	bool is_recursive = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto with_statement_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	CommonTableExpressionMap result;

	for (idx_t entry_idx = 0; entry_idx < with_statement_list.size(); entry_idx++) {
		auto with_entry =
		    transformer.Transform<pair<string, unique_ptr<CommonTableExpressionInfo>>>(with_statement_list[entry_idx]);

		if (is_recursive) {
			auto &query_node = with_entry.second->query_node;
			if (!query_node) {
				throw ParserException("Recursive CTEs with DML statements are not supported");
			}
			if (query_node->type == QueryNodeType::INSERT_QUERY_NODE ||
			    query_node->type == QueryNodeType::UPDATE_QUERY_NODE ||
			    query_node->type == QueryNodeType::DELETE_QUERY_NODE) {
				throw ParserException("Recursive CTEs with DML statements are not supported");
			}
			// Now safe to call on SELECT, VALUES, etc.
			query_node = ToRecursiveCTE(std::move(query_node), with_entry.first, with_entry.second->aliases,
			                            with_entry.second->key_targets);
		}
		auto cte_name = string(with_entry.first);

		auto it = result.map.find(cte_name);
		if (it != result.map.end()) {
			// can't have two CTEs with same name
			throw ParserException("Duplicate CTE name \"%s\"", cte_name);
		}
		result.map.insert(with_entry.first, std::move(with_entry.second));
	}
	return result;
}

pair<string, unique_ptr<CommonTableExpressionInfo>>
PEGTransformerFactory::TransformWithStatement(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = make_uniq<CommonTableExpressionInfo>();
	auto cte_name = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	transformer.TransformOptional<vector<string>>(list_pr, 1, result->aliases);
	transformer.TransformOptional<vector<unique_ptr<ParsedExpression>>>(list_pr, 2, result->key_targets);
	auto &materialized_opt = list_pr.Child<OptionalParseResult>(4);
	if (materialized_opt.HasResult()) {
		// If this has a result, we know it is either NEVER or ALWAYS
		bool not_materialized = transformer.Transform<bool>(materialized_opt.GetResult());
		if (not_materialized) {
			result->materialized = CTEMaterialize::CTE_MATERIALIZE_NEVER;
		} else {
			result->materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
		}
	}
	auto table_ref = transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(5));
	D_ASSERT(table_ref->type == TableReferenceType::SUBQUERY);
	auto subquery_ref = unique_ptr_cast<TableRef, SubqueryRef>(std::move(table_ref));
	result->query_node = std::move(subquery_ref->subquery->node);
	return make_pair(cte_name, std::move(result));
}

unique_ptr<TableRef> PEGTransformerFactory::TransformCTEBody(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// CTEBody <- Parens(CTEBodyContent)
	auto &inner = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	// CTEBodyContent <- SelectStatementInternal / Statement
	auto &content_list = inner.Cast<ListParseResult>();
	auto &body_choice = content_list.Child<ChoiceParseResult>(0);
	if (body_choice.GetResult().name == "SelectStatementInternal") {
		auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(body_choice.GetResult());
		return make_uniq<SubqueryRef>(std::move(select_statement));
	}
	// DML body (INSERT / UPDATE / DELETE) - transform as a Statement and extract its QueryNode
	auto sql_stmt = transformer.Transform<unique_ptr<SQLStatement>>(body_choice.GetResult());
	unique_ptr<QueryNode> query_node;
	switch (sql_stmt->type) {
	case StatementType::INSERT_STATEMENT:
		query_node = unique_ptr_cast<InsertQueryNode, QueryNode>(std::move(sql_stmt->Cast<InsertStatement>().node));
		break;
	case StatementType::UPDATE_STATEMENT:
		query_node = unique_ptr_cast<UpdateQueryNode, QueryNode>(std::move(sql_stmt->Cast<UpdateStatement>().node));
		break;
	case StatementType::DELETE_STATEMENT:
		query_node = unique_ptr_cast<DeleteQueryNode, QueryNode>(std::move(sql_stmt->Cast<DeleteStatement>().node));
		break;
	default:
		throw ParserException("A CTE body must be a SELECT, INSERT, UPDATE, or DELETE statement");
	}
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(query_node);
	return make_uniq<SubqueryRef>(std::move(select_statement));
}

bool PEGTransformerFactory::TransformMaterialized(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &not_opt = list_pr.Child<OptionalParseResult>(0);
	return not_opt.HasResult();
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformWindowDefinition(PEGTransformer &transformer,
                                                                              ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	transformer.in_window_definition = true;
	auto window_function = transformer.Transform<unique_ptr<WindowExpression>>(list_pr.Child<ListParseResult>(2));
	transformer.in_window_definition = false;
	window_function->SetAlias(list_pr.Child<IdentifierParseResult>(0).identifier);
	return std::move(window_function);
}

unique_ptr<SampleOptions> PEGTransformerFactory::TransformSampleEntryFunction(PEGTransformer &transformer,
                                                                              ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto result = transformer.Transform<unique_ptr<SampleOptions>>(extract_parens);
	transformer.TransformOptional<SampleMethod>(list_pr, 0, result->method);
	auto &repeatable_sample_opt = list_pr.Child<OptionalParseResult>(2);
	if (repeatable_sample_opt.HasResult()) {
		auto repeatable_seed = transformer.Transform<optional_idx>(repeatable_sample_opt.GetResult());
		result->seed = repeatable_seed;
		result->repeatable = true;
	}
	return result;
}

unique_ptr<SampleOptions> PEGTransformerFactory::TransformSampleEntryCount(PEGTransformer &transformer,
                                                                           ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto sample_count = transformer.Transform<unique_ptr<SampleOptions>>(list_pr.Child<ListParseResult>(0));
	auto &optional_properties = list_pr.Child<OptionalParseResult>(1);
	if (optional_properties.HasResult()) {
		auto &extract_parens = ExtractResultFromParens(optional_properties.GetResult()).Cast<ListParseResult>();
		auto properties = transformer.Transform<pair<SampleMethod, optional_idx>>(extract_parens);
		sample_count->method = properties.first;
		sample_count->seed = properties.second;
		if (sample_count->seed.IsValid()) {
			sample_count->repeatable = true;
		}
	}
	return sample_count;
}

bool PEGTransformerFactory::TransformSamplePercentage(PEGTransformer &transformer) {
	return true;
}

bool PEGTransformerFactory::TransformSampleRows(PEGTransformer &transformer) {
	return false;
}

unique_ptr<SelectStatement>
PEGTransformerFactory::TransformSelectParens(PEGTransformer &transformer,
                                             unique_ptr<SelectStatement> select_statement_internal) {
	return select_statement_internal;
}

vector<unique_ptr<ResultModifier>>
PEGTransformerFactory::TransformResultModifiers(PEGTransformer &transformer,
                                                optional<vector<OrderByNode>> order_by_clause,
                                                optional<unique_ptr<ResultModifier>> limit_offset) {
	vector<unique_ptr<ResultModifier>> result;
	if (order_by_clause) {
		auto order_modifier = make_uniq<OrderModifier>();
		order_modifier->orders = std::move(*order_by_clause);
		result.push_back(std::move(order_modifier));
	}
	if (limit_offset) {
		result.push_back(std::move(*limit_offset));
	}
	return result;
}

unique_ptr<ResultModifier>
PEGTransformerFactory::TransformLimitOffsetClause(PEGTransformer &transformer, LimitPercentResult limit_clause,
                                                  optional<LimitPercentResult> offset_clause) {
	LimitPercentResult empty_offset;
	return VerifyLimitOffset(limit_clause, offset_clause ? *offset_clause : empty_offset);
}

unique_ptr<ResultModifier>
PEGTransformerFactory::TransformOffsetLimitClause(PEGTransformer &transformer, LimitPercentResult offset_clause,
                                                  optional<LimitPercentResult> limit_clause) {
	LimitPercentResult empty_limit;
	return VerifyLimitOffset(limit_clause ? *limit_clause : empty_limit, offset_clause);
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformTableStatement(PEGTransformer &transformer,
                                                                           unique_ptr<BaseTableRef> base_table_name) {
	auto result = make_uniq<SelectStatement>();
	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<StarExpression>());
	node->from_table = std::move(base_table_name);
	result->node = std::move(node);
	return result;
}

unique_ptr<SelectStatement>
PEGTransformerFactory::TransformSimpleSelectParens(PEGTransformer &transformer,
                                                   unique_ptr<SelectStatement> simple_select) {
	return simple_select;
}

unique_ptr<SelectNode> PEGTransformerFactory::TransformSelectFromClause(PEGTransformer &transformer,
                                                                        unique_ptr<SelectNode> select_clause,
                                                                        optional<unique_ptr<TableRef>> from_clause) {
	if (from_clause) {
		select_clause->from_table = std::move(*from_clause);
	} else {
		select_clause->from_table = make_uniq<EmptyTableRef>();
	}
	return select_clause;
}

unique_ptr<SelectNode>
PEGTransformerFactory::TransformFromSelectClause(PEGTransformer &transformer, unique_ptr<TableRef> from_clause,
                                                 optional<unique_ptr<SelectNode>> select_clause) {
	unique_ptr<SelectNode> result;
	if (!select_clause) {
		result = make_uniq<SelectNode>();
		result->select_list.push_back(make_uniq<StarExpression>());
	} else {
		result = std::move(*select_clause);
	}
	result->from_table = std::move(from_clause);
	return result;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformUsingKey(PEGTransformer &transformer,
                                         vector<unique_ptr<ParsedExpression>> target_list) {
	return target_list;
}

unique_ptr<SelectNode>
PEGTransformerFactory::TransformSelectClause(PEGTransformer &transformer, optional<DistinctClause> distinct_clause,
                                             optional<vector<unique_ptr<ParsedExpression>>> target_list) {
	auto result = make_uniq<SelectNode>();
	if (distinct_clause && distinct_clause->is_distinct) {
		auto distinct_modifier = make_uniq<DistinctModifier>();
		for (auto &distinct_on : distinct_clause->distinct_targets) {
			distinct_modifier->distinct_on_targets.push_back(distinct_on->Copy());
		}
		result->modifiers.push_back(std::move(distinct_modifier));
	}
	if (!target_list) {
		throw ParserException("SELECT clause without selection list");
	}
	for (auto &expr_ptr : *target_list) {
		result->select_list.push_back(std::move(expr_ptr));
	}
	return result;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformTargetList(PEGTransformer &transformer,
                                           vector<unique_ptr<ParsedExpression>> aliased_expression) {
	return aliased_expression;
}

vector<string> PEGTransformerFactory::TransformColumnAliases(PEGTransformer &transformer,
                                                             const vector<string> &col_id_or_string) {
	return col_id_or_string;
}

DistinctClause PEGTransformerFactory::TransformDistinctAll(PEGTransformer &transformer) {
	DistinctClause result;
	result.is_distinct = false;
	return result;
}

DistinctClause
PEGTransformerFactory::TransformDistinctOn(PEGTransformer &transformer,
                                           optional<vector<unique_ptr<ParsedExpression>>> distinct_on_targets) {
	DistinctClause result;
	result.is_distinct = true;
	if (distinct_on_targets) {
		result.distinct_targets = std::move(*distinct_on_targets);
	}
	return result;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformDistinctOnTargets(PEGTransformer &transformer,
                                                  vector<unique_ptr<ParsedExpression>> expression) {
	return expression;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformTableSubquery(PEGTransformer &transformer, const bool &has_result,
                                                                   unique_ptr<TableRef> subquery_reference,
                                                                   const optional<TableAlias> &table_alias) {
	if (table_alias) {
		subquery_reference->alias = table_alias->name;
		subquery_reference->column_name_alias = table_alias->column_name_alias;
	}
	return subquery_reference;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformBaseTableRef(PEGTransformer &transformer,
                                                                  const optional<string> &table_alias_colon,
                                                                  unique_ptr<BaseTableRef> base_table_name,
                                                                  const optional<TableAlias> &table_alias,
                                                                  optional<unique_ptr<AtClause>> at_clause,
                                                                  optional<unique_ptr<SampleOptions>> sample_clause) {
	if (table_alias_colon) {
		base_table_name->alias = *table_alias_colon;
	}
	if (table_alias && table_alias_colon) {
		throw ParserException("Table reference %s cannot have two aliases", base_table_name->ToString());
	}
	if (table_alias) {
		base_table_name->alias = table_alias->name;
		base_table_name->column_name_alias = table_alias->column_name_alias;
	}
	if (at_clause) {
		base_table_name->at_clause = std::move(*at_clause);
	}
	if (sample_clause) {
		base_table_name->sample = std::move(*sample_clause);
	}
	return std::move(base_table_name);
}

string PEGTransformerFactory::TransformTableAliasColon(PEGTransformer &transformer, const string &col_id_or_string) {
	return col_id_or_string;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformValuesRef(PEGTransformer &transformer,
                                                               unique_ptr<SelectStatement> values_clause,
                                                               const optional<TableAlias> &table_alias) {
	auto subquery_ref = make_uniq<SubqueryRef>(std::move(values_clause));
	if (table_alias) {
		subquery_ref->alias = table_alias->name;
		subquery_ref->column_name_alias = table_alias->column_name_alias;
	}
	return std::move(subquery_ref);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformParensTableRef(PEGTransformer &transformer,
                                                                    const optional<string> &table_alias_colon,
                                                                    unique_ptr<TableRef> table_ref,
                                                                    const optional<TableAlias> &table_alias,
                                                                    optional<unique_ptr<SampleOptions>> sample_clause) {
	if (table_alias_colon && table_alias) {
		throw ParserException("Table reference %s cannot have two aliases", table_ref->ToString());
	}
	if (!table_alias_colon && !table_alias && !sample_clause) {
		return table_ref;
	}
	auto select_statement = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(table_ref);
	select_statement->node = std::move(select_node);
	auto subquery = make_uniq<SubqueryRef>(std::move(select_statement));
	if (table_alias_colon) {
		subquery->alias = *table_alias_colon;
	}
	if (table_alias) {
		subquery->alias = table_alias->name;
		subquery->column_name_alias = table_alias->column_name_alias;
	}
	if (sample_clause) {
		subquery->sample = std::move(*sample_clause);
	}
	return std::move(subquery);
}

vector<string> PEGTransformerFactory::TransformPivotGroupByList(PEGTransformer &transformer,
                                                                const vector<string> &col_id_or_string) {
	return col_id_or_string;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformPivotHeader(PEGTransformer &transformer,
                                                                         unique_ptr<ParsedExpression> base_expression) {
	return base_expression;
}

PivotColumn PEGTransformerFactory::TransformUnpivotValueList(PEGTransformer &transformer,
                                                             const vector<string> &unpivot_header,
                                                             vector<PivotColumnEntry> unpivot_target_list) {
	PivotColumn result;
	result.unpivot_names = unpivot_header;
	if (result.unpivot_names.size() != 1) {
		throw ParserException("UNPIVOT requires a single column name for the PIVOT IN clause");
	}
	result.entries = std::move(unpivot_target_list);
	return result;
}

vector<PivotColumnEntry>
PEGTransformerFactory::TransformPivotTargetList(PEGTransformer &transformer,
                                                vector<unique_ptr<ParsedExpression>> target_list) {
	vector<PivotColumnEntry> result;
	for (auto &target : target_list) {
		PivotColumnEntry pivot_entry;
		pivot_entry.alias = target->GetAlias();
		bool transformed = TransformPivotInList(target, pivot_entry);
		if (!transformed) {
			pivot_entry.expr = std::move(target);
		}
		result.push_back(std::move(pivot_entry));
	}
	return result;
}

vector<PivotColumnEntry>
PEGTransformerFactory::TransformUnpivotTargetList(PEGTransformer &transformer,
                                                  vector<unique_ptr<ParsedExpression>> target_list) {
	vector<PivotColumnEntry> result;
	for (auto &target : target_list) {
		PivotColumnEntry pivot_entry;
		pivot_entry.alias = target->GetAlias();
		pivot_entry.expr = std::move(target);
		result.push_back(std::move(pivot_entry));
	}
	return result;
}

unique_ptr<BaseTableRef> PEGTransformerFactory::TransformSchemaReservedTable(PEGTransformer &transformer,
                                                                             const string &schema_qualification,
                                                                             const string &reserved_table_name) {
	const auto description = TableDescription(INVALID_CATALOG, schema_qualification, reserved_table_name);
	return make_uniq<BaseTableRef>(description);
}

unique_ptr<BaseTableRef> PEGTransformerFactory::TransformCatalogReservedSchemaTable(
    PEGTransformer &transformer, const string &catalog_qualification, const string &reserved_schema_qualification,
    const string &reserved_table_name) {
	const auto description =
	    TableDescription(catalog_qualification, reserved_schema_qualification, reserved_table_name);
	return make_uniq<BaseTableRef>(description);
}

QualifiedName PEGTransformerFactory::TransformQualifiedTableFunction(PEGTransformer &transformer,
                                                                     const optional<string> &catalog_qualification,
                                                                     const optional<string> &schema_qualification,
                                                                     const string &table_function_name) {
	QualifiedName result;
	result.catalog = catalog_qualification ? *catalog_qualification : INVALID_CATALOG;
	result.schema = schema_qualification ? *schema_qualification : INVALID_SCHEMA;
	if (!result.catalog.empty() && result.schema.empty()) {
		result.schema = result.catalog;
		result.catalog = INVALID_CATALOG;
	}
	result.name = table_function_name;
	return result;
}

vector<FunctionArgument>
PEGTransformerFactory::TransformTableFunctionArguments(PEGTransformer &transformer,
                                                       optional<vector<FunctionArgument>> function_argument) {
	if (function_argument) {
		return std::move(*function_argument);
	}
	return {};
}

TableAlias PEGTransformerFactory::TransformTableAliasAs(PEGTransformer &transformer,
                                                        const QualifiedName &identifier_or_string_literal,
                                                        const optional<vector<string>> &column_aliases) {
	TableAlias result;
	result.name = identifier_or_string_literal.name;
	if (column_aliases) {
		result.column_name_alias = *column_aliases;
	}
	return result;
}

TableAlias PEGTransformerFactory::TransformTableAliasWithoutAs(PEGTransformer &transformer, const string &identifier,
                                                               const optional<vector<string>> &column_aliases) {
	TableAlias result;
	result.name = identifier;
	if (column_aliases) {
		result.column_name_alias = *column_aliases;
	}
	return result;
}

unique_ptr<AtClause> PEGTransformerFactory::TransformAtClause(PEGTransformer &transformer,
                                                              unique_ptr<AtClause> at_specifier) {
	return at_specifier;
}

unique_ptr<AtClause> PEGTransformerFactory::TransformAtSpecifier(PEGTransformer &transformer, const string &at_unit,
                                                                 unique_ptr<ParsedExpression> expression) {
	return make_uniq<AtClause>(at_unit, std::move(expression));
}

unique_ptr<TableRef> PEGTransformerFactory::TransformJoinWithoutOnClause(PEGTransformer &transformer,
                                                                         const JoinPrefix &join_prefix,
                                                                         unique_ptr<TableRef> table_ref) {
	auto result = make_uniq<JoinRef>();
	result->ref_type = join_prefix.ref_type;
	result->type = join_prefix.join_type;
	result->right = std::move(table_ref);
	return std::move(result);
}

JoinQualifier PEGTransformerFactory::TransformOnClause(PEGTransformer &transformer,
                                                       unique_ptr<ParsedExpression> expression) {
	JoinQualifier result;
	result.on_clause = std::move(expression);
	return result;
}

JoinQualifier PEGTransformerFactory::TransformUsingClause(PEGTransformer &transformer,
                                                          const vector<string> &column_name) {
	JoinQualifier result;
	for (auto &col_identifier : column_name) {
		if (col_identifier.empty()) {
			throw ParserException("Column identifier cannot be empty");
		}
		result.using_columns.push_back(col_identifier);
	}
	return result;
}

JoinPrefix PEGTransformerFactory::TransformCrossJoinPrefix(PEGTransformer &transformer) {
	JoinPrefix result;
	result.ref_type = JoinRefType::CROSS;
	return result;
}

JoinPrefix PEGTransformerFactory::TransformNaturalJoinPrefix(PEGTransformer &transformer,
                                                             const optional<JoinType> &join_type) {
	JoinPrefix result;
	result.ref_type = JoinRefType::NATURAL;
	result.join_type = join_type.value_or(JoinType::INNER);
	return result;
}

JoinPrefix PEGTransformerFactory::TransformPositionalJoinPrefix(PEGTransformer &transformer) {
	JoinPrefix result;
	result.ref_type = JoinRefType::POSITIONAL;
	return result;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformFromClause(PEGTransformer &transformer,
                                                                vector<unique_ptr<TableRef>> table_ref) {
	auto result_table_ref = std::move(table_ref[0]);
	if (table_ref.size() == 1) {
		return result_table_ref;
	}
	for (idx_t i = 1; i < table_ref.size(); i++) {
		auto cross_product = make_uniq<JoinRef>();
		cross_product->left = std::move(result_table_ref);
		cross_product->right = std::move(table_ref[i]);
		cross_product->ref_type = JoinRefType::CROSS;
		cross_product->is_implicit = true;
		result_table_ref = std::move(cross_product);
	}
	return result_table_ref;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformWhereClause(PEGTransformer &transformer,
                                                                         unique_ptr<ParsedExpression> expression) {
	return expression;
}

GroupByNode PEGTransformerFactory::TransformGroupByClause(PEGTransformer &transformer,
                                                          GroupByNode group_by_expressions) {
	return group_by_expressions;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformHavingClause(PEGTransformer &transformer,
                                                                          unique_ptr<ParsedExpression> expression) {
	return expression;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformQualifyClause(PEGTransformer &transformer,
                                                                           unique_ptr<ParsedExpression> expression) {
	return expression;
}

unique_ptr<SampleOptions> PEGTransformerFactory::TransformSampleClause(PEGTransformer &transformer,
                                                                       unique_ptr<SampleOptions> sample_entry) {
	return sample_entry;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformWindowClause(PEGTransformer &transformer,
                                             vector<unique_ptr<ParsedExpression>> window_definition) {
	return window_definition;
}

SampleMethod PEGTransformerFactory::TransformSampleFunction(PEGTransformer &transformer, const string &col_id) {
	return EnumUtil::FromString<SampleMethod>(col_id);
}

pair<SampleMethod, optional_idx>
PEGTransformerFactory::TransformSampleProperties(PEGTransformer &transformer, const string &col_id,
                                                 const optional<optional_idx> &sample_seed) {
	return make_pair(EnumUtil::FromString<SampleMethod>(col_id), sample_seed ? *sample_seed : optional_idx());
}

optional_idx PEGTransformerFactory::TransformRepeatableSample(PEGTransformer &transformer,
                                                              const optional_idx &sample_seed) {
	return sample_seed;
}

optional_idx PEGTransformerFactory::TransformSampleSeed(PEGTransformer &transformer,
                                                        unique_ptr<ParsedExpression> number_literal) {
	auto const_expr = number_literal->Cast<ConstantExpression>();
	return optional_idx(const_expr.GetValue().GetValue<idx_t>());
}

unique_ptr<SampleOptions> PEGTransformerFactory::TransformSampleCount(PEGTransformer &transformer,
                                                                      unique_ptr<ParsedExpression> sample_value,
                                                                      const optional<bool> &sample_unit) {
	auto result = make_uniq<SampleOptions>();
	if (sample_value->GetExpressionClass() != ExpressionClass::CONSTANT) {
		throw ParserException(sample_value->GetQueryLocation(),
		                      "Only constants are supported in sample clause currently");
	}
	auto &const_expr = sample_value->Cast<ConstantExpression>();
	auto &sample_value_const = const_expr.GetValue();
	result->is_percentage = sample_unit.value_or(false);
	if (result->is_percentage) {
		auto percentage = sample_value_const.GetValue<double>();
		if (percentage < 0 || percentage > 100) {
			throw ParserException("Sample sample_size %llf out of range, must be between 0 and 100", percentage);
		}
		result->sample_size = Value::DOUBLE(percentage);
		result->method = SampleMethod::SYSTEM_SAMPLE;
	} else {
		auto rows = sample_value_const.GetValue<int64_t>();
		if (rows < 0 || sample_value_const.GetValue<uint64_t>() > SampleOptions::MAX_SAMPLE_ROWS) {
			throw ParserException("Sample rows %lld out of range, must be between 0 and %lld", rows,
			                      SampleOptions::MAX_SAMPLE_ROWS);
		}
		result->sample_size = Value::BIGINT(rows);
		result->method = SampleMethod::RESERVOIR_SAMPLE;
	}
	return result;
}

GroupByNode PEGTransformerFactory::TransformGroupByAll(PEGTransformer &transformer) {
	GroupByNode result;
	result.group_expressions.push_back(make_uniq<StarExpression>());
	return result;
}

unique_ptr<TableRef>
PEGTransformerFactory::TransformSubqueryReference(PEGTransformer &transformer,
                                                  unique_ptr<SelectStatement> select_statement_internal) {
	return make_uniq<SubqueryRef>(std::move(select_statement_internal));
}

OrderByNode PEGTransformerFactory::TransformOrderByExpression(PEGTransformer &transformer,
                                                              unique_ptr<ParsedExpression> expression,
                                                              const optional<OrderType> &desc_or_asc,
                                                              const optional<OrderByNullType> &nulls_first_or_last) {
	return OrderByNode(desc_or_asc.value_or(OrderType::ORDER_DEFAULT),
	                   nulls_first_or_last.value_or(OrderByNullType::ORDER_DEFAULT), std::move(expression));
}

vector<OrderByNode> PEGTransformerFactory::TransformOrderByClause(PEGTransformer &transformer,
                                                                  vector<OrderByNode> order_by_expressions) {
	return order_by_expressions;
}

vector<OrderByNode> PEGTransformerFactory::TransformOrderByExpressionList(PEGTransformer &transformer,
                                                                          vector<OrderByNode> order_by_expression) {
	return order_by_expression;
}

vector<OrderByNode> PEGTransformerFactory::TransformOrderByAll(PEGTransformer &transformer,
                                                               const optional<OrderType> &desc_or_asc,
                                                               const optional<OrderByNullType> &nulls_first_or_last) {
	vector<OrderByNode> result;
	auto star_expr = make_uniq<StarExpression>();
	star_expr->IsColumnsMutable() = true;
	result.push_back(OrderByNode(desc_or_asc.value_or(OrderType::ORDER_DEFAULT),
	                             nulls_first_or_last.value_or(OrderByNullType::ORDER_DEFAULT), std::move(star_expr)));
	return result;
}

LimitPercentResult PEGTransformerFactory::TransformLimitClause(PEGTransformer &transformer,
                                                               LimitPercentResult limit_value) {
	return limit_value;
}

LimitPercentResult PEGTransformerFactory::TransformOffsetClause(PEGTransformer &transformer,
                                                                LimitPercentResult offset_value) {
	return offset_value;
}

LimitPercentResult PEGTransformerFactory::TransformOffsetValue(PEGTransformer &transformer,
                                                               unique_ptr<ParsedExpression> expression,
                                                               const bool &has_result) {
	LimitPercentResult result;
	result.expression = std::move(expression);
	return result;
}

LimitPercentResult PEGTransformerFactory::TransformLimitAll(PEGTransformer &transformer) {
	LimitPercentResult result;
	result.expression = make_uniq<ConstantExpression>(Value());
	result.is_percent = false;
	return result;
}

LimitPercentResult PEGTransformerFactory::TransformLimitLiteralPercent(PEGTransformer &transformer,
                                                                       unique_ptr<ParsedExpression> number_literal) {
	LimitPercentResult result;
	result.expression = std::move(number_literal);
	result.is_percent = true;
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformColIdExpression(PEGTransformer &transformer,
                                                                             const string &col_id,
                                                                             unique_ptr<ParsedExpression> expression) {
	expression->SetAlias(col_id);
	return expression;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExpressionAsCollabel(
    PEGTransformer &transformer, unique_ptr<ParsedExpression> expression, const string &col_label_or_string) {
	expression->SetAlias(col_label_or_string);
	return expression;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExpressionOptIdentifier(
    PEGTransformer &transformer, unique_ptr<ParsedExpression> expression, const optional<string> &identifier) {
	if (identifier) {
		expression->SetAlias(*identifier);
	}
	return expression;
}

unique_ptr<SelectStatement>
PEGTransformerFactory::TransformValuesClause(PEGTransformer &transformer,
                                             vector<vector<unique_ptr<ParsedExpression>>> values_expressions) {
	if (values_expressions.size() > 1) {
		const auto expected_size = values_expressions[0].size();
		for (idx_t i = 1; i < values_expressions.size(); i++) {
			if (values_expressions[i].size() != expected_size) {
				throw ParserException(
				    *values_expressions[i][0],
				    "VALUES lists must all be the same length, expected %d %s but found a list with %d %s",
				    expected_size, expected_size == 1 ? "entry" : "entries", values_expressions[i].size(),
				    values_expressions[i].size() == 1 ? "entry" : "entries");
			}
		}
	}
	auto result = make_uniq<ExpressionListRef>();
	result->alias = "valueslist";
	result->values = std::move(values_expressions);

	auto select_node = make_uniq<SelectNode>();
	select_node->from_table = std::move(result);
	select_node->select_list.push_back(make_uniq<StarExpression>());
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(select_node);
	return select_statement;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformValuesExpressions(PEGTransformer &transformer,
                                                  vector<unique_ptr<ParsedExpression>> expression) {
	return expression;
}

} // namespace duckdb
