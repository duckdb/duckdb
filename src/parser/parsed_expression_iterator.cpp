#include "duckdb/parser/parsed_expression_iterator.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/query_node/merge_query_node.hpp"
#include "duckdb/parser/query_node/copy_query_node.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/tableref/list.hpp"
#include "duckdb/parser/tableref/match_recognize_ref.hpp"

namespace duckdb {

void ParsedExpressionIterator::EnumerateChildren(const ParsedExpression &expression,
                                                 const std::function<void(const ParsedExpression &child)> &callback) {
	for (const auto &child : expression.Children()) {
		callback(child);
	}
}

void ParsedExpressionIterator::EnumerateChildren(ParsedExpression &expr,
                                                 const std::function<void(ParsedExpression &child)> &callback) {
	EnumerateChildren(expr, [&](unique_ptr<ParsedExpression> &child) {
		D_ASSERT(child);
		callback(*child);
	});
}

void ParsedExpressionIterator::EnumerateChildren(
    ParsedExpression &expr, const std::function<void(unique_ptr<ParsedExpression> &child)> &callback) {
	for (auto &child : expr.ChildrenMutable()) {
		callback(child);
	}
}

void ParsedExpressionIterator::EnumerateQueryNodeModifiers(
    QueryNode &node, const std::function<void(unique_ptr<ParsedExpression> &child)> &callback) {
	for (auto &modifier : node.modifiers) {
		switch (modifier->type) {
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit_modifier = modifier->Cast<LimitModifier>();
			if (limit_modifier.limit) {
				callback(limit_modifier.limit);
			}
			if (limit_modifier.offset) {
				callback(limit_modifier.offset);
			}
		} break;
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order_modifier = modifier->Cast<OrderModifier>();
			for (auto &order : order_modifier.orders) {
				callback(order.expression);
			}
		} break;

		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct_modifier = modifier->Cast<DistinctModifier>();
			for (auto &target : distinct_modifier.distinct_on_targets) {
				callback(target);
			}
		} break;

		// do nothing
		default:
			break;
		}
	}
}

namespace {

struct QueryExpressionIterator {
	const std::function<void(unique_ptr<ParsedExpression> &)> &expr_callback;
	const std::function<void(TableRef &)> &ref_callback;
	bool all_expressions;

	void Visit(TableRef &ref);
	void Visit(QueryNode &node);
};

void QueryExpressionIterator::Visit(TableRef &ref) {
	switch (ref.type) {
	case TableReferenceType::EXPRESSION_LIST: {
		auto &el_ref = ref.Cast<ExpressionListRef>();
		for (idx_t i = 0; i < el_ref.values.size(); i++) {
			for (idx_t j = 0; j < el_ref.values[i].size(); j++) {
				expr_callback(el_ref.values[i][j]);
			}
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &j_ref = ref.Cast<JoinRef>();
		Visit(*j_ref.left);
		Visit(*j_ref.right);
		if (j_ref.condition) {
			expr_callback(j_ref.condition);
		}
		for (auto &column : j_ref.duplicate_eliminated_columns) {
			expr_callback(column);
		}
		if (j_ref.ranking_expression) {
			expr_callback(j_ref.ranking_expression);
		}
		break;
	}
	case TableReferenceType::PIVOT: {
		auto &p_ref = ref.Cast<PivotRef>();
		Visit(*p_ref.source);
		for (auto &aggr : p_ref.aggregates) {
			expr_callback(aggr);
		}
		for (auto &pivot : p_ref.pivots) {
			for (auto &expr : pivot.pivot_expressions) {
				expr_callback(expr);
			}
			for (auto &entry : pivot.entries) {
				if (entry.expr) {
					expr_callback(entry.expr);
				}
			}
			if (pivot.subquery) {
				Visit(*pivot.subquery);
			}
		}
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &sq_ref = ref.Cast<SubqueryRef>();
		Visit(*sq_ref.subquery->node);
		break;
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &tf_ref = ref.Cast<TableFunctionRef>();
		expr_callback(tf_ref.function);
		if (all_expressions && tf_ref.subquery) {
			Visit(*tf_ref.subquery->node);
		}
		break;
	}
	case TableReferenceType::MATCH_RECOGNIZE: {
		auto &mr_ref = ref.Cast<MatchRecognizeRef>();
		for (auto &expr : mr_ref.config->partition_expressions) {
			expr_callback(expr);
		}
		for (auto &order : mr_ref.config->order_by_expressions) {
			expr_callback(order.expression);
		}
		for (auto &expr : mr_ref.config->measures_expression_list) {
			expr_callback(expr);
		}
		for (auto &expr : mr_ref.config->defines_expression_list) {
			expr_callback(expr);
		}
		if (mr_ref.config->pattern) {
			expr_callback(mr_ref.config->pattern);
		}
		if (mr_ref.input) {
			Visit(*mr_ref.input);
		}
		break;
	}
	case TableReferenceType::SHOW_REF: {
		auto &show_ref = ref.Cast<ShowRef>();
		if (show_ref.query) {
			Visit(*show_ref.query);
		}
		break;
	}
	case TableReferenceType::BASE_TABLE: {
		auto &base_ref = ref.Cast<BaseTableRef>();
		if (base_ref.at_clause && base_ref.at_clause->ExpressionMutable()) {
			expr_callback(base_ref.at_clause->ExpressionMutable());
		}
		break;
	}
	case TableReferenceType::EMPTY_FROM:
	case TableReferenceType::COLUMN_DATA:
	case TableReferenceType::DELIM_GET:
		// these TableRefs do not need to be unfolded
		break;
	case TableReferenceType::INVALID:
	case TableReferenceType::CTE:
	case TableReferenceType::BOUND_TABLE_REF:
		throw NotImplementedException("TableRef type not implemented for traversal");
	}
	ref_callback(ref);
}

void QueryExpressionIterator::Visit(QueryNode &node) {
	switch (node.type) {
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &rcte_node = node.Cast<RecursiveCTENode>();
		if (all_expressions) {
			for (auto &expr : rcte_node.key_targets) {
				expr_callback(expr);
			}
		}
		Visit(*rcte_node.left);
		Visit(*rcte_node.right);
		break;
	}
	case QueryNodeType::SELECT_NODE: {
		auto &sel_node = node.Cast<SelectNode>();
		for (idx_t i = 0; i < sel_node.select_list.size(); i++) {
			expr_callback(sel_node.select_list[i]);
		}
		for (idx_t i = 0; i < sel_node.groups.group_expressions.size(); i++) {
			expr_callback(sel_node.groups.group_expressions[i]);
		}
		if (sel_node.where_clause) {
			expr_callback(sel_node.where_clause);
		}
		if (sel_node.having) {
			expr_callback(sel_node.having);
		}
		if (sel_node.qualify) {
			expr_callback(sel_node.qualify);
		}

		if (sel_node.from_table) {
			Visit(*sel_node.from_table);
		}
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop_node = node.Cast<SetOperationNode>();
		for (auto &child : setop_node.children) {
			Visit(*child);
		}
		break;
	}
	case QueryNodeType::UPDATE_QUERY_NODE: {
		auto &upd_node = node.Cast<UpdateQueryNode>();
		if (upd_node.table) {
			Visit(*upd_node.table);
		}
		if (upd_node.from_table) {
			Visit(*upd_node.from_table);
		}
		if (upd_node.set_info) {
			for (auto &expr : upd_node.set_info->expressions) {
				expr_callback(expr);
			}
			if (upd_node.set_info->condition) {
				expr_callback(upd_node.set_info->condition);
			}
		}
		for (auto &expr : upd_node.returning_list) {
			expr_callback(expr);
		}
		break;
	}
	case QueryNodeType::DELETE_QUERY_NODE: {
		auto &del_node = node.Cast<DeleteQueryNode>();
		if (del_node.table) {
			Visit(*del_node.table);
		}
		for (auto &using_clause : del_node.using_clauses) {
			Visit(*using_clause);
		}
		if (del_node.condition) {
			expr_callback(del_node.condition);
		}
		for (auto &expr : del_node.returning_list) {
			expr_callback(expr);
		}
		break;
	}
	case QueryNodeType::INSERT_QUERY_NODE: {
		auto &ins_node = node.Cast<InsertQueryNode>();
		if (ins_node.select_statement) {
			Visit(*ins_node.select_statement->node);
		}
		if (ins_node.table_ref) {
			Visit(*ins_node.table_ref);
		}
		for (auto &expr : ins_node.returning_list) {
			expr_callback(expr);
		}
		if (ins_node.on_conflict_info && ins_node.on_conflict_info->set_info) {
			for (auto &expr : ins_node.on_conflict_info->set_info->expressions) {
				expr_callback(expr);
			}
			if (ins_node.on_conflict_info->set_info->condition) {
				expr_callback(ins_node.on_conflict_info->set_info->condition);
			}
		}
		if (ins_node.on_conflict_info && ins_node.on_conflict_info->condition) {
			expr_callback(ins_node.on_conflict_info->condition);
		}
		break;
	}
	case QueryNodeType::MERGE_QUERY_NODE: {
		auto &merge_node = node.Cast<MergeQueryNode>();
		if (merge_node.target) {
			Visit(*merge_node.target);
		}
		if (merge_node.source) {
			Visit(*merge_node.source);
		}
		if (merge_node.join_condition) {
			expr_callback(merge_node.join_condition);
		}
		for (auto &entry : merge_node.actions) {
			for (auto &action : entry.second) {
				if (action->condition) {
					expr_callback(action->condition);
				}
				if (action->update_info) {
					for (auto &expr : action->update_info->expressions) {
						expr_callback(expr);
					}
					if (action->update_info->condition) {
						expr_callback(action->update_info->condition);
					}
				}
				for (auto &expr : action->expressions) {
					expr_callback(expr);
				}
			}
		}
		for (auto &expr : merge_node.returning_list) {
			expr_callback(expr);
		}
		break;
	}
	case QueryNodeType::COPY_QUERY_NODE: {
		auto &copy_node = node.Cast<CopyQueryNode>();
		auto &copy_info = *copy_node.info;
		if (copy_info.select_statement) {
			Visit(*copy_info.select_statement);
		}
		if (copy_info.file_path_expression) {
			expr_callback(copy_info.file_path_expression);
		}
		for (auto &option : copy_info.parsed_options) {
			if (option.second) {
				expr_callback(option.second);
			}
		}
		break;
	}
	default:
		throw NotImplementedException("QueryNode type not implemented for traversal");
	}

	if (!node.modifiers.empty()) {
		ParsedExpressionIterator::EnumerateQueryNodeModifiers(node, expr_callback);
	}

	for (auto &kv : node.cte_map.map) {
		if (all_expressions) {
			for (auto &expr : kv.second->key_targets) {
				expr_callback(expr);
			}
			for (auto &expr : kv.second->payload_aggregates) {
				expr_callback(expr);
			}
		}
		if (kv.second->query_node) {
			Visit(*kv.second->query_node);
		}
	}
}

} // namespace

void ParsedExpressionIterator::EnumerateTableRefChildren(
    TableRef &ref, const std::function<void(unique_ptr<ParsedExpression> &child)> &expr_callback,
    const std::function<void(TableRef &ref)> &ref_callback) {
	QueryExpressionIterator {expr_callback, ref_callback, false}.Visit(ref);
}

void ParsedExpressionIterator::EnumerateQueryNodeChildren(
    QueryNode &node, const std::function<void(unique_ptr<ParsedExpression> &child)> &expr_callback,
    const std::function<void(TableRef &ref)> &ref_callback) {
	QueryExpressionIterator {expr_callback, ref_callback, false}.Visit(node);
}

void ParsedExpressionIterator::EnumerateAllQueryNodeChildren(
    QueryNode &node, const std::function<void(unique_ptr<ParsedExpression> &child)> &expr_callback) {
	QueryExpressionIterator {expr_callback, DefaultRefCallback, true}.Visit(node);
}

void ParsedExpressionIterator::VisitExpressionClass(
    const ParsedExpression &expr, ExpressionClass expr_class,
    const std::function<void(const ParsedExpression &child)> &callback) {
	if (expr.GetExpressionClass() == expr_class) {
		callback(expr);
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { VisitExpressionClass(child, expr_class, callback); });
}

void ParsedExpressionIterator::VisitExpressionClassMutable(
    ParsedExpression &expr, ExpressionClass expr_class, const std::function<void(ParsedExpression &child)> &callback) {
	if (expr.GetExpressionClass() == expr_class) {
		callback(expr);
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { VisitExpressionClassMutable(child, expr_class, callback); });
}

} // namespace duckdb
