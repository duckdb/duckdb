#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/expression_binder/group_binder.hpp"
#include "duckdb/planner/expression_binder/having_binder.hpp"
#include "duckdb/planner/expression_binder/order_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/parser/expression/table_star_expression.hpp"

using namespace std;

namespace duckdb {

static int64_t BindConstant(Binder &binder, ClientContext &context, string clause, unique_ptr<ParsedExpression> &expr) {
	ConstantBinder constant_binder(binder, context, clause);
	auto bound_expr = constant_binder.Bind(expr);
	Value value = ExpressionExecutor::EvaluateScalar(*bound_expr);
	if (!TypeIsNumeric(value.type)) {
		throw BinderException("LIMIT clause can only contain numeric constants!");
	}
	int64_t limit_value = value.GetValue<int64_t>();
	if (limit_value < 0) {
		throw BinderException("LIMIT must not be negative");
	}
	return limit_value;
}

unique_ptr<Expression> Binder::BindFilter(unique_ptr<ParsedExpression> condition) {
	WhereBinder where_binder(*this, context);
	return where_binder.Bind(condition);
}

unique_ptr<Expression> Binder::BindOrderExpression(OrderBinder &order_binder, unique_ptr<ParsedExpression> expr) {
	// we treat the Distinct list as a order by
	auto bound_expr = order_binder.Bind(move(expr));
	if (!bound_expr) {
		// DISTINCT ON non-integer constant
		// remove the expression from the DISTINCT ON list
		return nullptr;
	}
	assert(bound_expr->type == ExpressionType::BOUND_COLUMN_REF);
	return bound_expr;
}

unique_ptr<BoundResultModifier> Binder::BindLimit(LimitModifier &limit_mod) {
	auto result = make_unique<BoundLimitModifier>();
	if (limit_mod.limit) {
		result->limit = BindConstant(*this, context, "LIMIT clause", limit_mod.limit);
		result->offset = 0;
	}
	if (limit_mod.offset) {
		result->offset = BindConstant(*this, context, "OFFSET clause", limit_mod.offset);
		if (!limit_mod.limit) {
			result->limit = std::numeric_limits<int64_t>::max();
		}
	}
	return move(result);
}

void Binder::BindModifiers(OrderBinder &order_binder, QueryNode &statement, BoundQueryNode &result) {
	for (auto &mod : statement.modifiers) {
		unique_ptr<BoundResultModifier> bound_modifier;
		switch (mod->type) {
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct = (DistinctModifier &)*mod;
			auto bound_distinct = make_unique<BoundDistinctModifier>();
			for (idx_t i = 0; i < distinct.distinct_on_targets.size(); i++) {
				auto expr = BindOrderExpression(order_binder, move(distinct.distinct_on_targets[i]));
				if (!expr) {
					continue;
				}
				bound_distinct->target_distincts.push_back(move(expr));
			}
			bound_modifier = move(bound_distinct);
			break;
		}
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order = (OrderModifier &)*mod;
			auto bound_order = make_unique<BoundOrderModifier>();
			for (idx_t i = 0; i < order.orders.size(); i++) {
				BoundOrderByNode node;
				node.type = order.orders[i].type;
				node.expression = BindOrderExpression(order_binder, move(order.orders[i].expression));
				if (!node.expression) {
					continue;
				}
				bound_order->orders.push_back(move(node));
			}
			if (bound_order->orders.size() > 0) {
				bound_modifier = move(bound_order);
			}
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER:
			bound_modifier = BindLimit((LimitModifier &)*mod);
			break;
		default:
			throw Exception("Unsupported result modifier");
		}
		if (bound_modifier) {
			result.modifiers.push_back(move(bound_modifier));
		}
	}
}

void Binder::BindModifierTypes(BoundQueryNode &result, const vector<SQLType> &sql_types, idx_t projection_index) {
	for (auto &bound_mod : result.modifiers) {
		switch (bound_mod->type) {
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct = (BoundDistinctModifier &)*bound_mod;
			if (distinct.target_distincts.size() == 0) {
				// DISTINCT without a target: push references to the standard select list
				for (idx_t i = 0; i < sql_types.size(); i++) {
					distinct.target_distincts.push_back(
					    make_unique<BoundColumnRefExpression>(GetInternalType(sql_types[i]), ColumnBinding(projection_index, i)));
				}
			} else {
				// DISTINCT with target list: set types
				for (idx_t i = 0; i < distinct.target_distincts.size(); i++) {
					auto &expr = distinct.target_distincts[i];
					assert(expr->type == ExpressionType::BOUND_COLUMN_REF);
					auto &bound_colref = (BoundColumnRefExpression &)*expr;
					if (bound_colref.binding.column_index == INVALID_INDEX) {
						throw BinderException("Ambiguous name in DISTINCT ON!");
					}
					assert(bound_colref.binding.column_index < sql_types.size());
					bound_colref.return_type = GetInternalType(sql_types[bound_colref.binding.column_index]);
				}
			}
			for(idx_t i = 0; i < distinct.target_distincts.size(); i++) {
				auto &bound_colref = (BoundColumnRefExpression &)*distinct.target_distincts[i];
				auto sql_type = sql_types[bound_colref.binding.column_index];
				if (sql_type.id == SQLTypeId::VARCHAR) {
					distinct.target_distincts[i] = ExpressionBinder::PushCollation(context, move(distinct.target_distincts[i]), sql_type.collation, true);
				}
			}
			break;
		}
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order = (BoundOrderModifier &)*bound_mod;
			for (idx_t i = 0; i < order.orders.size(); i++) {
				auto &expr = order.orders[i].expression;
				assert(expr->type == ExpressionType::BOUND_COLUMN_REF);
				auto &bound_colref = (BoundColumnRefExpression &)*expr;
				if (bound_colref.binding.column_index == INVALID_INDEX) {
					throw BinderException("Ambiguous name in ORDER BY!");
				}
				assert(bound_colref.binding.column_index < sql_types.size());
				auto sql_type = sql_types[bound_colref.binding.column_index];
				bound_colref.return_type = GetInternalType(sql_types[bound_colref.binding.column_index]);
				if (sql_type.id == SQLTypeId::VARCHAR) {
					order.orders[i].expression = ExpressionBinder::PushCollation(context, move(order.orders[i].expression), sql_type.collation);
				}
			}
			break;
		}
		default:
			break;
		}
	}
}

unique_ptr<BoundQueryNode> Binder::BindNode(SelectNode &statement) {
	auto result = make_unique<BoundSelectNode>();
	result->projection_index = GenerateTableIndex();
	result->group_index = GenerateTableIndex();
	result->aggregate_index = GenerateTableIndex();
	result->window_index = GenerateTableIndex();
	result->unnest_index = GenerateTableIndex();
	result->prune_index = GenerateTableIndex();

	// first bind the FROM table statement
	result->from_table = Bind(*statement.from_table);

	// visit the select list and expand any "*" statements
	vector<unique_ptr<ParsedExpression>> new_select_list;
	for (auto &select_element : statement.select_list) {
		if (select_element->GetExpressionType() == ExpressionType::STAR) {
			// * statement, expand to all columns from the FROM clause
			bind_context.GenerateAllColumnExpressions(new_select_list);
		} else if (select_element->GetExpressionType() == ExpressionType::TABLE_STAR) {
			auto table_star =
			    (TableStarExpression *)select_element.get(); // TODO this cast to explicit class is a bit dirty?
			bind_context.GenerateAllColumnExpressions(new_select_list, table_star->relation_name);
		} else {
			// regular statement, add it to the list
			new_select_list.push_back(move(select_element));
		}
	}
	statement.select_list = move(new_select_list);

	// create a mapping of (alias -> index) and a mapping of (Expression -> index) for the SELECT list
	unordered_map<string, idx_t> alias_map;
	expression_map_t<idx_t> projection_map;
	for (idx_t i = 0; i < statement.select_list.size(); i++) {
		auto &expr = statement.select_list[i];
		result->names.push_back(expr->GetName());
		if (!expr->alias.empty()) {
			alias_map[expr->alias] = i;
		}
		ExpressionBinder::BindTableNames(*this, *expr);
		projection_map[expr.get()] = i;
		result->original_expressions.push_back(expr->Copy());
	}
	result->column_count = statement.select_list.size();

	// first visit the WHERE clause
	// the WHERE clause happens before the GROUP BY, PROJECTION or HAVING clauses
	if (statement.where_clause) {
		result->where_clause = BindFilter(move(statement.where_clause));
	}

	// now bind all the result modifiers; including DISTINCT and ORDER BY targets
	OrderBinder order_binder({this}, result->projection_index, statement, alias_map, projection_map);
	BindModifiers(order_binder, statement, *result);

	vector<unique_ptr<ParsedExpression>> unbound_groups;
	BoundGroupInformation info;
	if (statement.groups.size() > 0) {
		// the statement has a GROUP BY clause, bind it
		unbound_groups.resize(statement.groups.size());
		GroupBinder group_binder(*this, context, statement, result->group_index, alias_map, info.alias_map);
		for (idx_t i = 0; i < statement.groups.size(); i++) {

			// we keep a copy of the unbound expression;
			// we keep the unbound copy around to check for group references in the SELECT and HAVING clause
			// the reason we want the unbound copy is because we want to figure out whether an expression
			// is a group reference BEFORE binding in the SELECT/HAVING binder
			group_binder.unbound_expression = statement.groups[i]->Copy();
			group_binder.bind_index = i;

			// bind the groups
			SQLType group_type;
			auto bound_expr = group_binder.Bind(statement.groups[i], &group_type);
			assert(bound_expr->return_type != TypeId::INVALID);
			info.group_types.push_back(group_type);
			// push a potential collation, if necessary
			bound_expr = ExpressionBinder::PushCollation(context, move(bound_expr), group_type.collation, true);
			result->groups.push_back(move(bound_expr));

			// in the unbound expression we DO bind the table names of any ColumnRefs
			// we do this to make sure that "table.a" and "a" are treated the same
			// if we wouldn't do this then (SELECT test.a FROM test GROUP BY a) would not work because "test.a" <> "a"
			// hence we convert "a" -> "test.a" in the unbound expression
			unbound_groups[i] = move(group_binder.unbound_expression);
			ExpressionBinder::BindTableNames(*this, *unbound_groups[i]);
			info.map[unbound_groups[i].get()] = i;
		}
	}

	// bind the HAVING clause, if any
	if (statement.having) {
		HavingBinder having_binder(*this, context, *result, info);
		ExpressionBinder::BindTableNames(*this, *statement.having);
		result->having = having_binder.Bind(statement.having);
	}

	// after that, we bind to the SELECT list
	SelectBinder select_binder(*this, context, *result, info);
	vector<SQLType> internal_sql_types;
	for (idx_t i = 0; i < statement.select_list.size(); i++) {
		SQLType result_type;
		auto expr = select_binder.Bind(statement.select_list[i], &result_type);
		if (statement.aggregate_handling == AggregateHandling::FORCE_AGGREGATES && select_binder.BoundColumns()) {
			if (select_binder.BoundAggregates()) {
				throw BinderException("Cannot mix aggregates with non-aggregated columns!");
			}
			// we are forcing aggregates, and the node has columns bound
			// this entry becomes a group
			auto group_type = expr->return_type;
			auto group_ref = make_unique<BoundColumnRefExpression>(
			    group_type, ColumnBinding(result->group_index, result->groups.size()));
			result->groups.push_back(move(expr));
			expr = move(group_ref);
		}
		result->select_list.push_back(move(expr));
		if (i < result->column_count) {
			result->types.push_back(result_type);
		}
		internal_sql_types.push_back(result_type);
		if (statement.aggregate_handling == AggregateHandling::FORCE_AGGREGATES) {
			select_binder.ResetBindings();
		}
	}
	result->need_prune = result->select_list.size() > result->column_count;

	// in the normal select binder, we bind columns as if there is no aggregation
	// i.e. in the query [SELECT i, SUM(i) FROM integers;] the "i" will be bound as a normal column
	// since we have an aggregation, we need to either (1) throw an error, or (2) wrap the column in a FIRST() aggregate
	// we choose the former one [CONTROVERSIAL: this is the PostgreSQL behavior]
	if (result->groups.size() > 0 || result->aggregates.size() > 0 || statement.having) {
		if (statement.aggregate_handling == AggregateHandling::NO_AGGREGATES_ALLOWED) {
			throw BinderException("Aggregates cannot be present in a Project relation!");
		} else if (statement.aggregate_handling == AggregateHandling::STANDARD_HANDLING) {
			if (select_binder.BoundColumns()) {
				throw BinderException("column must appear in the GROUP BY clause or be used in an aggregate function");
			}
		}
	}

	// now that the SELECT list is bound, we set the types of DISTINCT/ORDER BY expressions
	BindModifierTypes(*result, internal_sql_types, result->projection_index);
	return move(result);
}

} // namespace duckdb
