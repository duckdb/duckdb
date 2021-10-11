#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/expression_binder/group_binder.hpp"
#include "duckdb/planner/expression_binder/having_binder.hpp"
#include "duckdb/planner/expression_binder/order_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"

namespace duckdb {
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
	D_ASSERT(bound_expr->type == ExpressionType::BOUND_COLUMN_REF);
	return bound_expr;
}

unique_ptr<Expression> Binder::BindDelimiter(ClientContext &context, unique_ptr<ParsedExpression> delimiter,
                                             int64_t &delimiter_value) {
	auto new_binder = Binder::CreateBinder(context, this, true);
	ExpressionBinder expr_binder(*new_binder, context);
	expr_binder.target_type = LogicalType::UBIGINT;
	auto expr = expr_binder.Bind(delimiter);
	if (expr->IsFoldable()) {
		//! this is a constant
		Value value = ExpressionExecutor::EvaluateScalar(*expr).CastAs(LogicalType::BIGINT);
		delimiter_value = value.GetValue<int64_t>();
		return nullptr;
	}
	return expr;
}

unique_ptr<BoundResultModifier> Binder::BindLimit(LimitModifier &limit_mod) {
	auto result = make_unique<BoundLimitModifier>();
	if (limit_mod.limit) {
		result->limit = BindDelimiter(context, move(limit_mod.limit), result->limit_val);
	}
	if (limit_mod.offset) {
		result->offset = BindDelimiter(context, move(limit_mod.offset), result->offset_val);
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
			for (auto &distinct_on_target : distinct.distinct_on_targets) {
				auto expr = BindOrderExpression(order_binder, move(distinct_on_target));
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
			auto &config = DBConfig::GetConfig(context);
			for (auto &order_node : order.orders) {
				auto order_expression = BindOrderExpression(order_binder, move(order_node.expression));
				if (!order_expression) {
					continue;
				}
				auto type = order_node.type == OrderType::ORDER_DEFAULT ? config.default_order_type : order_node.type;
				auto null_order = order_node.null_order == OrderByNullType::ORDER_DEFAULT ? config.default_null_order
				                                                                          : order_node.null_order;
				bound_order->orders.emplace_back(type, null_order, move(order_expression));
			}
			if (!bound_order->orders.empty()) {
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

void Binder::BindModifierTypes(BoundQueryNode &result, const vector<LogicalType> &sql_types, idx_t projection_index) {
	for (auto &bound_mod : result.modifiers) {
		switch (bound_mod->type) {
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct = (BoundDistinctModifier &)*bound_mod;
			if (distinct.target_distincts.empty()) {
				// DISTINCT without a target: push references to the standard select list
				for (idx_t i = 0; i < sql_types.size(); i++) {
					distinct.target_distincts.push_back(
					    make_unique<BoundColumnRefExpression>(sql_types[i], ColumnBinding(projection_index, i)));
				}
			} else {
				// DISTINCT with target list: set types
				for (auto &expr : distinct.target_distincts) {
					D_ASSERT(expr->type == ExpressionType::BOUND_COLUMN_REF);
					auto &bound_colref = (BoundColumnRefExpression &)*expr;
					if (bound_colref.binding.column_index == INVALID_INDEX) {
						throw BinderException("Ambiguous name in DISTINCT ON!");
					}
					D_ASSERT(bound_colref.binding.column_index < sql_types.size());
					bound_colref.return_type = sql_types[bound_colref.binding.column_index];
				}
			}
			for (auto &target_distinct : distinct.target_distincts) {
				auto &bound_colref = (BoundColumnRefExpression &)*target_distinct;
				auto sql_type = sql_types[bound_colref.binding.column_index];
				if (sql_type.id() == LogicalTypeId::VARCHAR) {
					target_distinct = ExpressionBinder::PushCollation(context, move(target_distinct),
					                                                  StringType::GetCollation(sql_type), true);
				}
			}
			break;
		}
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order = (BoundOrderModifier &)*bound_mod;
			for (auto &order_node : order.orders) {
				auto &expr = order_node.expression;
				D_ASSERT(expr->type == ExpressionType::BOUND_COLUMN_REF);
				auto &bound_colref = (BoundColumnRefExpression &)*expr;
				if (bound_colref.binding.column_index == INVALID_INDEX) {
					throw BinderException("Ambiguous name in ORDER BY!");
				}
				D_ASSERT(bound_colref.binding.column_index < sql_types.size());
				auto sql_type = sql_types[bound_colref.binding.column_index];
				bound_colref.return_type = sql_types[bound_colref.binding.column_index];
				if (sql_type.id() == LogicalTypeId::VARCHAR) {
					order_node.expression = ExpressionBinder::PushCollation(context, move(order_node.expression),
					                                                        StringType::GetCollation(sql_type));
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

	// bind the sample clause
	if (statement.sample) {
		result->sample_options = move(statement.sample);
	}

	// visit the select list and expand any "*" statements
	vector<unique_ptr<ParsedExpression>> new_select_list;
	for (auto &select_element : statement.select_list) {
		if (select_element->GetExpressionType() == ExpressionType::STAR) {
			// * statement, expand to all columns from the FROM clause
			bind_context.GenerateAllColumnExpressions((StarExpression &)*select_element, new_select_list);
		} else {
			// regular statement, add it to the list
			new_select_list.push_back(move(select_element));
		}
	}
	if (new_select_list.empty()) {
		throw BinderException("SELECT list is empty after resolving * expressions!");
	}
	statement.select_list = move(new_select_list);

	// create a mapping of (alias -> index) and a mapping of (Expression -> index) for the SELECT list
	unordered_map<string, idx_t> alias_map;
	expression_map_t<idx_t> projection_map;
	for (idx_t i = 0; i < statement.select_list.size(); i++) {
		auto &expr = statement.select_list[i];
		result->names.push_back(expr->GetName());
		ExpressionBinder::BindTableNames(*this, *expr);
		if (!expr->alias.empty()) {
			alias_map[expr->alias] = i;
			result->names[i] = expr->alias;
		}
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
	auto &group_expressions = statement.groups.group_expressions;
	if (!group_expressions.empty()) {
		// the statement has a GROUP BY clause, bind it
		unbound_groups.resize(group_expressions.size());
		GroupBinder group_binder(*this, context, statement, result->group_index, alias_map, info.alias_map);
		for (idx_t i = 0; i < group_expressions.size(); i++) {

			// we keep a copy of the unbound expression;
			// we keep the unbound copy around to check for group references in the SELECT and HAVING clause
			// the reason we want the unbound copy is because we want to figure out whether an expression
			// is a group reference BEFORE binding in the SELECT/HAVING binder
			group_binder.unbound_expression = group_expressions[i]->Copy();
			group_binder.bind_index = i;

			// bind the groups
			LogicalType group_type;
			auto bound_expr = group_binder.Bind(group_expressions[i], &group_type);
			D_ASSERT(bound_expr->return_type.id() != LogicalTypeId::INVALID);

			// push a potential collation, if necessary
			bound_expr =
			    ExpressionBinder::PushCollation(context, move(bound_expr), StringType::GetCollation(group_type), true);
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
		HavingBinder having_binder(*this, context, *result, info, alias_map);
		ExpressionBinder::BindTableNames(*this, *statement.having, &alias_map);
		result->having = having_binder.Bind(statement.having);
	}

	// after that, we bind to the SELECT list
	SelectBinder select_binder(*this, context, *result, info);
	vector<LogicalType> internal_sql_types;
	for (idx_t i = 0; i < statement.select_list.size(); i++) {
		LogicalType result_type;
		auto expr = select_binder.Bind(statement.select_list[i], &result_type);
		if (statement.aggregate_handling == AggregateHandling::FORCE_AGGREGATES && select_binder.HasBoundColumns()) {
			if (select_binder.BoundAggregates()) {
				throw BinderException("Cannot mix aggregates with non-aggregated columns!");
			}
			// we are forcing aggregates, and the node has columns bound
			// this entry becomes a group
			auto group_ref = make_unique<BoundColumnRefExpression>(
			    expr->return_type, ColumnBinding(result->group_index, result->groups.size()));
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
	if (!result->groups.empty() || !result->aggregates.empty() || statement.having) {
		if (statement.aggregate_handling == AggregateHandling::NO_AGGREGATES_ALLOWED) {
			throw BinderException("Aggregates cannot be present in a Project relation!");
		} else if (statement.aggregate_handling == AggregateHandling::STANDARD_HANDLING) {
			if (select_binder.HasBoundColumns()) {
				auto &bound_columns = select_binder.GetBoundColumns();
				throw BinderException(
				    FormatError(bound_columns[0].query_location,
				                "column \"%s\" must appear in the GROUP BY clause or be used in an aggregate function",
				                bound_columns[0].name));
			}
		}
	}

	// now that the SELECT list is bound, we set the types of DISTINCT/ORDER BY expressions
	BindModifierTypes(*result, internal_sql_types, result->projection_index);
	return move(result);
}

} // namespace duckdb
