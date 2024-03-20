#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression_binder/column_alias_binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/expression_binder/group_binder.hpp"
#include "duckdb/planner/expression_binder/having_binder.hpp"
#include "duckdb/planner/expression_binder/order_binder.hpp"
#include "duckdb/planner/expression_binder/qualify_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

unique_ptr<Expression> Binder::BindOrderExpression(OrderBinder &order_binder, unique_ptr<ParsedExpression> expr) {
	// we treat the distinct list as an ORDER BY
	auto bound_expr = order_binder.Bind(std::move(expr));
	if (!bound_expr) {
		// DISTINCT ON non-integer constant
		// remove the expression from the DISTINCT ON list
		return nullptr;
	}
	D_ASSERT(bound_expr->type == ExpressionType::BOUND_COLUMN_REF);
	return bound_expr;
}

BoundLimitNode Binder::BindLimitValue(OrderBinder &order_binder, unique_ptr<ParsedExpression> limit_val,
                                      bool is_percentage, bool is_offset) {
	auto new_binder = Binder::CreateBinder(context, this, true);
	if (limit_val->HasSubquery()) {
		if (!order_binder.HasExtraList()) {
			throw BinderException("Subquery in LIMIT/OFFSET not supported in set operation");
		}
		auto bound_limit = order_binder.CreateExtraReference(std::move(limit_val));
		if (is_percentage) {
			return BoundLimitNode::ExpressionPercentage(std::move(bound_limit));
		} else {
			return BoundLimitNode::ExpressionValue(std::move(bound_limit));
		}
	}
	ExpressionBinder expr_binder(*new_binder, context);
	auto target_type = is_percentage ? LogicalType::DOUBLE : LogicalType::BIGINT;
	;
	expr_binder.target_type = target_type;
	auto expr = expr_binder.Bind(limit_val);
	if (expr->IsFoldable()) {
		//! this is a constant
		auto val = ExpressionExecutor::EvaluateScalar(context, *expr).CastAs(context, target_type);
		if (is_percentage) {
			D_ASSERT(!is_offset);
			double percentage_val;
			if (val.IsNull()) {
				percentage_val = 100.0;
			} else {
				percentage_val = val.GetValue<double>();
			}
			if (Value::IsNan(percentage_val) || percentage_val < 0 || percentage_val > 100) {
				throw OutOfRangeException("Limit percent out of range, should be between 0% and 100%");
			}
			return BoundLimitNode::ConstantPercentage(percentage_val);
		} else {
			int64_t constant_val;
			if (val.IsNull()) {
				constant_val = is_offset ? 0 : NumericLimits<int64_t>::Maximum();
			} else {
				constant_val = val.GetValue<int64_t>();
			}
			if (constant_val < 0) {
				throw BinderException(expr->query_location, "LIMIT/OFFSET cannot be negative");
			}
			return BoundLimitNode::ConstantValue(constant_val);
		}
	}
	if (!new_binder->correlated_columns.empty()) {
		throw BinderException("Correlated columns not supported in LIMIT/OFFSET");
	}
	// move any correlated columns to this binder
	MoveCorrelatedExpressions(*new_binder);
	if (is_percentage) {
		return BoundLimitNode::ExpressionPercentage(std::move(expr));
	} else {
		return BoundLimitNode::ExpressionValue(std::move(expr));
	}
}

duckdb::unique_ptr<BoundResultModifier> Binder::BindLimit(OrderBinder &order_binder, LimitModifier &limit_mod) {
	auto result = make_uniq<BoundLimitModifier>();
	if (limit_mod.limit) {
		result->limit_val = BindLimitValue(order_binder, std::move(limit_mod.limit), false, false);
	}
	if (limit_mod.offset) {
		result->offset_val = BindLimitValue(order_binder, std::move(limit_mod.offset), false, true);
	}
	return std::move(result);
}

unique_ptr<BoundResultModifier> Binder::BindLimitPercent(OrderBinder &order_binder, LimitPercentModifier &limit_mod) {
	auto result = make_uniq<BoundLimitModifier>();
	if (limit_mod.limit) {
		result->limit_val = BindLimitValue(order_binder, std::move(limit_mod.limit), true, false);
	}
	if (limit_mod.offset) {
		result->offset_val = BindLimitValue(order_binder, std::move(limit_mod.offset), false, true);
	}
	return std::move(result);
}

void Binder::BindModifiers(OrderBinder &order_binder, QueryNode &statement, BoundQueryNode &result) {
	for (auto &mod : statement.modifiers) {
		unique_ptr<BoundResultModifier> bound_modifier;
		switch (mod->type) {
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct = mod->Cast<DistinctModifier>();
			auto bound_distinct = make_uniq<BoundDistinctModifier>();
			bound_distinct->distinct_type =
			    distinct.distinct_on_targets.empty() ? DistinctType::DISTINCT : DistinctType::DISTINCT_ON;
			if (distinct.distinct_on_targets.empty()) {
				for (idx_t i = 0; i < result.names.size(); i++) {
					distinct.distinct_on_targets.push_back(
					    make_uniq<ConstantExpression>(Value::INTEGER(UnsafeNumericCast<int32_t>(1 + i))));
				}
			}
			for (auto &distinct_on_target : distinct.distinct_on_targets) {
				auto expr = BindOrderExpression(order_binder, std::move(distinct_on_target));
				if (!expr) {
					continue;
				}
				bound_distinct->target_distincts.push_back(std::move(expr));
			}
			bound_modifier = std::move(bound_distinct);
			break;
		}
		case ResultModifierType::ORDER_MODIFIER: {
			auto &order = mod->Cast<OrderModifier>();
			auto bound_order = make_uniq<BoundOrderModifier>();
			auto &config = DBConfig::GetConfig(context);
			D_ASSERT(!order.orders.empty());
			auto &order_binders = order_binder.GetBinders();
			if (order.orders.size() == 1 && order.orders[0].expression->type == ExpressionType::STAR) {
				auto &star = order.orders[0].expression->Cast<StarExpression>();
				if (star.exclude_list.empty() && star.replace_list.empty() && !star.expr) {
					// ORDER BY ALL
					// replace the order list with the all elements in the SELECT list
					auto order_type = order.orders[0].type;
					auto null_order = order.orders[0].null_order;

					vector<OrderByNode> new_orders;
					for (idx_t i = 0; i < order_binder.MaxCount(); i++) {
						new_orders.emplace_back(
						    order_type, null_order,
						    make_uniq<ConstantExpression>(Value::INTEGER(UnsafeNumericCast<int32_t>(i + 1))));
					}
					order.orders = std::move(new_orders);
				}
			}
#if 0
			// When this verification is enabled, replace ORDER BY x, y with ORDER BY create_sort_key(x, y)
			// note that we don't enable this during actual verification since it doesn't always work
			// e.g. it breaks EXPLAIN output on queries
			bool can_replace = true;
			for (auto &order_node : order.orders) {
				if (order_node.expression->type == ExpressionType::VALUE_CONSTANT) {
					// we cannot replace the sort key when we order by literals (e.g. ORDER BY 1, 2`
					can_replace = false;
					break;
				}
			}
			if (!order_binder.HasExtraList()) {
				// we can only do the replacement when we can order by elements that are not in the selection list
				can_replace = false;
			}
			if (can_replace) {
				vector<unique_ptr<ParsedExpression>> sort_key_parameters;
				for (auto &order_node : order.orders) {
					sort_key_parameters.push_back(std::move(order_node.expression));
					auto type = config.ResolveOrder(order_node.type);
					auto null_order = config.ResolveNullOrder(type, order_node.null_order);
					string sort_param = EnumUtil::ToString(type) + " " + EnumUtil::ToString(null_order);
					sort_key_parameters.push_back(make_uniq<ConstantExpression>(Value(sort_param)));
				}
				order.orders.clear();
				auto create_sort_key = make_uniq<FunctionExpression>("create_sort_key", std::move(sort_key_parameters));
				order.orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, std::move(create_sort_key));
			}
#endif
			for (auto &order_node : order.orders) {
				vector<unique_ptr<ParsedExpression>> order_list;
				order_binders[0]->ExpandStarExpression(std::move(order_node.expression), order_list);

				auto type = config.ResolveOrder(order_node.type);
				auto null_order = config.ResolveNullOrder(type, order_node.null_order);
				for (auto &order_expr : order_list) {
					auto bound_expr = BindOrderExpression(order_binder, std::move(order_expr));
					if (!bound_expr) {
						continue;
					}
					bound_order->orders.emplace_back(type, null_order, std::move(bound_expr));
				}
			}
			if (!bound_order->orders.empty()) {
				bound_modifier = std::move(bound_order);
			}
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER:
			bound_modifier = BindLimit(order_binder, mod->Cast<LimitModifier>());
			break;
		case ResultModifierType::LIMIT_PERCENT_MODIFIER:
			bound_modifier = BindLimitPercent(order_binder, mod->Cast<LimitPercentModifier>());
			break;
		default:
			throw InternalException("Unsupported result modifier");
		}
		if (bound_modifier) {
			result.modifiers.push_back(std::move(bound_modifier));
		}
	}
}

static void AssignReturnType(unique_ptr<Expression> &expr, const vector<LogicalType> &sql_types) {
	if (!expr) {
		return;
	}
	if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
		return;
	}
	auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
	bound_colref.return_type = sql_types[bound_colref.binding.column_index];
}

void Binder::BindModifierTypes(BoundQueryNode &result, const vector<LogicalType> &sql_types, idx_t,
                               const vector<idx_t> &expansion_count) {
	for (auto &bound_mod : result.modifiers) {
		switch (bound_mod->type) {
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct = bound_mod->Cast<BoundDistinctModifier>();
			D_ASSERT(!distinct.target_distincts.empty());
			// set types of distinct targets
			for (auto &expr : distinct.target_distincts) {
				D_ASSERT(expr->type == ExpressionType::BOUND_COLUMN_REF);
				auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
				if (bound_colref.binding.column_index == DConstants::INVALID_INDEX) {
					throw BinderException("Ambiguous name in DISTINCT ON!");
				}

				idx_t max_count = sql_types.size();
				if (bound_colref.binding.column_index > max_count - 1) {
					D_ASSERT(bound_colref.return_type == LogicalType::ANY);
					throw BinderException("ORDER term out of range - should be between 1 and %lld", max_count);
				}

				bound_colref.return_type = sql_types[bound_colref.binding.column_index];
			}
			for (auto &target_distinct : distinct.target_distincts) {
				auto &bound_colref = target_distinct->Cast<BoundColumnRefExpression>();
				const auto &sql_type = sql_types[bound_colref.binding.column_index];
				ExpressionBinder::PushCollation(context, target_distinct, sql_type, true);
			}
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit = bound_mod->Cast<BoundLimitModifier>();
			AssignReturnType(limit.limit_val.GetExpression(), sql_types);
			AssignReturnType(limit.offset_val.GetExpression(), sql_types);
			break;
		}
		case ResultModifierType::ORDER_MODIFIER: {

			auto &order = bound_mod->Cast<BoundOrderModifier>();
			for (auto &order_node : order.orders) {

				auto &expr = order_node.expression;
				D_ASSERT(expr->type == ExpressionType::BOUND_COLUMN_REF);
				auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
				if (bound_colref.binding.column_index == DConstants::INVALID_INDEX) {
					throw BinderException("Ambiguous name in ORDER BY!");
				}

				if (!expansion_count.empty() && bound_colref.return_type.id() != LogicalTypeId::ANY) {
					bound_colref.binding.column_index = expansion_count[bound_colref.binding.column_index];
				}

				idx_t max_count = sql_types.size();
				if (bound_colref.binding.column_index > max_count - 1) {
					D_ASSERT(bound_colref.return_type == LogicalType::ANY);
					throw BinderException("ORDER term out of range - should be between 1 and %lld", max_count);
				}

				const auto &sql_type = sql_types[bound_colref.binding.column_index];
				bound_colref.return_type = sql_type;

				ExpressionBinder::PushCollation(context, order_node.expression, sql_type);
			}
			break;
		}
		default:
			break;
		}
	}
}

unique_ptr<BoundQueryNode> Binder::BindNode(SelectNode &statement) {
	D_ASSERT(statement.from_table);
	// first bind the FROM table statement
	auto from = std::move(statement.from_table);
	auto from_table = Bind(*from);
	return BindSelectNode(statement, std::move(from_table));
}

void Binder::BindWhereStarExpression(unique_ptr<ParsedExpression> &expr) {
	// expand any expressions in the upper AND recursively
	if (expr->type == ExpressionType::CONJUNCTION_AND) {
		auto &conj = expr->Cast<ConjunctionExpression>();
		for (auto &child : conj.children) {
			BindWhereStarExpression(child);
		}
		return;
	}
	if (expr->type == ExpressionType::STAR) {
		auto &star = expr->Cast<StarExpression>();
		if (!star.columns) {
			throw ParserException("STAR expression is not allowed in the WHERE clause. Use COLUMNS(*) instead.");
		}
	}
	// expand the stars for this expression
	vector<unique_ptr<ParsedExpression>> new_conditions;
	ExpandStarExpression(std::move(expr), new_conditions);
	if (new_conditions.empty()) {
		throw ParserException("COLUMNS expansion resulted in empty set of columns");
	}

	// set up an AND conjunction between the expanded conditions
	expr = std::move(new_conditions[0]);
	for (idx_t i = 1; i < new_conditions.size(); i++) {
		auto and_conj = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(expr),
		                                                 std::move(new_conditions[i]));
		expr = std::move(and_conj);
	}
}

unique_ptr<BoundQueryNode> Binder::BindSelectNode(SelectNode &statement, unique_ptr<BoundTableRef> from_table) {
	D_ASSERT(from_table);
	D_ASSERT(!statement.from_table);
	auto result = make_uniq<BoundSelectNode>();
	result->projection_index = GenerateTableIndex();
	result->group_index = GenerateTableIndex();
	result->aggregate_index = GenerateTableIndex();
	result->groupings_index = GenerateTableIndex();
	result->window_index = GenerateTableIndex();
	result->prune_index = GenerateTableIndex();

	result->from_table = std::move(from_table);
	// bind the sample clause
	if (statement.sample) {
		result->sample_options = std::move(statement.sample);
	}

	// visit the select list and expand any "*" statements
	vector<unique_ptr<ParsedExpression>> new_select_list;
	ExpandStarExpressions(statement.select_list, new_select_list);

	if (new_select_list.empty()) {
		throw BinderException("SELECT list is empty after resolving * expressions!");
	}
	statement.select_list = std::move(new_select_list);

	// create a mapping of (alias -> index) and a mapping of (Expression -> index) for the SELECT list
	case_insensitive_map_t<idx_t> alias_map;
	parsed_expression_map_t<idx_t> projection_map;
	for (idx_t i = 0; i < statement.select_list.size(); i++) {
		auto &expr = statement.select_list[i];
		result->names.push_back(expr->GetName());
		ExpressionBinder::QualifyColumnNames(*this, expr);
		if (!expr->alias.empty()) {
			alias_map[expr->alias] = i;
			result->names[i] = expr->alias;
		}
		projection_map[*expr] = i;
		result->original_expressions.push_back(expr->Copy());
	}
	result->column_count = statement.select_list.size();

	// first visit the WHERE clause
	// the WHERE clause happens before the GROUP BY, PROJECTION or HAVING clauses
	if (statement.where_clause) {
		// bind any star expressions in the WHERE clause
		BindWhereStarExpression(statement.where_clause);

		ColumnAliasBinder alias_binder(*result, alias_map);
		WhereBinder where_binder(*this, context, &alias_binder);
		unique_ptr<ParsedExpression> condition = std::move(statement.where_clause);
		result->where_clause = where_binder.Bind(condition);
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

			// find out whether the expression contains a subquery, it can't be copied if so
			auto &bound_expr_ref = *bound_expr;
			bool contains_subquery = bound_expr_ref.HasSubquery();

			// push a potential collation, if necessary
			bool requires_collation = ExpressionBinder::PushCollation(context, bound_expr, group_type, true);
			if (!contains_subquery && requires_collation) {
				// if there is a collation on a group x, we should group by the collated expr,
				// but also push a first(x) aggregate in case x is selected (uncollated)
				info.collated_groups[i] = result->aggregates.size();

				auto first_fun = FirstFun::GetFunction(LogicalType::VARCHAR);
				vector<unique_ptr<Expression>> first_children;
				// FIXME: would be better to just refer to this expression, but for now we copy
				first_children.push_back(bound_expr_ref.Copy());

				FunctionBinder function_binder(context);
				auto function = function_binder.BindAggregateFunction(first_fun, std::move(first_children));
				result->aggregates.push_back(std::move(function));
			}
			result->groups.group_expressions.push_back(std::move(bound_expr));

			// in the unbound expression we DO bind the table names of any ColumnRefs
			// we do this to make sure that "table.a" and "a" are treated the same
			// if we wouldn't do this then (SELECT test.a FROM test GROUP BY a) would not work because "test.a" <> "a"
			// hence we convert "a" -> "test.a" in the unbound expression
			unbound_groups[i] = std::move(group_binder.unbound_expression);
			ExpressionBinder::QualifyColumnNames(*this, unbound_groups[i]);
			info.map[*unbound_groups[i]] = i;
		}
	}
	result->groups.grouping_sets = std::move(statement.groups.grouping_sets);

	// bind the HAVING clause, if any
	if (statement.having) {
		HavingBinder having_binder(*this, context, *result, info, alias_map, statement.aggregate_handling);
		ExpressionBinder::QualifyColumnNames(*this, statement.having);
		result->having = having_binder.Bind(statement.having);
	}

	// bind the QUALIFY clause, if any
	unique_ptr<QualifyBinder> qualify_binder;
	if (statement.qualify) {
		if (statement.aggregate_handling == AggregateHandling::FORCE_AGGREGATES) {
			throw BinderException("Combining QUALIFY with GROUP BY ALL is not supported yet");
		}
		qualify_binder = make_uniq<QualifyBinder>(*this, context, *result, info, alias_map);
		ExpressionBinder::QualifyColumnNames(*this, statement.qualify);
		result->qualify = qualify_binder->Bind(statement.qualify);
		if (qualify_binder->HasBoundColumns() && qualify_binder->BoundAggregates()) {
			throw BinderException("Cannot mix aggregates with non-aggregated columns!");
		}
	}

	// after that, we bind to the SELECT list
	SelectBinder select_binder(*this, context, *result, info, alias_map);

	// if we expand select-list expressions, e.g., via UNNEST, then we need to possibly
	// adjust the column index of the already bound ORDER BY modifiers, and not only set their types
	vector<LogicalType> modifier_sql_types;
	vector<idx_t> modifier_expansion_count;

	vector<idx_t> group_by_all_indexes;
	vector<string> new_names;

	for (idx_t i = 0; i < statement.select_list.size(); i++) {
		bool is_window = statement.select_list[i]->IsWindow();
		idx_t unnest_count = result->unnests.size();
		LogicalType result_type;
		auto expr = select_binder.Bind(statement.select_list[i], &result_type, true);
		bool is_original_column = i < result->column_count;
		bool can_group_by_all =
		    statement.aggregate_handling == AggregateHandling::FORCE_AGGREGATES && is_original_column;

		if (select_binder.HasExpandedExpressions()) {
			if (!is_original_column) {
				throw InternalException("Only original columns can have expanded expressions");
			}
			if (statement.aggregate_handling == AggregateHandling::FORCE_AGGREGATES) {
				throw BinderException("UNNEST of struct cannot be combined with GROUP BY ALL");
			}

			auto &struct_expressions = select_binder.ExpandedExpressions();
			D_ASSERT(!struct_expressions.empty());
			modifier_expansion_count.push_back(modifier_sql_types.size());

			for (auto &struct_expr : struct_expressions) {
				modifier_sql_types.push_back(struct_expr->return_type);
				new_names.push_back(struct_expr->GetName());
				result->types.push_back(struct_expr->return_type);
				result->select_list.push_back(std::move(struct_expr));
			}

			struct_expressions.clear();
			continue;
		}

		// not an expanded expression
		modifier_expansion_count.push_back(modifier_sql_types.size());
		modifier_sql_types.push_back(result_type);

		if (can_group_by_all && select_binder.HasBoundColumns()) {
			if (select_binder.BoundAggregates()) {
				throw BinderException("Cannot mix aggregates with non-aggregated columns!");
			}
			if (is_window) {
				throw BinderException("Cannot group on a window clause");
			}
			if (result->unnests.size() > unnest_count) {
				throw BinderException("Cannot group on an UNNEST or UNLIST clause");
			}
			// we are forcing aggregates, and the node has columns bound
			// this entry becomes a group
			group_by_all_indexes.push_back(i);
		}

		result->select_list.push_back(std::move(expr));
		if (is_original_column) {
			new_names.push_back(std::move(result->names[i]));
			result->types.push_back(result_type);
		}

		if (can_group_by_all) {
			select_binder.ResetBindings();
		}
	}

	// push the GROUP BY ALL expressions into the group set
	for (auto &group_by_all_index : group_by_all_indexes) {
		auto &expr = result->select_list[group_by_all_index];
		auto group_ref = make_uniq<BoundColumnRefExpression>(
		    expr->return_type, ColumnBinding(result->group_index, result->groups.group_expressions.size()));
		result->groups.group_expressions.push_back(std::move(expr));
		expr = std::move(group_ref);
	}
	result->column_count = new_names.size();
	result->names = std::move(new_names);
	result->need_prune = result->select_list.size() > result->column_count;

	// in the normal select binder, we bind columns as if there is no aggregation
	// i.e. in the query [SELECT i, SUM(i) FROM integers;] the "i" will be bound as a normal column
	// since we have an aggregation, we need to either (1) throw an error, or (2) wrap the column in a FIRST() aggregate
	// we choose the former one [CONTROVERSIAL: this is the PostgreSQL behavior]
	if (!result->groups.group_expressions.empty() || !result->aggregates.empty() || statement.having ||
	    !result->groups.grouping_sets.empty()) {
		if (statement.aggregate_handling == AggregateHandling::NO_AGGREGATES_ALLOWED) {
			throw BinderException("Aggregates cannot be present in a Project relation!");
		} else {
			vector<reference<BaseSelectBinder>> to_check_binders;
			to_check_binders.push_back(select_binder);
			if (qualify_binder) {
				to_check_binders.push_back(*qualify_binder);
			}
			for (auto &binder : to_check_binders) {
				auto &sel_binder = binder.get();
				if (!sel_binder.HasBoundColumns()) {
					continue;
				}
				auto &bound_columns = sel_binder.GetBoundColumns();
				string error;
				error = "column \"%s\" must appear in the GROUP BY clause or must be part of an aggregate function.";
				if (statement.aggregate_handling == AggregateHandling::FORCE_AGGREGATES) {
					error += "\nGROUP BY ALL will only group entries in the SELECT list. Add it to the SELECT list or "
					         "GROUP BY this entry explicitly.";
					throw BinderException(bound_columns[0].query_location, error, bound_columns[0].name);
				} else {
					error +=
					    "\nEither add it to the GROUP BY list, or use \"ANY_VALUE(%s)\" if the exact value of \"%s\" "
					    "is not important.";
					throw BinderException(bound_columns[0].query_location, error, bound_columns[0].name,
					                      bound_columns[0].name, bound_columns[0].name);
				}
			}
		}
	}

	// QUALIFY clause requires at least one window function to be specified in at least one of the SELECT column list or
	// the filter predicate of the QUALIFY clause
	if (statement.qualify && result->windows.empty()) {
		throw BinderException("at least one window function must appear in the SELECT column or QUALIFY clause");
	}

	// now that the SELECT list is bound, we set the types of DISTINCT/ORDER BY expressions
	BindModifierTypes(*result, modifier_sql_types, result->projection_index, modifier_expansion_count);
	return std::move(result);
}

} // namespace duckdb
