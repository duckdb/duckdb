#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/bound_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/query_node/select_node.hpp"
#include "planner/binder.hpp"


#include <unordered_set>

using namespace duckdb;
using namespace std;

void Binder::Bind(SelectNode &statement) {
	// first visit the FROM table statement
	if (statement.from_table) {
		AcceptChild(&statement.from_table);
	}
	auto &binding = statement.binding;

	// visit the select list and expand any "*" statements
	vector<unique_ptr<Expression>> new_select_list;
	for (auto &select_element : statement.select_list) {
		if (select_element->GetExpressionType() == ExpressionType::STAR) {
			// * statement, expand to all columns from the FROM clause
			bind_context->GenerateAllColumnExpressions(new_select_list);
		} else {
			// regular statement, add it to the list
			new_select_list.push_back(move(select_element));
		}
	}
	statement.select_list = move(new_select_list);

	binding.column_count = statement.select_list.size();
	binding.projection_index = bind_context->GenerateTableIndex();

	// first visit the WHERE clause
	// the WHERE clause happens before the GROUP BY, PROJECTION or HAVING clauses
	if (statement.where_clause) {
		if (statement.where_clause->IsAggregate()) {
			throw ParserException("WHERE clause cannot contain aggregates!");
		}
		if (statement.where_clause->IsWindow()) {
			throw ParserException("WHERE clause cannot contain window functions!");
		}
		VisitAndResolveType(&statement.where_clause);
	}

	// gather the list of aliases in the SELECT clause
	// these can be referenced in either the ORDER BY or the GROUP BY clauses
	unordered_map<string, uint32_t> alias_map;
	expression_map_t<uint32_t> projections;
	for(size_t i = 0; i < statement.select_list.size(); i++) {
		auto &expr = statement.select_list[i];
		if (!expr->alias.empty()) {
			alias_map[expr->alias] = i;
		}
		projections[expr.get()] = i;
	}

	vector<uint32_t> order_references;
	// first we resolve the ORDER BY clause, if any
	if (statement.HasOrder()) {
		// columns in the ORDER BY clause:
		// FIRST refer to the select clause aliases,
		// THEN if no match is found, refer to the original tables
		for (size_t i = 0; i < statement.orderby.orders.size(); i++) {
			auto &order = statement.orderby.orders[i].expression;
			// is the ORDER BY expression a constant integer? (e.g. ORDER BY 1)
			if (order->type == ExpressionType::VALUE_CONSTANT) {
				auto &constant = (ConstantExpression &)*order;
				// ORDER BY a constant
				if (!TypeIsIntegral(constant.value.type)) {
					// non-integral expression, we discard the constant ordering, as ORDER BY <constant> has no effect
					statement.orderby.orders.erase(statement.orderby.orders.begin() + i);
					i--;
					continue;
				}
				// INTEGER constant: we use the integer as an index into the select list (e.g. ORDER BY 1)
				auto index = constant.value.GetNumericValue();
				if (index < 1 || index > statement.select_list.size()) {
					throw BinderException("ORDER term out of range - should be between 1 and %d",
										(int)statement.select_list.size());
				}
				order_references.push_back(index - 1);
				continue;
			}
			// is the ORDER BY expression an alias reference?
			if (order->type == ExpressionType::COLUMN_REF) {
				auto &colref = (ColumnRefExpression&) *order;
				if (colref.table_name.empty()) {
					// no table name, check if it references an alias in the SELECT clause
					auto entry = alias_map.find(colref.column_name);
					if (entry != alias_map.end()) {
						// it does! point it to that entry
						order_references.push_back(entry->second);
						continue;
					}
				}
			}
			// does the ORDER BY clause already point to an entry in the projection list?
			auto entry = projections.find(order.get());
			if (entry != projections.end()) {
				// the ORDER BY clause found a matching entry in the projection list
				// just point to that entry
				order_references.push_back(entry->second);
				continue;
			}
			// otherwise, we need to push the ORDER BY entry into the select list
			order_references.push_back(statement.select_list.size());
			statement.select_list.push_back(move(order));
		}
	}
	assert(order_references.size() == statement.orderby.orders.size());
	
	if (statement.HasAggregation()) {
		expression_map_t<uint32_t> groups;
		// the statement has an aggregation
		// we need to create a LogicalAggregate to compute the aggregation
		binding.group_index = bind_context->GenerateTableIndex();
		binding.aggregate_index = bind_context->GenerateTableIndex();
		if (statement.HasGroup()) {
			// the statement has a GROUP BY clause
			// columns in GROUP BY clauses:
			// FIRST refer to the original tables, and
			// THEN if no match is found refer to aliases in the SELECT list

			// iterate over the groups and bind the columns
			unordered_set<uint32_t> bound_aliases;
			for(size_t i = 0; i < statement.groupby.groups.size(); i++) {
				if (statement.groupby.groups[i]->IsWindow()) {
					throw ParserException("WINDOW FUNCTIONS are not allowed in GROUP BY");
				}
				if (statement.groupby.groups[i]->IsAggregate()) {
					throw ParserException("AGGREGATE FUNCTIONS are not allowed in GROUP BY");
				}
				bool found_alias = false;
				if (statement.groupby.groups[i]->type == ExpressionType::COLUMN_REF) {
					auto &colref = (ColumnRefExpression&) *statement.groupby.groups[i];
					// the group is a column reference
					if (colref.table_name.empty()) {
						// there is no explicit table name: we can potentially bind to an alias
						// first check if it can be bound to a base table
						colref.table_name = bind_context->GetMatchingBinding(colref.column_name);
						if (colref.table_name.empty()) {
							// there was no base table we could bind to: check the set of aliases
							auto entry = alias_map.find(colref.column_name);
							if (entry == alias_map.end()) {
								throw BinderException("Referenced column \"%s\" not found in FROM clause or alias list!", colref.column_name.c_str());
							}
							// we successfully bound to an alias
							if (bound_aliases.find(entry->second) != bound_aliases.end()) {
								// however, the alias has already been bound to before!
								// this only happens if we group on the same alias twice (e.g. GROUP BY k, k)
								// in this case, we can just remove the entry as grouping on the same entry twice has no additional effect
								statement.groupby.groups.erase(statement.groupby.groups.begin() + i);
								i--;
								continue;
							}
							// in this case we have to move the computation of the GROUP BY column into this expression
							statement.groupby.groups[i] = move(statement.select_list[entry->second]);
							// now visit the GROUP BY expression and resolve its type
							VisitAndResolveType(&statement.groupby.groups[i]);
							// replace the entry in the select list with a reference to this group
							statement.select_list[entry->second] = make_unique<BoundColumnRefExpression>(*statement.groupby.groups[i], statement.groupby.groups[i]->return_type, ColumnBinding(binding.group_index, i));

							// add the entry to the set of bound aliases
							bound_aliases.insert(entry->second);
							found_alias = true;
						}
					}
				} 
				if (!found_alias) {
					// if we did not find an alias to bind, we bind to the tables in the FROM clause as usual
					VisitAndResolveType(&statement.groupby.groups[i]);
				}
				groups[statement.groupby.groups[i].get()] = i;
			}
		}
		// now we have to deal with aggregates and GROUP BY columns in the select list
		// we extract aggregates and grouping columns and replace them with BoundColumnRefExpressions that refer to them
		for (size_t i = 0; i < statement.select_list.size(); i++) {
			statement.select_list[i] = ExtractAggregatesAndGroups(move(statement.select_list[i]), statement, groups);
		}
		if (statement.HasHaving()) {
			// also extract aggregates and grouping columns from the HAVING clause
			if (statement.groupby.having->IsWindow()) {
				throw ParserException("WINDOW FUNCTIONS are not allowed in HAVING clause");
			}
			statement.groupby.having = ExtractAggregatesAndGroups(move(statement.groupby.having), statement, groups);
			VisitAndResolveType(&statement.groupby.having);
		}
	} else if (statement.HasHaving()) {
		throw ParserException("a GROUP BY clause is required before HAVING");
	}

	if (statement.HasWindow()) {
		// extract window functions from the select list
		binding.window_index = bind_context->GenerateTableIndex();
		for (size_t i = 0; i < statement.select_list.size(); i++) {
			statement.select_list[i] = ExtractWindowFunctions(move(statement.select_list[i]), binding.windows, binding.window_index);
		}
	}

	// visit the expressions in the select list and bind them
	for (size_t i = 0; i < statement.select_list.size(); i++) {
		auto &select_element = statement.select_list[i];
		VisitAndResolveType(&select_element);
		// add the output element of the SELECT list to the set of types
		statement.types.push_back(select_element->return_type);
	}

	// now we push expressions into the ORDER BY referencing the projection list
	for(size_t i = 0; i < order_references.size(); i++) {
		auto expression_index = order_references[i];
		assert(expression_index < statement.select_list.size());
		auto &referenced_expression = statement.select_list[expression_index];
		statement.orderby.orders[i].expression = make_unique<BoundColumnRefExpression>(*referenced_expression, referenced_expression->return_type, ColumnBinding(binding.projection_index, expression_index));
	}
}

unique_ptr<Expression> Binder::ExtractAggregatesAndGroups(unique_ptr<Expression> expr, SelectNode &node, expression_map_t<uint32_t>& groups) {
	// this method extracts aggregates and group by columns from a select list element
	// it performs the following tasks:
	// (1) aggregates have to be extracted and replaced with BoundColumnRefExpression that reference the aggregate index
	// (2) group references have to be extracted and replaced with BoundcolumnRefExpression that reference the group index
	// (3) any remaining ColumnRefExpressions have to be wrapped in a FIRST() aggregate before being bound
	auto &binding = node.binding;
	// first we extract any aggregates
	expr = ExtractAggregates(move(expr), binding.aggregates, binding.aggregate_index);
	// now we extract any GROUP BY references 
	if (groups.size() > 0) {
		expr = ExtractGroupReferences(move(expr), binding.group_index, groups);
	}
	// finally we wrap remaining ColumnRefExpressions into a FIRST() aggregate
	expr = WrapInFirstAggregate(move(expr), binding.aggregates, binding.aggregate_index);
	return expr;
}

unique_ptr<Expression> Binder::ExtractExpressionClass(unique_ptr<Expression> expr, vector<unique_ptr<Expression>> &result,
                                                 size_t bind_index, ExpressionClass expr_class) {
	if (expr->GetExpressionClass() == expr_class) {
		// first we visit the node and resolve its type
		VisitAndResolveType(&expr);
		// now we create a BoundColumnRef that references the entry
		auto colref_expr = make_unique<BoundColumnRefExpression>(*expr, expr->return_type, ColumnBinding(bind_index, result.size()));
		result.push_back(move(expr));
		return colref_expr;
	}
	expr->EnumerateChildren([&](unique_ptr<Expression> expr) -> unique_ptr<Expression> {
		return ExtractExpressionClass(move(expr), result, bind_index, expr_class);
	});
	return expr;
}

unique_ptr<Expression> Binder::ExtractAggregates(unique_ptr<Expression> expr, vector<unique_ptr<Expression>> &result, size_t aggregate_index) {
	return ExtractExpressionClass(move(expr), result, aggregate_index, ExpressionClass::AGGREGATE);
}

unique_ptr<Expression> Binder::ExtractWindowFunctions(unique_ptr<Expression> expr, vector<unique_ptr<Expression>> &result, size_t window_index) {
	return ExtractExpressionClass(move(expr), result, window_index, ExpressionClass::WINDOW);
}

unique_ptr<Expression> Binder::ExtractGroupReferences(unique_ptr<Expression> expr, size_t group_index, expression_map_t<uint32_t> &groups) {
	assert(expr->GetExpressionClass() != ExpressionClass::AGGREGATE);

	// check if the expression is a GroupBy expression
	auto entry = groups.find(expr.get());
	if (entry != groups.end()) {
		auto group = entry->first;
		// group reference! turn expression into a reference to the group
		return make_unique<BoundColumnRefExpression>(*expr, group->return_type, ColumnBinding(group_index, entry->second));
	}
	// not an aggregate and not a column reference
	// iterate over the children
	expr->EnumerateChildren([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		return ExtractGroupReferences(move(child), group_index, groups);
	});
	return expr;
}

unique_ptr<Expression> Binder::WrapInFirstAggregate(unique_ptr<Expression> expr, vector<unique_ptr<Expression>> &result,
                                                 size_t aggregate_index) {
	if (expr->type == ExpressionType::COLUMN_REF) {
		// a ColumnRefExpression that is (1) not inside an aggregate and (2) not a GROUP BY column
		// wrap it inside a FIRST() aggregate
		// first visit the column reference
		VisitAndResolveType(&expr);
		// create a colref that refers to this aggregate
		auto colref_expr = make_unique<BoundColumnRefExpression>(*expr, expr->return_type, ColumnBinding(aggregate_index, result.size()));
		// now we create a FIRST aggregate
		auto first_aggregate = make_unique<AggregateExpression>(ExpressionType::AGGREGATE_FIRST, move(expr));
		first_aggregate->ResolveType();
		// add the FIRST aggregate to the set of aggregates
		result.push_back(move(first_aggregate));
		// return the replacement BoundColumnRefExpression
		return move(colref_expr);
	}

	expr->EnumerateChildren([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		return WrapInFirstAggregate(move(child), result, aggregate_index);
	});
	return expr;
}

void Binder::VisitAndResolveType(unique_ptr<Expression>* expr) {
	VisitExpression(expr);
	(*expr)->ResolveType();
	assert((*expr)->return_type != TypeId::INVALID);
}
