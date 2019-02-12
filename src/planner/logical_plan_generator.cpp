#include "planner/logical_plan_generator.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/expression/list.hpp"
#include "parser/query_node/list.hpp"
#include "parser/statement/list.hpp"
#include "parser/tableref/list.hpp"
#include "planner/operator/list.hpp"
#include "planner/binder.hpp"

#include <map>

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::CreatePlan(SQLStatement &statement) {
	switch (statement.type) {
	case StatementType::SELECT:
		CreatePlan((SelectStatement &)statement);
		break;
	case StatementType::INSERT:
		CreatePlan((InsertStatement &)statement);
		break;
	case StatementType::COPY:
		CreatePlan((CopyStatement &)statement);
		break;
	case StatementType::DELETE:
		CreatePlan((DeleteStatement &)statement);
		break;
	case StatementType::UPDATE:
		CreatePlan((UpdateStatement &)statement);
		break;
	case StatementType::ALTER:
		CreatePlan((AlterTableStatement &)statement);
		break;
	case StatementType::CREATE_TABLE:
		CreatePlan((CreateTableStatement &)statement);
		break;
	case StatementType::CREATE_INDEX:
		CreatePlan((CreateIndexStatement &)statement);
		break;
	default:
		throw NotImplementedException("Statement type");
		break;
	}
}

void LogicalPlanGenerator::CreatePlan(QueryNode &node) {
	if (node.type == QueryNodeType::SELECT_NODE) {
		CreatePlan((SelectNode &)node);
	} else {
		assert(node.type == QueryNodeType::SET_OPERATION_NODE);
		CreatePlan((SetOperationNode &)node);
	}
}

class HasCorrelatedExpressions : public LogicalOperatorVisitor {
public:
	HasCorrelatedExpressions() : 
		has_correlated_expressions(false) {}

	void VisitOperator(LogicalOperator &op) override {
		VisitOperatorExpressions(op);
	}
	void Visit(BoundColumnRefExpression &expr) override {
		if (expr.depth == 0) {
			return;
		}
		// correlated column reference
		assert(expr.depth == 1);
		has_correlated_expressions = true;
	}

	bool has_correlated_expressions;
};

class RewriteCorrelatedExpressions : public LogicalOperatorVisitor {
public:
	RewriteCorrelatedExpressions(ColumnBinding base_binding, column_binding_map_t<size_t>& correlated_map) : 
		base_binding(base_binding), correlated_map(correlated_map) {}

	void VisitOperator(LogicalOperator &op) override {
		VisitOperatorExpressions(op);
	}
	void Visit(BoundColumnRefExpression &expr) override {
		if (expr.depth == 0) {
			return;
		}
		// correlated column reference
		// replace with the entry referring to the duplicate eliminated scan
		assert(expr.depth == 1);
		auto entry = correlated_map.find(expr.binding);
		assert(entry != correlated_map.end());
		expr.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
		expr.depth = 0;
	}
private:
	ColumnBinding base_binding;
	column_binding_map_t<size_t>& correlated_map;
};

struct FlattenDependentJoins {
	FlattenDependentJoins(Binder& binder, const vector<CorrelatedColumnInfo>& correlated_columns) : 
		binder(binder), correlated_columns(correlated_columns) {}


	//! Detects which Logical Operators have correlated expressions that they are dependent upon, filling the has_correlated_expressions map.
	bool DetectCorrelatedExpressions(LogicalOperator *op) {
		assert(op);
		// check if this entry has correlated expressions
		HasCorrelatedExpressions visitor;
		visitor.VisitOperator(*op);
		bool has_correlation = visitor.has_correlated_expressions;
		// now visit the children of this entry and check if they have correlated expressions
		for(auto &child : op->children) {
			// we OR the property with its children such that has_correlation is true if either
			// (1) this node has a correlated expression or
			// (2) one of its children has a correlated expression
			if (DetectCorrelatedExpressions(child.get())) {
				has_correlation = true;
			}
		}
		// set the entry in the map
		has_correlated_expressions[op] = has_correlation;
		return has_correlation;
	}

	unique_ptr<LogicalOperator> PushDownDependentJoin(unique_ptr<LogicalOperator> plan) {
		// first check if the logical operator has correlated expressions
		auto entry = has_correlated_expressions.find(plan.get());
		assert(entry != has_correlated_expressions.end());
		if (!entry->second) {
			// we reached a node without correlated expressions
			// we can eliminate the dependent join now and create a simple cross product
			auto cross_product = make_unique<LogicalCrossProduct>();
			// now create the duplicate eliminated scan for this node
			auto delim_index = binder.GenerateTableIndex();
			this->base_binding = ColumnBinding(delim_index, 0);
			auto delim_scan = make_unique<LogicalChunkGet>(delim_index, delim_types);
			cross_product->children.push_back(move(delim_scan));
			cross_product->children.push_back(move(plan));
			return move(cross_product);
		}
		switch(plan->type) {
			case LogicalOperatorType::FILTER: {
				// filter
				// first we flatten the dependent join in the child of the filter
				plan->children[0] = PushDownDependentJoin(move(plan->children[0]));
				// then we replace any correlated expressions with the corresponding entry in the correlated_map
				RewriteCorrelatedExpressions rewriter(base_binding, correlated_map);
				rewriter.VisitOperator(*plan);
				return plan;
			}
			case LogicalOperatorType::PROJECTION: {
				// projection
				// first we flatten the dependent join in the child of the projection
				plan->children[0] = PushDownDependentJoin(move(plan->children[0]));
				// then we replace any correlated expressions with the corresponding entry in the correlated_map
				RewriteCorrelatedExpressions rewriter(base_binding, correlated_map);
				rewriter.VisitOperator(*plan);
				// now we add all the columns of the delim_scan to the projection list
				auto proj = (LogicalProjection*) plan.get();
				for(size_t i = 0; i < correlated_columns.size(); i++) {
					auto colref = make_unique<BoundColumnRefExpression>("", correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
					plan->expressions.push_back(move(colref));
				}
				base_binding.table_index = proj->table_index;
				this->delim_offset = base_binding.column_index = plan->expressions.size() - correlated_columns.size();
				this->data_offset = 0;
				return plan;
			}
			case LogicalOperatorType::AGGREGATE_AND_GROUP_BY: {
				// aggregate and group by
				// first we flatten the dependent join in the child of the projection
				plan->children[0] = PushDownDependentJoin(move(plan->children[0]));
				// then we replace any correlated expressions with the corresponding entry in the correlated_map
				RewriteCorrelatedExpressions rewriter(base_binding, correlated_map);
				rewriter.VisitOperator(*plan);
				// now we add all the columns of the delim_scan to the grouping operators AND the projection list
				auto aggr = (LogicalAggregate*) plan.get();
				for(size_t i = 0; i < correlated_columns.size(); i++) {
					auto colref = make_unique<BoundColumnRefExpression>("", correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
					aggr->groups.push_back(move(colref));
				}
				// now we update the delim_index
				base_binding.table_index = aggr->group_index;
				this->delim_offset = base_binding.column_index = aggr->groups.size() - correlated_columns.size();
				this->data_offset = aggr->groups.size();
				return plan;
			}
			case LogicalOperatorType::LIMIT: {
				auto &limit = (LogicalLimit&) *plan;
				if (limit.offset > 0) {
					throw ParserException("OFFSET not supported in correlated subquery");
				}
				plan->children[0] = PushDownDependentJoin(move(plan->children[0]));
				if (limit.limit == 0) {
					// limit = 0 means we return zero columns here
					return plan;
				} else {
					// limit > 0 does nothing
					return move(plan->children[0]);
				}
			}
			case LogicalOperatorType::ORDER_BY:
				throw ParserException("ORDER BY not supported in correlated subquery");
			default:
				throw NotImplementedException("Logical operator type for dependent join");
		}
	}

	Binder& binder;
	ColumnBinding base_binding;
	size_t delim_offset;
	size_t data_offset;
	unordered_map<LogicalOperator*, bool> has_correlated_expressions;
	column_binding_map_t<size_t> correlated_map;
	const vector<CorrelatedColumnInfo>& correlated_columns;
	vector<TypeId> delim_types;
};


unique_ptr<Expression> LogicalPlanGenerator::VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) {
	// first visit the children of the Subquery expression, if any
	VisitExpressionChildren(expr);

	// check if the subquery is correlated
	auto &subquery = (SubqueryExpression&) *expr.subquery;
	// first we translate the QueryNode of the subquery into a logical plan
	LogicalPlanGenerator generator(*expr.binder, context);
	generator.CreatePlan(*subquery.subquery);
	if (!generator.root) {
		throw Exception("Can't plan subquery");
	}
	auto plan = move(generator.root);
	switch(subquery.subquery_type) {
	case SubqueryType::EXISTS: {
		if (expr.IsCorrelated()) {
			throw Exception("Correlated exists not handled yet!");
		}
		// uncorrelated EXISTS
		// we only care about existence, hence we push a LIMIT 1 operator
		auto limit = make_unique<LogicalLimit>(1, 0);
		limit->AddChild(move(plan));
		plan = move(limit);

		// now we push a COUNT(*) aggregate onto the limit, this will be either 0 or 1 (EXISTS or NOT EXISTS)
		auto count_star = make_unique<AggregateExpression>(ExpressionType::AGGREGATE_COUNT_STAR, nullptr);
		count_star->ResolveType();
		auto count_type = count_star->return_type;
		vector<unique_ptr<Expression>> aggregate_list;
		aggregate_list.push_back(move(count_star));
		auto aggregate_index = binder.GenerateTableIndex();
		auto aggregate = make_unique<LogicalAggregate>(binder.GenerateTableIndex(), aggregate_index, move(aggregate_list));
		aggregate->AddChild(move(plan));
		plan = move(aggregate);

		// now we push a projection with a comparison to 1
		auto left_child = make_unique<BoundColumnRefExpression>("", count_type, ColumnBinding(aggregate_index, 0));
		auto right_child = make_unique<ConstantExpression>(Value::Numeric(count_type, 1));
		auto comparison = make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(left_child), move(right_child));


		vector<unique_ptr<Expression>> projection_list;
		projection_list.push_back(move(comparison));
		auto projection_index = binder.GenerateTableIndex();
		auto projection = make_unique<LogicalProjection>(projection_index, move(projection_list));
		projection->AddChild(move(plan));
		plan = move(projection);

		// we add it to the main query by adding a cross product
		// FIXME: should use something else besides cross product as we always add only one scalar constant
		if (root) {
			auto cross_product = make_unique<LogicalCrossProduct>();
			cross_product->AddChild(move(root));
			cross_product->AddChild(move(plan));
			root = move(cross_product);
		} else {
			root = move(plan);
		}

		// we replace the original subquery with a ColumnRefExpression refering to the result of the projection (either TRUE or FALSE)
		return make_unique<BoundColumnRefExpression>(expr, TypeId::BOOLEAN, ColumnBinding(projection_index, 0));
	}
	case SubqueryType::SCALAR: {
		if (!expr.IsCorrelated()) {
			// in the uncorrelated case we are only interested in the first result of the query
			// hence we simply push a LIMIT 1 to get the first row of the subquery
			auto limit = make_unique<LogicalLimit>(1, 0);
			limit->AddChild(move(plan));
			plan = move(limit);
			// we push an aggregate that returns the FIRST element
			vector<unique_ptr<Expression>> expressions;
			auto bound = make_unique<BoundExpression>(expr.return_type, 0);
			auto first_agg = make_unique<AggregateExpression>(ExpressionType::AGGREGATE_FIRST, move(bound));
			first_agg->ResolveType();
			expressions.push_back(move(first_agg));
			auto aggr_index = binder.GenerateTableIndex();
			auto aggr = make_unique<LogicalAggregate>(binder.GenerateTableIndex(), aggr_index, move(expressions));
			aggr->AddChild(move(plan));
			plan = move(aggr);

			// in the uncorrelated case, we add the value to the main query through a cross product
			// FIXME: should use something else besides cross product as we always add only one scalar constant and cross product is not optimized for this. 
			assert(root);
			auto cross_product = make_unique<LogicalCrossProduct>();
			cross_product->AddChild(move(root));
			cross_product->AddChild(move(plan));
			root = move(cross_product);
				
			// we replace the original subquery with a BoundColumnRefExpression refering to the first result of the aggregation
			return make_unique<BoundColumnRefExpression>(expr, expr.return_type, ColumnBinding(aggr_index, 0));
		} else {
			// in the correlated case, we push first a DUPLICATE ELIMINATED left outer join (as entries WITHOUT a join partner result in NULL)
			auto delim_join = make_unique<LogicalJoin>(JoinType::SINGLE);
			// the left side is the original plan
			delim_join->AddChild(move(root));
			delim_join->is_duplicate_eliminated = true;
			delim_join->null_values_are_equal = true;
			// the right side is a DEPENDENT join between the duplicate eliminated scan and the subquery
			// first get the set of correlated columns in the subquery
			// these are the columns returned by the duplicate eliminated scan
			auto &correlated_columns = expr.binder->correlated_columns;
			FlattenDependentJoins flatten(binder, correlated_columns);
			for(size_t i = 0; i < correlated_columns.size(); i++) {
				auto &col = correlated_columns[i];
				flatten.correlated_map[col.binding] = i;
				flatten.delim_types.push_back(col.type);
				delim_join->duplicate_eliminated_columns.push_back(make_unique<BoundColumnRefExpression>("", col.type, col.binding));
			}

			// now we have a dependent join between "delim_scan" and the subquery plan
			// however, we do not explicitly create it
			// instead, we eliminate the dependent join by pushing it down into the right side of the plan
			// first we check which operators have correlated expressions in the first place
			flatten.DetectCorrelatedExpressions(plan.get());

			// now we push the dependent join down
			auto dependent_join = flatten.PushDownDependentJoin(move(plan));
			// push a subquery node under the duplicate eliminated join
			auto subquery_index = binder.GenerateTableIndex();
			auto subquery = make_unique<LogicalSubquery>(subquery_index, correlated_columns.size() + 1);
			subquery->AddChild(move(dependent_join));
			// now we create the join conditions between the dependent join and the original table
			for(size_t i = 0; i < correlated_columns.size(); i++) {
				auto &col = correlated_columns[i];
				JoinCondition cond;
				cond.left = make_unique<BoundColumnRefExpression>(col.name, col.type, col.binding);
				cond.right = make_unique<BoundColumnRefExpression>(col.name, col.type, ColumnBinding(subquery_index, flatten.delim_offset + i));
				cond.comparison = ExpressionType::COMPARE_EQUAL;
				delim_join->conditions.push_back(move(cond));
			}

			delim_join->AddChild(move(subquery));
			root = move(delim_join);
			// finally push the BoundColumnRefExpression referring to the data element
			return make_unique<BoundColumnRefExpression>(expr, expr.return_type, ColumnBinding(subquery_index, flatten.data_offset));
		}
	}
	default: {
		assert(subquery.subquery_type == SubqueryType::ANY);
		if (expr.IsCorrelated()) {
			throw Exception("Correlated ANY not handled yet!");
		}
		// we generate a MARK join that results in either (TRUE, FALSE or NULL)
		// subquery has NULL values -> result is (TRUE or NULL)
		// subquery has no NULL values -> result is (TRUE, FALSE or NULL [if input is NULL])
		// first we push a subquery to the right hand side
		auto subquery_index = binder.GenerateTableIndex();
		auto logical_subquery = make_unique<LogicalSubquery>(subquery_index, 1);
		logical_subquery->AddChild(move(plan));
		plan = move(logical_subquery);

		// then we generate the MARK join with the subquery
		auto join = make_unique<LogicalJoin>(JoinType::MARK);
		join->AddChild(move(root));
		join->AddChild(move(plan));
		// create the JOIN condition
		JoinCondition cond;
		cond.left = move(subquery.child);
		cond.right = make_unique<BoundExpression>(cond.left->return_type, 0);
		cond.comparison = subquery.comparison_type;
		join->conditions.push_back(move(cond));
		root = move(join);

		// we replace the original subquery with a BoundColumnRefExpression refering to the mark column
		return make_unique<BoundColumnRefExpression>(expr, expr.return_type, ColumnBinding(subquery_index, 0));
	}
	}
	return nullptr;
}

unique_ptr<TableRef> LogicalPlanGenerator::Visit(BaseTableRef &expr) {
	// FIXME: catalog access should only happen once in binder
	auto table = context.db.catalog.GetTable(context.ActiveTransaction(), expr.schema_name, expr.table_name);
	auto alias = expr.alias.empty() ? expr.table_name : expr.alias;

	auto index = binder.bind_context.GetBindingIndex(alias);

	vector<column_t> column_ids;
	// look in the context for this table which columns are required
	for (auto &bound_column : binder.bind_context.bound_columns[alias]) {
		column_ids.push_back(table->name_map[bound_column]);
	}
	if (require_row_id || column_ids.size() == 0) {
		// no column ids selected
		// the query is like SELECT COUNT(*) FROM table, or SELECT 42 FROM table
		// return just the row id
		column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}

	auto get_table = make_unique<LogicalGet>(table, index, column_ids);
	if (root) {
		get_table->AddChild(move(root));
	}
	root = move(get_table);
	return nullptr;
}

unique_ptr<TableRef> LogicalPlanGenerator::Visit(CrossProductRef &expr) {
	auto cross_product = make_unique<LogicalCrossProduct>();

	if (root) {
		throw Exception("Cross product cannot have children!");
	}

	AcceptChild(&expr.left);
	assert(root);
	cross_product->AddChild(move(root));
	root = nullptr;

	AcceptChild(&expr.right);
	assert(root);
	cross_product->AddChild(move(root));
	root = nullptr;

	root = move(cross_product);
	return nullptr;
}

unique_ptr<TableRef> LogicalPlanGenerator::Visit(JoinRef &expr) {
	VisitExpression(&expr.condition);
	auto join = make_unique<LogicalJoin>(expr.type);

	if (root) {
		throw Exception("Joins product cannot have children!");
	}

	// we do not generate joins here

	AcceptChild(&expr.left);
	assert(root);
	join->AddChild(move(root));
	root = nullptr;

	AcceptChild(&expr.right);
	assert(root);
	join->AddChild(move(root));
	root = nullptr;

	join->expressions.push_back(move(expr.condition));
	LogicalFilter::SplitPredicates(join->expressions);

	root = move(join);

	return nullptr;
}

unique_ptr<TableRef> LogicalPlanGenerator::Visit(SubqueryRef &expr) {
	// generate the logical plan for the subquery
	// this happens separately from the current LogicalPlan generation
	LogicalPlanGenerator generator(*expr.binder, context);

	size_t column_count = expr.subquery->GetSelectList().size();
	generator.CreatePlan(*expr.subquery);

	auto index = binder.bind_context.GetBindingIndex(expr.alias);

	if (root) {
		throw Exception("Subquery cannot have children");
	}
	root = make_unique<LogicalSubquery>(index, column_count);
	root->children.push_back(move(generator.root));
	return nullptr;
}

unique_ptr<TableRef> LogicalPlanGenerator::Visit(TableFunction &expr) {
	// FIXME: catalog access should only happen once in binder
	auto function_definition = (FunctionExpression *)expr.function.get();
	auto function = context.db.catalog.GetTableFunction(context.ActiveTransaction(), function_definition);

	auto index = binder.bind_context.GetBindingIndex(expr.alias.empty() ? function_definition->function_name : expr.alias);

	if (root) {
		throw Exception("Table function cannot have children");
	}
	root = make_unique<LogicalTableFunction>(function, index, move(expr.function));
	return nullptr;
}
