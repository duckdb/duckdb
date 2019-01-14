#include "execution/physical_plan_generator.hpp"

#include "execution/column_binding_resolver.hpp"
#include "execution/operator/list.hpp"
#include "parser/expression/list.hpp"
#include "planner/operator/list.hpp"
#include "storage/order_index.hpp"
#include "storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

class PlanSubqueries : public LogicalOperatorVisitor {
public:
	PlanSubqueries(ClientContext &context) : context(context) {
	}

protected:
	using SQLNodeVisitor::Visit;
	void Visit(SubqueryExpression &expr) override {
		assert(expr.op);
		PhysicalPlanGenerator generator(context);
		generator.CreatePlan(move(expr.op));
		expr.plan = move(generator.plan);
	}

private:
	ClientContext &context;
};

void PhysicalPlanGenerator::CreatePlan(unique_ptr<LogicalOperator> op) {
	// first resolve column references
	ColumnBindingResolver resolver;
	resolver.VisitOperator(*op);
	// now resolve types of all the operators
	op->ResolveOperatorTypes();
	// create the physical plan of any subqueries
	PlanSubqueries planner(context);
	planner.VisitOperator(*op);
	// then create the main physical plan
	VisitOperator(*op);
	assert(plan); // Unknown error in physical plan generation"
}

void PhysicalPlanGenerator::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::GET:
		Visit((LogicalGet &)op);
		break;
	case LogicalOperatorType::PROJECTION:
		Visit((LogicalProjection &)op);
		break;
	case LogicalOperatorType::FILTER:
		Visit((LogicalFilter &)op);
		break;
	case LogicalOperatorType::AGGREGATE_AND_GROUP_BY:
		Visit((LogicalAggregate &)op);
		break;
	case LogicalOperatorType::WINDOW:
		Visit((LogicalWindow &)op);
		break;
	case LogicalOperatorType::LIMIT:
		Visit((LogicalLimit &)op);
		break;
	case LogicalOperatorType::ORDER_BY:
		Visit((LogicalOrder &)op);
		break;
	case LogicalOperatorType::COPY:
		Visit((LogicalCopy &)op);
		break;
	case LogicalOperatorType::TABLE_FUNCTION:
		Visit((LogicalTableFunction &)op);
		break;
	case LogicalOperatorType::JOIN:
		Visit((LogicalJoin &)op);
		break;
	case LogicalOperatorType::CROSS_PRODUCT:
		Visit((LogicalCrossProduct &)op);
		break;
	case LogicalOperatorType::UNION:
		Visit((LogicalUnion &)op);
		break;
	case LogicalOperatorType::EXCEPT:
		Visit((LogicalExcept &)op);
		break;
	case LogicalOperatorType::INTERSECT:
		Visit((LogicalIntersect &)op);
		break;
	case LogicalOperatorType::INSERT:
		Visit((LogicalInsert &)op);
		break;
	case LogicalOperatorType::DELETE:
		Visit((LogicalDelete &)op);
		break;
	case LogicalOperatorType::UPDATE:
		Visit((LogicalUpdate &)op);
		break;
	case LogicalOperatorType::CREATE:
		Visit((LogicalCreate &)op);
		break;
	case LogicalOperatorType::CREATE_INDEX:
		Visit((LogicalCreateIndex &)op);
		break;
	case LogicalOperatorType::EXPLAIN:
		Visit((LogicalExplain &)op);
		break;
	case LogicalOperatorType::PRUNE_COLUMNS:
		Visit((LogicalPruneColumns &)op);
		break;
	default:
		LogicalOperatorVisitor::VisitOperator(op);
		break;
	}
}

void PhysicalPlanGenerator::Visit(LogicalAggregate &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	if (op.groups.size() == 0) {
		// no groups
		if (!plan) {
			// and no FROM clause, use a dummy aggregate
			auto groupby = make_unique<PhysicalHashAggregate>(op, move(op.expressions));
			this->plan = move(groupby);
		} else {
			// but there is a FROM clause
			// special case: aggregate entire columns together
			auto groupby = make_unique<PhysicalHashAggregate>(op, move(op.expressions));
			groupby->children.push_back(move(plan));
			this->plan = move(groupby);
		}
	} else {
		// groups! create a GROUP BY aggregator
		if (!plan) {
			throw Exception("Cannot have GROUP BY without FROM clause!");
		}

		auto groupby = make_unique<PhysicalHashAggregate>(op, move(op.expressions), move(op.groups));
		groupby->children.push_back(move(plan));
		this->plan = move(groupby);
	}
}

void PhysicalPlanGenerator::Visit(LogicalCrossProduct &op) {
	assert(!plan); // Cross product should be the first node of a plan!

	assert(op.children.size() == 2);

	VisitOperator(*op.children[0]);
	auto left = move(plan);
	VisitOperator(*op.children[1]);
	auto right = move(plan);

	plan = make_unique<PhysicalCrossProduct>(op, move(left), move(right));
}

void PhysicalPlanGenerator::Visit(LogicalDelete &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);
	assert(plan); // Delete node cannot be the first node of a plan!

	auto del = make_unique<PhysicalDelete>(op, *op.table, *op.table->storage);
	del->children.push_back(move(plan));
	this->plan = move(del);
}

void PhysicalPlanGenerator::Visit(LogicalUpdate &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);
	assert(plan); // Update node cannot be the first node of a plan!

	auto update = make_unique<PhysicalUpdate>(op, *op.table, *op.table->storage, op.columns, move(op.expressions));
	update->children.push_back(move(plan));
	this->plan = move(update);
}

void PhysicalPlanGenerator::Visit(LogicalCreate &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);
	assert(!plan); // CREATE node must be first node of the plan!

	this->plan = make_unique<PhysicalCreate>(op, op.schema, move(op.info));
}

void PhysicalPlanGenerator::Visit(LogicalCreateIndex &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);
	assert(!plan); // CREATE INDEX node must be first node of the plan!

	this->plan = make_unique<PhysicalCreateIndex>(op, op.table, op.column_ids, move(op.expressions), move(op.info));
}

#include "optimizer/matcher/expression_matcher.hpp"

//! Attempt to create an index scan from a filter + get, if possible
// FIXME: this should not be done here
static unique_ptr<PhysicalOperator> CreateIndexScan(LogicalFilter &filter, LogicalGet &scan) {
	if (!scan.table) {
		return nullptr;
	}

	auto &storage = *scan.table->storage;

	if (storage.indexes.size() == 0) {
		// no indexes on the table, can't rewrite
		return nullptr;
	}
	// check all the indexes
	for (size_t j = 0; j < storage.indexes.size(); j++) {
		Value low_value, high_value, equal_value;
		int low_index = -1, high_index = -1, equal_index = -1;
		auto &index = storage.indexes[j];
		// FIXME: assume every index is order index currently
		assert(index->type == IndexType::ORDER_INDEX);
		auto order_index = (OrderIndex *)index.get();
		// try to find a matching index for any of the filter expressions
		auto expr = filter.expressions[0].get();
		auto low_comparison_type = expr->type;
		auto high_comparison_type = expr->type;
		for (size_t i = 0; i < filter.expressions.size(); i++) {
			expr = filter.expressions[i].get();
			// create a matcher for a comparison with a constant
			ComparisonExpressionMatcher matcher;
			// match on a comparison type
			matcher.expr_type = make_unique<ComparisonExpressionTypeMatcher>();
			// match on a constant comparison with the indexed expression
			matcher.matchers.push_back(make_unique<ExpressionEqualityMatcher>(order_index->expressions[0].get()));
			matcher.matchers.push_back(make_unique<ConstantExpressionMatcher>());
			matcher.policy = SetMatcher::Policy::UNORDERED;

			vector<Expression *> bindings;
			if (matcher.Match(expr, bindings)) {
				// range or equality comparison with constant value
				// we can use our index here
				// bindings[0] = the expression
				// bindings[1] = the index expression
				// bindings[2] = the constant
				auto comparison = (ComparisonExpression *)bindings[0];
				assert(bindings[0]->GetExpressionClass() == ExpressionClass::COMPARISON);
				assert(bindings[1]->Equals(order_index->expressions[0].get()));
				assert(bindings[2]->type == ExpressionType::VALUE_CONSTANT);

				auto constant_value = ((ConstantExpression *)bindings[2])->value;
				auto comparison_type = comparison->type;
				if (comparison->right.get() == bindings[1]) {
					// the expression is on the right side, we flip them around
					comparison_type = ComparisonExpression::FlipComparisionExpression(comparison_type);
				}
				if (comparison_type == ExpressionType::COMPARE_EQUAL) {
					// equality value
					// equality overrides any other bounds so we just break here
					equal_index = i;
					equal_value = constant_value;
					break;
				} else if (comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
				           comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
					// greater than means this is a lower bound
					low_index = i;
					low_value = constant_value;
					low_comparison_type = comparison_type;
				} else {
					// smaller than means this is an upper bound
					high_index = i;
					high_value = constant_value;
					high_comparison_type = comparison_type;
				}
			}
		}
		if (equal_index >= 0 || low_index >= 0 || high_index >= 0) {
			auto index_scan =
			    make_unique<PhysicalIndexScan>(scan, *scan.table, *scan.table->storage, *order_index, scan.column_ids);
			if (equal_index >= 0) {
				index_scan->equal_value = equal_value;
				index_scan->equal_index = true;
				filter.expressions.erase(filter.expressions.begin() + equal_index);
			}
			if (low_index >= 0) {
				index_scan->low_value = low_value;
				index_scan->low_index = true;
				index_scan->low_expression_type = low_comparison_type;
				filter.expressions.erase(filter.expressions.begin() + low_index);
			}
			if (high_index >= 0) {
				index_scan->high_value = high_value;
				index_scan->high_index = true;
				index_scan->high_expression_type = high_comparison_type;
				filter.expressions.erase(filter.expressions.begin() + high_index);
			}
			return move(index_scan);
		}
	}
	return nullptr;
}

void PhysicalPlanGenerator::Visit(LogicalFilter &op) {
	if (op.empty_result) {
		// the filter is guaranteed to produce an empty result
		// create a node that returns an empty result instead of generating the rest of the plan
		this->plan = make_unique<PhysicalEmptyResult>(op.types);
		return;
	}
	if (op.children[0]->type == LogicalOperatorType::GET) {
		// filter + get
		// check if we can transform this into an index scan
		auto scan = (LogicalGet *)op.children[0].get();
		auto node = CreateIndexScan(op, *scan);
		if (node) {
			// we can do an index scan, move it to the root node
			assert(!plan);
			plan = move(node);
		} else {
			// we can't do an index scan, create a normal table scan
			LogicalOperatorVisitor::VisitOperatorChildren(op);
		}
	} else {
		LogicalOperatorVisitor::VisitOperatorChildren(op);
	}

	assert(plan); // Filter cannot be the first node of a plan!"
	if (op.expressions.size() > 0) {
		// create a filter if there is anything to filter
		auto filter = make_unique<PhysicalFilter>(op, move(op.expressions));
		filter->children.push_back(move(plan));
		this->plan = move(filter);
	}
}

void PhysicalPlanGenerator::Visit(LogicalGet &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	if (!op.table) {
		vector<TypeId> types = {TypeId::BIGINT};
		this->plan = make_unique<PhysicalDummyScan>(types);
		return;
	}
	auto scan = make_unique<PhysicalTableScan>(op, *op.table, *op.table->storage, op.column_ids);
	assert(!plan); // Scan has to be the first node of a plan!
	this->plan = move(scan);
}

void PhysicalPlanGenerator::Visit(LogicalJoin &op) {
	assert(!plan); // Cross product should be the first node of a plan!

	// now visit the children
	assert(op.children.size() == 2);

	VisitOperator(*op.children[0]);
	auto left = move(plan);
	VisitOperator(*op.children[1]);
	auto right = move(plan);

	if (op.conditions.size() == 0) {
		plan = make_unique<PhysicalCrossProduct>(op, move(left), move(right));
		return;
	}
	assert(op.conditions.size() > 0);

	bool has_equality = false;
	bool has_inequality = false;
	for (auto &cond : op.conditions) {
		VisitExpression(&cond.left);
		VisitExpression(&cond.right);
		if (cond.comparison == ExpressionType::COMPARE_EQUAL) {
			has_equality = true;
		}
		if (cond.comparison == ExpressionType::COMPARE_NOTEQUAL) {
			has_inequality = true;
		}
	}

	assert(left);
	assert(right);
	if (has_equality) {
		// equality join: use hash join
		plan = make_unique<PhysicalHashJoin>(op, move(left), move(right), move(op.conditions), op.type);
	} else {
		if (op.conditions.size() == 1 && op.type == JoinType::INNER && !has_inequality) {
			// range join: use piecewise merge join
			plan = make_unique<PhysicalPiecewiseMergeJoin>(op, move(left), move(right), move(op.conditions), op.type);
		} else {
			// non-equality join: use nested loop
			if (op.type == JoinType::INNER) {
				plan =
				    make_unique<PhysicalNestedLoopJoinInner>(op, move(left), move(right), move(op.conditions), op.type);
			} else if (op.type == JoinType::ANTI || op.type == JoinType::SEMI) {
				plan =
				    make_unique<PhysicalNestedLoopJoinSemi>(op, move(left), move(right), move(op.conditions), op.type);
			} else {
				throw NotImplementedException("Unimplemented nested loop join type!");
			}
		}
	}
}

void PhysicalPlanGenerator::Visit(LogicalLimit &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	auto limit = make_unique<PhysicalLimit>(op, op.limit, op.offset);
	assert(plan); // Limit cannot be the first node of a plan!
	limit->children.push_back(move(plan));
	this->plan = move(limit);
}

void PhysicalPlanGenerator::Visit(LogicalOrder &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	assert(plan); // Order cannot be the first node of a plan!

	auto order = make_unique<PhysicalOrder>(op, move(op.description));
	order->children.push_back(move(plan));
	this->plan = move(order);
}

void PhysicalPlanGenerator::Visit(LogicalProjection &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	auto projection = make_unique<PhysicalProjection>(op, move(op.expressions));
	if (plan) {
		projection->children.push_back(move(plan));
	}
	this->plan = move(projection);
}

void PhysicalPlanGenerator::Visit(LogicalInsert &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	auto insertion = make_unique<PhysicalInsert>(op, op.table, move(op.insert_values), op.column_index_map);
	if (plan) {
		insertion->children.push_back(move(plan));
	}
	this->plan = move(insertion);
}

void PhysicalPlanGenerator::Visit(LogicalPruneColumns &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	assert(plan); // Prune columns cannot be the first node of a plan!"
	if (plan->GetTypes().size() > op.column_limit) {
		// only prune if we need to
		auto node = make_unique<PhysicalPruneColumns>(op, op.column_limit);
		node->children.push_back(move(plan));
		this->plan = move(node);
	}
}

void PhysicalPlanGenerator::Visit(LogicalTableFunction &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	assert(!plan); // Table function has to be first node of the plan!
	this->plan = make_unique<PhysicalTableFunction>(op, op.function, move(op.function_call));
}

void PhysicalPlanGenerator::Visit(LogicalCopy &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	if (plan) {
		auto copy = make_unique<PhysicalCopy>(op, move(op.file_path), move(op.is_from), move(op.delimiter),
		                                      move(op.quote), move(op.escape));
		copy->children.push_back(move(plan));
		this->plan = move(copy);
	} else {
		auto copy = make_unique<PhysicalCopy>(op, op.table, move(op.file_path), move(op.is_from), move(op.delimiter),
		                                      move(op.quote), move(op.escape), move(op.select_list));
		this->plan = move(copy);
	}
}

void PhysicalPlanGenerator::Visit(LogicalExplain &op) {
	auto logical_plan_opt = op.children[0]->ToString();
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	if (plan) {
		op.physical_plan = plan->ToString();
	}

	vector<string> keys = {"logical_plan", "logical_opt", "physical_plan"};
	vector<string> values = {op.logical_plan_unopt, logical_plan_opt, op.physical_plan};

	this->plan = make_unique<PhysicalExplain>(op, keys, values);
}

void PhysicalPlanGenerator::Visit(LogicalUnion &op) {
	assert(op.children.size() == 2);

	VisitOperator(*op.children[0]);
	auto left = move(plan);
	VisitOperator(*op.children[1]);
	auto right = move(plan);

	if (left->GetTypes() != right->GetTypes()) {
		throw Exception("Type mismatch for UNION");
	}
	plan = make_unique<PhysicalUnion>(op, move(left), move(right));
}

void PhysicalPlanGenerator::GenerateExceptIntersect(LogicalOperator &op, JoinType join_type) {
	assert(op.children.size() == 2);

	VisitOperator(*op.children[0]);
	auto top = move(plan);
	VisitOperator(*op.children[1]);
	auto bottom = move(plan);

	auto top_types = top->GetTypes();
	if (top_types != bottom->GetTypes()) {
		throw Exception("Type mismatch for EXCEPT");
	}

	vector<JoinCondition> conditions;

	// create equality condition for all columns
	for (size_t i = 0; i < top_types.size(); i++) {
		JoinCondition cond;
		cond.comparison = ExpressionType::COMPARE_EQUAL;
		cond.left = make_unique_base<Expression, ColumnRefExpression>(top_types[i], i);
		cond.right = make_unique_base<Expression, ColumnRefExpression>(top_types[i], i);
		conditions.push_back(move(cond));
	}
	plan = make_unique<PhysicalHashJoin>(op, move(top), move(bottom), move(conditions), join_type);
}

void PhysicalPlanGenerator::Visit(LogicalExcept &op) {
	GenerateExceptIntersect(op, JoinType::ANTI);
}

void PhysicalPlanGenerator::Visit(LogicalIntersect &op) {
	GenerateExceptIntersect(op, JoinType::SEMI);
}

void PhysicalPlanGenerator::Visit(LogicalWindow &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);
	auto window = make_unique<PhysicalWindow>(op, move(op.expressions));
	if (plan) {
		window->children.push_back(move(plan));
	}
	this->plan = move(window);
}
