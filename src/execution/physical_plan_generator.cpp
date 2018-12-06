#include "execution/physical_plan_generator.hpp"

#include "execution/column_binding_resolver.hpp"
#include "execution/operator/list.hpp"
#include "parser/expression/list.hpp"
#include "planner/operator/list.hpp"
#include "storage/order_index.hpp"
#include "storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

void PhysicalPlanGenerator::CreatePlan(unique_ptr<LogicalOperator> logical) {
	// first resolve column references
	ColumnBindingResolver resolver;
	logical->Accept(&resolver);
	// now resolve types of all the operators
	logical->ResolveOperatorTypes();
	// then create the physical plan
	logical->Accept(this);
	if (!this->plan) {
		throw Exception("Unknown error in physical plan generation");
	}
}

void PhysicalPlanGenerator::Visit(LogicalAggregate &op) {
	LogicalOperatorVisitor::Visit(op);

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
	if (plan) {
		throw Exception("Cross product should be the first node of a plan!");
	}

	assert(op.children.size() == 2);

	op.children[0]->Accept(this);
	auto left = move(plan);
	op.children[1]->Accept(this);
	auto right = move(plan);

	plan = make_unique<PhysicalCrossProduct>(op, move(left), move(right));
}

void PhysicalPlanGenerator::Visit(LogicalDelete &op) {
	LogicalOperatorVisitor::Visit(op);

	if (!plan) {
		throw Exception("Delete node cannot be the first node of a plan!");
	}

	auto del = make_unique<PhysicalDelete>(op, *op.table, *op.table->storage);
	del->children.push_back(move(plan));
	this->plan = move(del);
}

void PhysicalPlanGenerator::Visit(LogicalUpdate &op) {
	LogicalOperatorVisitor::Visit(op);

	if (!plan) {
		throw Exception("Update node cannot be the first node of a plan!");
	}

	auto update = make_unique<PhysicalUpdate>(op, *op.table, *op.table->storage, op.columns, move(op.expressions));
	update->children.push_back(move(plan));
	this->plan = move(update);
}

void PhysicalPlanGenerator::Visit(LogicalCreate &op) {
	LogicalOperatorVisitor::Visit(op);

	if (plan) {
		throw Exception("CREATE node must be first node of the plan!");
	}

	this->plan = make_unique<PhysicalCreate>(op, op.schema, move(op.info));
}

void PhysicalPlanGenerator::Visit(LogicalCreateIndex &op) {
	LogicalOperatorVisitor::Visit(op);

	if (plan) {
		throw Exception("CREATE INDEX node must be first node of the plan!");
	}

	this->plan = make_unique<PhysicalCreateIndex>(op, op.table, op.column_ids, move(op.expressions), move(op.info));
}

//! Attempt to create an index scan from a filter + get, if possible
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
		auto &index = storage.indexes[j];
		// FIXME: assume every index is order index currently
		assert(index->type == IndexType::ORDER_INDEX);
		auto order_index = (OrderIndex *)index.get();
		// try to find a matching index for any of the filter expressions
		// currently we only look for equality predicates
		for (size_t i = 0; i < filter.expressions.size(); i++) {
			auto expr = filter.expressions[i].get();
			if (expr->type == ExpressionType::COMPARE_EQUAL ||
			    expr->type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
			    expr->type == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
			    expr->type == ExpressionType::COMPARE_LESSTHAN || expr->type == ExpressionType::COMPARE_GREATERTHAN) {
				auto comparison = (ComparisonExpression *)expr;
				unique_ptr<Expression> value;
				// check if any of the two children is an index
				if (order_index->expressions[0]->Equals(comparison->children[0].get()) &&
				    comparison->children[1]->type == ExpressionType::VALUE_CONSTANT) {
					value = move(comparison->children[1]);
				} else if (order_index->expressions[0]->Equals(comparison->children[1].get()) &&
				           comparison->children[0]->type == ExpressionType::VALUE_CONSTANT) {
					value = move(comparison->children[0]);
					expr->type = ComparisonExpression::FlipComparisionExpression(expr->type);
				}
				if (value) {
					// we can use the index here!
					// create an index scan
					// FIXME: use statistics to see if it is worth it
					unique_ptr<Expression> low_value, high_value;
					auto expr_low = expr->type;
					auto expr_high = expr->type;
					if (expr->type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
					    expr->type == ExpressionType::COMPARE_GREATERTHAN ||
					    expr->type == ExpressionType::COMPARE_EQUAL) {
						low_value = move(value);
					} else if (expr->type == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
					           expr->type == ExpressionType::COMPARE_LESSTHAN) {
						high_value = move(value);
					}
					filter.expressions.erase(filter.expressions.begin() + i);
					if (!(expr_low == ExpressionType::COMPARE_EQUAL)) {
						for (size_t k = i; k < filter.expressions.size(); k++) {
							expr = filter.expressions[k].get();
							comparison = (ComparisonExpression *)expr;
							value = nullptr;
							if (order_index->expressions[0]->Equals(comparison->children[0].get()) &&
							    comparison->children[1]->type == ExpressionType::VALUE_CONSTANT) {
								value = move(comparison->children[1]);
							} else if (order_index->expressions[0]->Equals(comparison->children[1].get()) &&
							           comparison->children[0]->type == ExpressionType::VALUE_CONSTANT) {
								value = move(comparison->children[0]);
								expr->type = ComparisonExpression::FlipComparisionExpression(expr->type);
							}
							if (value) {
								if ((expr->type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
								     expr->type == ExpressionType::COMPARE_GREATERTHAN) &&
								    ((ConstantExpression *)value.get())->value >=
								        ((ConstantExpression *)low_value.get())->value) {
									if (((ConstantExpression *)value.get())->value ==
									        ((ConstantExpression *)low_value.get())->value &&
									    (expr_low == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
									     expr->type == ExpressionType::COMPARE_GREATERTHANOREQUALTO)) {
										expr_low = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
									} else {
										low_value = move(value);
										expr_low = expr->type;
									}

								} else if ((expr->type == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
								            expr->type == ExpressionType::COMPARE_LESSTHAN) &&
								           ((ConstantExpression *)value.get())->value <=
								               ((ConstantExpression *)high_value.get())->value) {
									if (((ConstantExpression *)value.get())->value ==
									        ((ConstantExpression *)high_value.get())->value &&
									    (expr_high == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
									     expr->type == ExpressionType::COMPARE_LESSTHANOREQUALTO)) {
										expr_high = ExpressionType::COMPARE_LESSTHANOREQUALTO;
									} else {
										high_value = move(value);
										expr_high = expr->type;
									}
								}
								filter.expressions.erase(filter.expressions.begin() + k);
								k--;
							}
						}
					}
					auto index_scan =
					    make_unique<PhysicalIndexScan>(scan, *scan.table, *scan.table->storage, *order_index,
					                                   scan.column_ids, move(low_value), move(high_value));
					index_scan->low_expression_type = expr_low;
					index_scan->high_expression_type = expr_high;
					return move(index_scan);
				}
			}
		}
	}
	return nullptr;
}

void PhysicalPlanGenerator::Visit(LogicalFilter &op) {
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
			LogicalOperatorVisitor::Visit(op);
		}
	} else {
		LogicalOperatorVisitor::Visit(op);
	}

	if (!plan) {
		throw Exception("Filter cannot be the first node of a plan!");
	}

	if (op.expressions.size() > 0) {
		// create a filter if there is anything to filter
		auto filter = make_unique<PhysicalFilter>(op, move(op.expressions));
		filter->children.push_back(move(plan));
		this->plan = move(filter);
	}
}

void PhysicalPlanGenerator::Visit(LogicalGet &op) {
	LogicalOperatorVisitor::Visit(op);

	if (!op.table) {
		vector<TypeId> types = {TypeId::BIGINT};
		this->plan = make_unique<PhysicalDummyScan>(types);
		return;
	}
	auto scan = make_unique<PhysicalTableScan>(op, *op.table, *op.table->storage, op.column_ids);
	if (plan) {
		throw Exception("Scan has to be the first node of a plan!");
	}
	this->plan = move(scan);
}

void PhysicalPlanGenerator::Visit(LogicalJoin &op) {
	if (plan) {
		throw Exception("Cross product should be the first node of a plan!");
	}

	// now visit the children
	assert(op.children.size() == 2);
	assert(op.conditions.size() > 0);

	op.children[0]->Accept(this);
	auto left = move(plan);
	op.children[1]->Accept(this);
	auto right = move(plan);

	bool has_equality = false;
	bool has_inequality = false;
	for (auto &cond : op.conditions) {
		cond.left->Accept(this);
		cond.right->Accept(this);
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
	LogicalOperatorVisitor::Visit(op);

	auto limit = make_unique<PhysicalLimit>(op, op.limit, op.offset);
	if (!plan) {
		throw Exception("Limit cannot be the first node of a plan!");
	}
	limit->children.push_back(move(plan));
	this->plan = move(limit);
}

void PhysicalPlanGenerator::Visit(LogicalOrder &op) {
	LogicalOperatorVisitor::Visit(op);

	if (!plan) {
		throw Exception("Order cannot be the first node of a plan!");
	}

	auto order = make_unique<PhysicalOrder>(op, move(op.description));
	order->children.push_back(move(plan));
	this->plan = move(order);
}

void PhysicalPlanGenerator::Visit(LogicalProjection &op) {
	LogicalOperatorVisitor::Visit(op);

	auto projection = make_unique<PhysicalProjection>(op, move(op.expressions));
	if (plan) {
		projection->children.push_back(move(plan));
	}
	this->plan = move(projection);
}

void PhysicalPlanGenerator::Visit(LogicalInsert &op) {
	LogicalOperatorVisitor::Visit(op);

	auto insertion = make_unique<PhysicalInsert>(op, op.table, move(op.insert_values), op.column_index_map);
	if (plan) {
		insertion->children.push_back(move(plan));
	}
	this->plan = move(insertion);
}

void PhysicalPlanGenerator::Visit(LogicalPruneColumns &op) {
	LogicalOperatorVisitor::Visit(op);

	if (!plan) {
		throw Exception("Prune columns cannot be the first node of a plan!");
	}
	if (plan->GetTypes().size() > op.column_limit) {
		// only prune if we need to
		auto node = make_unique<PhysicalPruneColumns>(op, op.column_limit);
		node->children.push_back(move(plan));
		this->plan = move(node);
	}
}

void PhysicalPlanGenerator::Visit(LogicalTableFunction &op) {
	LogicalOperatorVisitor::Visit(op);

	if (plan) {
		throw Exception("Table function has to be first node of the plan!");
	}
	this->plan = make_unique<PhysicalTableFunction>(op, op.function, move(op.function_call));
}

void PhysicalPlanGenerator::Visit(LogicalCopy &op) {
	LogicalOperatorVisitor::Visit(op);

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
	LogicalOperatorVisitor::Visit(op);

	if (plan) {
		op.physical_plan = plan->ToString();
	}

	vector<string> keys = {"logical_plan", "logical_opt", "physical_plan"};
	vector<string> values = {op.logical_plan_unopt, logical_plan_opt, op.physical_plan};

	this->plan = make_unique<PhysicalExplain>(op, keys, values);
}

void PhysicalPlanGenerator::Visit(LogicalUnion &op) {
	assert(op.children.size() == 2);

	op.children[0]->Accept(this);
	auto left = move(plan);
	op.children[1]->Accept(this);
	auto right = move(plan);

	if (left->GetTypes() != right->GetTypes()) {
		throw Exception("Type mismatch for UNION");
	}
	plan = make_unique<PhysicalUnion>(op, move(left), move(right));
}

static void GenerateExceptIntersect(PhysicalPlanGenerator *generator, LogicalOperator &op, JoinType join_type) {
	assert(op.children.size() == 2);
	assert(generator);

	op.children[0]->Accept(generator);
	auto top = move(generator->plan);
	op.children[1]->Accept(generator);
	auto bottom = move(generator->plan);

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
	generator->plan = make_unique<PhysicalHashJoin>(op, move(top), move(bottom), move(conditions), join_type);
}

void PhysicalPlanGenerator::Visit(LogicalExcept &op) {
	GenerateExceptIntersect(this, op, JoinType::ANTI);
}

void PhysicalPlanGenerator::Visit(LogicalIntersect &op) {
	GenerateExceptIntersect(this, op, JoinType::SEMI);
}


void PhysicalPlanGenerator::Visit(LogicalWindow &op) {
	auto window = make_unique<PhysicalWindow>(op, move(op.expressions));
	if (plan) {
		window->children.push_back(move(plan));
	}
	this->plan = move(window);
}

unique_ptr<Expression> PhysicalPlanGenerator::Visit(SubqueryExpression &expr) {
	PhysicalPlanGenerator generator(context, this);
	generator.CreatePlan(move(expr.op));
	expr.plan = move(generator.plan);
	return nullptr;
}
