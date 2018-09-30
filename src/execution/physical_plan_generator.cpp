
#include "execution/physical_plan_generator.hpp"
#include "execution/column_binding_resolver.hpp"

#include "execution/operator/list.hpp"
#include "parser/expression/list.hpp"
#include "planner/operator/list.hpp"

#include "storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

bool PhysicalPlanGenerator::CreatePlan(unique_ptr<LogicalOperator> logical) {
	this->success = false;
	try {
		// first resolve column references
		ColumnBindingResolver resolver;
		logical->Accept(&resolver);
		// then create the physical plan
		logical->Accept(this);
		if (!this->plan) {
			throw Exception("Unknown error in physical plan generation");
		}
		this->success = true;
	} catch (Exception &ex) {
		this->message = ex.GetMessage();
	} catch (...) {
		this->message = "UNHANDLED EXCEPTION TYPE THROWN IN PLANNER!";
	}
	return this->success;
}

void PhysicalPlanGenerator::Visit(LogicalAggregate &op) {
	LogicalOperatorVisitor::Visit(op);

	if (op.groups.size() == 0) {
		// no groups
		if (!plan) {
			// and no FROM clause, use a dummy aggregate
			auto groupby =
			    make_unique<PhysicalHashAggregate>(move(op.expressions));
			this->plan = move(groupby);
		} else {
			// but there is a FROM clause
			// special case: aggregate entire columns together
			auto groupby =
			    make_unique<PhysicalHashAggregate>(move(op.expressions));
			groupby->children.push_back(move(plan));
			this->plan = move(groupby);
		}
	} else {
		// groups! create a GROUP BY aggregator
		if (!plan) {
			throw Exception("Cannot have GROUP BY without FROM clause!");
		}

		auto groupby = make_unique<PhysicalHashAggregate>(move(op.expressions),
		                                                  move(op.groups));
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

	plan = make_unique<PhysicalCrossProduct>(move(left), move(right));
}

void PhysicalPlanGenerator::Visit(LogicalDelete &op) {
	LogicalOperatorVisitor::Visit(op);

	if (!plan) {
		throw Exception("Delete node cannot be the first node of a plan!");
	}

	auto del = make_unique<PhysicalDelete>(*op.table->storage);
	del->children.push_back(move(plan));
	this->plan = move(del);
}

void PhysicalPlanGenerator::Visit(LogicalUpdate &op) {
	LogicalOperatorVisitor::Visit(op);

	if (!plan) {
		throw Exception("Update node cannot be the first node of a plan!");
	}

	auto update = make_unique<PhysicalUpdate>(*op.table->storage, op.columns,
	                                          move(op.expressions));
	update->children.push_back(move(plan));
	this->plan = move(update);
}

void PhysicalPlanGenerator::Visit(LogicalFilter &op) {
	LogicalOperatorVisitor::Visit(op);

	if (!plan) {
		throw Exception("Filter cannot be the first node of a plan!");
	}

	auto filter = make_unique<PhysicalFilter>(move(op.expressions));
	filter->children.push_back(move(plan));
	this->plan = move(filter);
}

void PhysicalPlanGenerator::Visit(LogicalGet &op) {
	LogicalOperatorVisitor::Visit(op);

	if (!op.table) {
		this->plan = make_unique<PhysicalDummyScan>();
		return;
	}

	auto scan =
	    make_unique<PhysicalTableScan>(*op.table->storage, op.column_ids);
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

	op.children[0]->Accept(this);
	auto left = move(plan);
	op.children[1]->Accept(this);
	auto right = move(plan);

	for (auto &cond : op.conditions) {
		cond.left->Accept(this);
		cond.right->Accept(this);
	}

	plan = make_unique<PhysicalNestedLoopJoin>(move(left), move(right),
	                                           move(op.conditions), op.type);
}

void PhysicalPlanGenerator::Visit(LogicalLimit &op) {
	LogicalOperatorVisitor::Visit(op);

	auto limit = make_unique<PhysicalLimit>(op.limit, op.offset);
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

	auto order = make_unique<PhysicalOrder>(move(op.description));
	order->children.push_back(move(plan));
	this->plan = move(order);
}

void PhysicalPlanGenerator::Visit(LogicalProjection &op) {
	LogicalOperatorVisitor::Visit(op);

	auto projection = make_unique<PhysicalProjection>(move(op.expressions));
	if (plan) {
		projection->children.push_back(move(plan));
	}
	this->plan = move(projection);
}

void PhysicalPlanGenerator::Visit(LogicalInsert &op) {
	LogicalOperatorVisitor::Visit(op);

	auto insertion = make_unique<PhysicalInsert>(
	    op.table, move(op.insert_values), op.column_index_map);
	if (plan) {
		insertion->children.push_back(move(plan));
	}
	this->plan = move(insertion);
}

void PhysicalPlanGenerator::Visit(SubqueryExpression &expr) {
	PhysicalPlanGenerator generator(context, this);
	generator.CreatePlan(move(expr.op));
	expr.plan = move(generator.plan);
}

void PhysicalPlanGenerator::Visit(LogicalCopy &op) {
	LogicalOperatorVisitor::Visit(op);

	if (plan) {
		auto copy = make_unique<PhysicalCopy>(
		    move(op.file_path), move(op.is_from), move(op.delimiter),
		    move(op.quote), move(op.escape));
		copy->children.push_back(move(plan));
		this->plan = move(copy);
	} else {
		auto copy = make_unique<PhysicalCopy>(
		    op.table, move(op.file_path), move(op.is_from), move(op.delimiter),
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

	// Construct a dummy plan that just returns the plan strings
	auto scan = make_unique<PhysicalDummyScan>();

	vector<TypeId> types = {TypeId::VARCHAR, TypeId::VARCHAR};
	scan->chunk.Initialize(types, false);
	scan->chunk.count = 3;

	scan->chunk.data[0].count = 3;
	scan->chunk.data[0].SetStringValue(0, "logical_plan");
	scan->chunk.data[0].SetStringValue(1, "logical_opt");
	scan->chunk.data[0].SetStringValue(2, "physical_plan");

	scan->chunk.data[1].count = 3;
	scan->chunk.data[1].SetStringValue(0, op.logical_plan_unopt.c_str());
	scan->chunk.data[1].SetStringValue(1, logical_plan_opt.c_str());
	scan->chunk.data[1].SetStringValue(2, op.physical_plan.c_str());

	scan->chunk.Verify();

	std::vector<std::unique_ptr<Expression>> select_list;
	select_list.push_back(make_unique<ColumnRefExpression>(TypeId::VARCHAR, 0));
	select_list.push_back(make_unique<ColumnRefExpression>(TypeId::VARCHAR, 1));

	select_list[0]->alias = "explain_key";
	select_list[1]->alias = "explain_value";

	auto projection = make_unique<PhysicalProjection>(move(select_list));
	projection->children.push_back(move(scan));
	this->plan = move(projection);
}

void PhysicalPlanGenerator::Visit(LogicalUnion &op) {
	assert(op.children.size() == 2);

	op.children[0]->Accept(this);
	auto top = move(plan);
	op.children[1]->Accept(this);
	auto bottom = move(plan);

	if (top->GetTypes() != bottom->GetTypes()) {
		throw Exception("Type mismatch for UNION");
	}
	plan = make_unique<PhysicalUnion>(move(top), move(bottom));
}
