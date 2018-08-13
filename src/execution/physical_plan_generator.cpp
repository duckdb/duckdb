
#include "execution/physical_plan_generator.hpp"

#include "execution/operator/physical_copy.hpp"
#include "execution/operator/physical_filter.hpp"
#include "execution/operator/physical_hash_aggregate.hpp"
#include "execution/operator/physical_insert.hpp"
#include "execution/operator/physical_limit.hpp"
#include "execution/operator/physical_list.hpp"
#include "execution/operator/physical_order.hpp"
#include "execution/operator/physical_projection.hpp"
#include "execution/operator/physical_table_scan.hpp"
#include "parser/expression/expression_list.hpp"

#include "planner/operator/logical_aggregate.hpp"
#include "planner/operator/logical_copy.hpp"
#include "planner/operator/logical_distinct.hpp"
#include "planner/operator/logical_filter.hpp"
#include "planner/operator/logical_get.hpp"
#include "planner/operator/logical_insert.hpp"
#include "planner/operator/logical_limit.hpp"
#include "planner/operator/logical_list.hpp"
#include "planner/operator/logical_order.hpp"
#include "planner/operator/logical_projection.hpp"

#include "storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

bool PhysicalPlanGenerator::CreatePlan(unique_ptr<LogicalOperator> logical,
                                       unique_ptr<BindContext> context) {
	this->context = move(context);
	this->success = false;
	try {
		logical->Accept(this);
		if (!this->plan) {
			throw Exception("Unknown error in physical plan generation");
		}
		this->success = true;
	} catch (Exception ex) {
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
			    make_unique<PhysicalHashAggregate>(move(op.select_list));
			this->plan = move(groupby);
		} else {
			// but there is a FROM clause
			// special case: aggregate entire columns together
			auto groupby =
			    make_unique<PhysicalHashAggregate>(move(op.select_list));
			groupby->children.push_back(move(plan));
			this->plan = move(groupby);
		}
	} else {
		// groups! create a GROUP BY aggregator
		if (!plan) {
			throw Exception("Cannot have GROUP BY without FROM clause!");
		}

		auto groupby = make_unique<PhysicalHashAggregate>(move(op.select_list),
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
	auto left_map = table_index_map;
	table_index_map.clear();

	op.children[1]->Accept(this);
	auto right = move(plan);
	auto right_map = table_index_map;
	table_index_map.clear();

	size_t left_columns = 0;
	for (auto &entry : left_map) {
		left_columns += entry.second.column_count;
		table_index_map[entry.first] = entry.second;
	}
	for (auto &entry : right_map) {
		table_index_map[entry.first] = {left_columns +
		                                    entry.second.column_offset,
		                                entry.second.column_count};
	}

	plan = make_unique<PhysicalCrossProduct>(move(left), move(right));
}

void PhysicalPlanGenerator::Visit(LogicalDistinct &op) {
	LogicalOperatorVisitor::Visit(op);

	throw NotImplementedException("distinct clause");
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

	std::vector<size_t> column_ids;
	// look in the context for this table which columns are required
	for (auto &bound_column : context->bound_columns[op.alias]) {
		column_ids.push_back(op.table->name_map[bound_column]);
	}
	if (column_ids.size() == 0) {
		// no column ids selected
		// the query is like SELECT COUNT(*) FROM table, or SELECT 42 FROM table
		// return just the first column
		column_ids.push_back(0);
	}

	auto table_entry = context->regular_table_alias_map.find(op.alias);
	assert(table_entry != context->regular_table_alias_map.end());
	size_t table_index = table_entry->second.index;
	table_index_map[table_index] = {0, column_ids.size()};

	auto scan = make_unique<PhysicalTableScan>(op.table->storage, column_ids);
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
	auto left_map = table_index_map;
	table_index_map.clear();

	op.children[1]->Accept(this);
	auto right = move(plan);
	auto right_map = table_index_map;
	table_index_map.clear();

	size_t left_columns = 0;
	for (auto &entry : left_map) {
		left_columns += entry.second.column_count;
		table_index_map[entry.first] = entry.second;
	}
	for (auto &entry : right_map) {
		table_index_map[entry.first] = {left_columns +
		                                    entry.second.column_offset,
		                                entry.second.column_count};
	}

	// now visit the child expressions to resolve column reference indices
	op.condition->Accept(this);

	plan = make_unique<PhysicalNestedLoopJoin>(move(left), move(right),
	                                           move(op.condition), op.type);
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

	auto projection = make_unique<PhysicalProjection>(move(op.select_list));
	if (plan) {
		projection->children.push_back(move(plan));
	}
	this->plan = move(projection);
}

void PhysicalPlanGenerator::Visit(LogicalInsert &op) {
	LogicalOperatorVisitor::Visit(op);

	auto insertion = make_unique<PhysicalInsert>(op.table, move(op.value_list));
	if (plan) {
		throw Exception("Insert should be root node");
	}
	this->plan = move(insertion);
}

void PhysicalPlanGenerator::Visit(SubqueryExpression &expr) {
	PhysicalPlanGenerator generator(catalog, this);
	generator.CreatePlan(move(expr.op), move(expr.context));
	expr.plan = move(generator.plan);
}

void PhysicalPlanGenerator::Visit(ColumnRefExpression &expr) {
	if (expr.table_index == (size_t)-1) {
		return;
	}

	// we have to check the upper table index map if this expression refers to a
	// parent query
	PhysicalPlanGenerator *generator = this;
	auto depth = expr.depth;
	while (depth > 0) {
		generator = generator->parent;
		assert(generator);
		depth--;
	}
	auto entry = generator->table_index_map.find(expr.table_index);
	if (entry == generator->table_index_map.end()) {
		throw Exception("Could not bind to table of this index at this point "
		                "in the query!");
	}
	expr.index += entry->second.column_offset;
}

void PhysicalPlanGenerator::Visit(LogicalCopy &op) {
	LogicalOperatorVisitor::Visit(op);

	auto copy = make_unique<PhysicalCopy>(op.table, move(op.file_path),
	                                      move(op.is_from), move(op.delimiter),
	                                      move(op.quote), move(op.escape));
	if (plan) {
		throw Exception("Copy should be root node");
	}
	this->plan = move(copy);
}