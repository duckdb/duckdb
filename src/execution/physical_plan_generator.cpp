#include "execution/physical_plan_generator.hpp"

#include "execution/column_binding_resolver.hpp"
#include "execution/operator/join/physical_blockwise_nl_join.hpp"
#include "execution/operator/list.hpp"
#include "parser/expression/list.hpp"
#include "planner/operator/list.hpp"
#include "storage/order_index.hpp"
#include "storage/storage_manager.hpp"

#include <unordered_set>

using namespace duckdb;
using namespace std;

void PhysicalPlanGenerator::CreatePlan(unique_ptr<LogicalOperator> op) {
	// first resolve column references
	ColumnBindingResolver resolver;
	resolver.VisitOperator(*op);
	// now resolve types of all the operators
	op->ResolveOperatorTypes();
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
	case LogicalOperatorType::EMPTY_RESULT:
		Visit((LogicalEmptyResult &)op);
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
	case LogicalOperatorType::ANY_JOIN:
		Visit((LogicalAnyJoin &)op);
		break;
	case LogicalOperatorType::DELIM_JOIN:
		Visit((LogicalDelimJoin &)op);
		break;
	case LogicalOperatorType::COMPARISON_JOIN:
		Visit((LogicalComparisonJoin &)op);
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
	case LogicalOperatorType::CHUNK_GET:
		Visit((LogicalChunkGet &)op);
		break;
	case LogicalOperatorType::DELIM_GET:
		Visit((LogicalDelimGet &)op);
		break;
	case LogicalOperatorType::UPDATE:
		Visit((LogicalUpdate &)op);
		break;
	case LogicalOperatorType::CREATE_TABLE:
		Visit((LogicalCreateTable &)op);
		break;
	case LogicalOperatorType::CREATE_INDEX:
		Visit((LogicalCreateIndex &)op);
		break;
	case LogicalOperatorType::EXPLAIN:
		Visit((LogicalExplain &)op);
		break;
	case LogicalOperatorType::DISTINCT:
		Visit((LogicalDistinct &)op);
		break;
	case LogicalOperatorType::PRUNE_COLUMNS:
		Visit((LogicalPruneColumns &)op);
		break;
	case LogicalOperatorType::PREPARE:
		Visit((LogicalPrepare &)op);
		break;
	case LogicalOperatorType::EXECUTE:
		Visit((LogicalExecute &)op);
		break;
	case LogicalOperatorType::SUBQUERY:
		LogicalOperatorVisitor::VisitOperator(op);
		break;
	case LogicalOperatorType::JOIN:
	case LogicalOperatorType::INVALID:
		assert(0);
		break;
	}
}

void PhysicalPlanGenerator::Visit(LogicalAggregate &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	if (op.groups.size() == 0) {
		// no groups
		if (!plan) {
			// and no FROM clause, use a dummy aggregate
			auto groupby = make_unique<PhysicalHashAggregate>(op.types, move(op.expressions));
			this->plan = move(groupby);
		} else {
			// but there is a FROM clause
			// special case: aggregate entire columns together
			auto groupby = make_unique<PhysicalHashAggregate>(op.types, move(op.expressions));
			groupby->children.push_back(move(plan));
			this->plan = move(groupby);
		}
	} else {
		// groups! create a GROUP BY aggregator
		if (!plan) {
			throw Exception("Cannot have GROUP BY without FROM clause!");
		}

		auto groupby = make_unique<PhysicalHashAggregate>(op.types, move(op.expressions), move(op.groups));
		groupby->children.push_back(move(plan));
		this->plan = move(groupby);
	}
}

void PhysicalPlanGenerator::Visit(LogicalChunkGet &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	// create a PhysicalChunkScan pointing towards the owned collection
	assert(!plan);
	assert(op.collection);
	auto chunk_scan = make_unique<PhysicalChunkScan>(op.types, PhysicalOperatorType::CHUNK_SCAN);
	chunk_scan->owned_collection = move(op.collection);
	chunk_scan->collection = chunk_scan->owned_collection.get();
	plan = move(chunk_scan);
}

void PhysicalPlanGenerator::Visit(LogicalDelimGet &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	// create a PhysicalChunkScan without an owned_collection, the collection will be added later
	assert(!plan);
	auto chunk_scan = make_unique<PhysicalChunkScan>(op.types, PhysicalOperatorType::DELIM_SCAN);
	plan = move(chunk_scan);
}

static unique_ptr<PhysicalOperator> CreateDistinct(unique_ptr<PhysicalOperator> child) {
	assert(child);
	// create a PhysicalHashAggregate that groups by the input columns
	auto &types = child->GetTypes();
	vector<unique_ptr<Expression>> groups, expressions;
	for (size_t i = 0; i < types.size(); i++) {
		groups.push_back(make_unique<BoundExpression>(types[i], i));
	}
	auto groupby =
	    make_unique<PhysicalHashAggregate>(types, move(expressions), move(groups), PhysicalOperatorType::DISTINCT);
	groupby->children.push_back(move(child));
	return move(groupby);
}

void PhysicalPlanGenerator::Visit(LogicalDistinct &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);
	assert(plan);
	this->plan = CreateDistinct(move(plan));
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

	// get the index of the row_id column
	assert(op.expressions.size() == 1);
	assert(op.expressions[0]->type == ExpressionType::BOUND_REF);
	auto &bound_ref = (BoundExpression &)*op.expressions[0];

	auto del = make_unique<PhysicalDelete>(op, *op.table, *op.table->storage, bound_ref.index);
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

void PhysicalPlanGenerator::Visit(LogicalCreateTable &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	auto create = make_unique<PhysicalCreateTable>(op, op.schema, move(op.info));
	if (plan) {
		create->children.push_back(move(plan));
	}
	this->plan = move(create);
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

void PhysicalPlanGenerator::Visit(LogicalEmptyResult &op) {
	plan = make_unique<PhysicalEmptyResult>(op.types);
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
		this->plan = make_unique<PhysicalDummyScan>(op.types);
		return;
	}
	auto scan = make_unique<PhysicalTableScan>(op, *op.table, *op.table->storage, op.column_ids);
	assert(!plan); // Scan has to be the first node of a plan!
	this->plan = move(scan);
}

static void GatherDelimScans(PhysicalOperator *op, vector<PhysicalOperator *> &delim_scans) {
	assert(op);
	if (op->type == PhysicalOperatorType::DELIM_SCAN) {
		delim_scans.push_back(op);
	}
	for (auto &child : op->children) {
		GatherDelimScans(child.get(), delim_scans);
	}
}

void PhysicalPlanGenerator::Visit(LogicalDelimJoin &op) {
	// first create the underlying join
	Visit((LogicalComparisonJoin &)op);
	// this should create a join, not a cross product
	assert(plan && plan->type != PhysicalOperatorType::CROSS_PRODUCT);
	// duplicate eliminated join
	// first gather the scans on the duplicate eliminated data set from the RHS
	vector<PhysicalOperator *> delim_scans;
	GatherDelimScans(plan->children[1].get(), delim_scans);
	if (delim_scans.size() == 0) {
		// no duplicate eliminated scans in the RHS!
		// in this case we don't need to create a delim join
		// just push the normal join
		return;
	}
	vector<TypeId> delim_types;
	for (auto &delim_expr : op.duplicate_eliminated_columns) {
		delim_types.push_back(delim_expr->return_type);
	}
	if (op.type == JoinType::MARK) {
		assert(plan->type == PhysicalOperatorType::HASH_JOIN);
		auto &hash_join = (PhysicalHashJoin &)*plan;
		// correlated eliminated MARK join
		// a correlated MARK join should always have exactly one non-correlated column
		// (namely the actual predicate of the ANY() expression)
		assert(delim_types.size() + 1 == hash_join.conditions.size());
		// push duplicate eliminated columns into the hash table:
		// - these columns will be considered as equal for NULL values AND
		// - the has_null and has_result flags will be grouped by these columns
		auto &info = hash_join.hash_table->correlated_mark_join_info;

		vector<TypeId> payload_types = {TypeId::BIGINT, TypeId::BIGINT}; // COUNT types
		vector<ExpressionType> aggregate_types = {ExpressionType::AGGREGATE_COUNT_STAR,
		                                          ExpressionType::AGGREGATE_COUNT};

		info.correlated_counts = make_unique<SuperLargeHashTable>(1024, delim_types, payload_types, aggregate_types);
		info.correlated_types = delim_types;
		// FIXME: these can be initialized "empty" (without allocating empty vectors)
		info.group_chunk.Initialize(delim_types);
		info.payload_chunk.Initialize(payload_types);
		info.result_chunk.Initialize(payload_types);
	}
	// now create the duplicate eliminated join
	auto delim_join = make_unique<PhysicalDelimJoin>(op, move(plan), delim_scans);
	// we still have to create the DISTINCT clause that is used to generate the duplicate eliminated chunk
	// we create a ChunkCollectionScan that pulls from the delim_join LHS
	auto chunk_scan = make_unique<PhysicalChunkScan>(delim_join->children[0]->GetTypes(), PhysicalOperatorType::CHUNK_SCAN);
	chunk_scan->collection = &delim_join->lhs_data;
	// now we need to create a projection that projects only the duplicate eliminated columns
	assert(op.duplicate_eliminated_columns.size() > 0);
	auto projection = make_unique<PhysicalProjection>(delim_types, move(op.duplicate_eliminated_columns));
	projection->children.push_back(move(chunk_scan));
	// finally create the distinct clause on top of the projection
	delim_join->distinct = CreateDistinct(move(projection));
	plan = move(delim_join);
}

void PhysicalPlanGenerator::Visit(LogicalAnyJoin &op) {
	assert(!plan); // join should be the first node of a plan!

	// first visit the child nodes
	assert(op.children.size() == 2);
	VisitOperator(*op.children[0]);
	auto left = move(plan);
	VisitOperator(*op.children[1]);
	auto right = move(plan);

	assert(op.condition);
	// create the blockwise NL join
	plan = make_unique<PhysicalBlockwiseNLJoin>(op, move(left), move(right), move(op.condition), op.type);
}

void PhysicalPlanGenerator::Visit(LogicalComparisonJoin &op) {
	assert(!plan); // join should be the first node of a plan!

	// now visit the children
	assert(op.children.size() == 2);

	VisitOperator(*op.children[0]);
	auto left = move(plan);
	VisitOperator(*op.children[1]);
	auto right = move(plan);

	if (op.conditions.size() == 0) {
		// no conditions: insert a cross product
		plan = make_unique<PhysicalCrossProduct>(op, move(left), move(right));
		return;
	}

	bool has_equality = false;
	bool has_inequality = false;
	bool has_null_equal_conditions = false;
	for (auto &cond : op.conditions) {
		VisitExpression(&cond.right);
		if (cond.comparison == ExpressionType::COMPARE_EQUAL) {
			has_equality = true;
		}
		if (cond.comparison == ExpressionType::COMPARE_NOTEQUAL) {
			has_inequality = true;
		}
		if (cond.null_values_are_equal) {
			has_null_equal_conditions = true;
			assert(cond.comparison == ExpressionType::COMPARE_EQUAL);
		}
	}

	assert(left);
	assert(right);
	if (has_equality) {
		// equality join: use hash join
		plan = make_unique<PhysicalHashJoin>(op, move(left), move(right), move(op.conditions), op.type);
	} else {
		assert(!has_null_equal_conditions); // don't support this for anything but hash joins for now
		if (op.conditions.size() == 1 && (op.type == JoinType::MARK || op.type == JoinType::INNER) && !has_inequality) {
			// range join: use piecewise merge join
			plan = make_unique<PhysicalPiecewiseMergeJoin>(op, move(left), move(right), move(op.conditions), op.type);
		} else {
			// inequality join: use nested loop
			plan = make_unique<PhysicalNestedLoopJoin>(op, move(left), move(right), move(op.conditions), op.type);
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

// we use this to find parameters in the prepared statement
class PrepareParameterVisitor : public SQLNodeVisitor {
public:
	PrepareParameterVisitor(unordered_map<size_t, ParameterExpression *> &parameter_expression_map)
	    : parameter_expression_map(parameter_expression_map) {
	}

protected:
	using SQLNodeVisitor::Visit;
	void Visit(ParameterExpression &expr) override {
		if (expr.return_type == TypeId::INVALID) {
			throw Exception("Could not determine type for prepared statement parameter. Consider using a CAST on it.");
		}
		auto it = parameter_expression_map.find(expr.parameter_nr);
		if (it != parameter_expression_map.end()) {
			throw Exception("Duplicate parameter index. Use $1, $2 etc. to differentiate.");
		}
		parameter_expression_map[expr.parameter_nr] = &expr;
	}

private:
	unordered_map<size_t, ParameterExpression *> &parameter_expression_map;
};

void PhysicalPlanGenerator::Visit(LogicalPrepare &op) {
	assert(!plan); // prepare has to be top node
	assert(op.children.size() == 1);
	// create the physical plan for the prepare statement.

	auto entry = make_unique<PreparedStatementCatalogEntry>(op.name, op.statement_type);
	entry->names = op.children[0]->GetNames();

	// find tables
	op.GetTableBindings(entry->tables);

	// generate physical plan
	VisitOperator(*op.children[0]);
	assert(plan);

	// find parameters and add to info
	PrepareParameterVisitor ppv(entry->parameter_expression_map);
	plan->Accept(&ppv);

	entry->types = plan->types;
	entry->plan = move(plan);

	// now store plan in context
	if (!context.prepared_statements->CreateEntry(context.ActiveTransaction(), op.name, move(entry))) {
		throw Exception("Failed to prepare statement");
	}
	vector<TypeId> prep_return_types = {TypeId::BOOLEAN};
	plan = make_unique<PhysicalDummyScan>(prep_return_types);
}

void PhysicalPlanGenerator::Visit(LogicalExecute &op) {
	assert(!plan); // execute has to be top node
	plan = make_unique<PhysicalExecute>(op.prep->plan.get());
}

void PhysicalPlanGenerator::Visit(LogicalTableFunction &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	assert(!plan); // Table function has to be first node of the plan!
	plan = make_unique<PhysicalTableFunction>(op, op.function, move(op.function_call));
}

void PhysicalPlanGenerator::Visit(LogicalCopy &op) {
	LogicalOperatorVisitor::VisitOperatorChildren(op);

	if (plan) {
		// COPY from select statement
		assert(!op.table);
		auto copy = make_unique<PhysicalCopy>(op, move(op.info));
		copy->names = op.names;
		copy->children.push_back(move(plan));
		plan = move(copy);
	} else {
		// COPY from table
		auto copy = make_unique<PhysicalCopy>(op, op.table, move(op.info));
		plan = move(copy);
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
		cond.left = make_unique<BoundExpression>(top_types[i], i);
		cond.right = make_unique<BoundExpression>(top_types[i], i);
		cond.null_values_are_equal = true;
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
