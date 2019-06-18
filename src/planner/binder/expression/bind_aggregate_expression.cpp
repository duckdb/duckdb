#include "parser/expression/aggregate_expression.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"
#include "planner/expression/bound_columnref_expression.hpp"
#include "planner/expression_binder/aggregate_binder.hpp"
#include "planner/expression_binder/select_binder.hpp"
#include "planner/query_node/bound_select_node.hpp"

using namespace duckdb;
using namespace std;

static SQLType ResolveSumType(SQLType input_type) {
	switch (input_type.id) {
	case SQLTypeId::SQLNULL:
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
		return SQLType(SQLTypeId::BIGINT);
	case SQLTypeId::FLOAT:
		return SQLType(SQLTypeId::FLOAT);
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return SQLType(SQLTypeId::DECIMAL);
	default:
		throw BinderException("Unsupported SQLType %s for SUM aggregate", SQLTypeToString(input_type).c_str());
	}
}

static SQLType ResolveSTDDevType(SQLType input_type) {
	switch (input_type.id) {
	case SQLTypeId::SQLNULL:
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return SQLType(SQLTypeId::DECIMAL);
	default:
		throw BinderException("Unsupported SQLType %s for STDDEV_SAMP aggregate", SQLTypeToString(input_type).c_str());
	}
}

static SQLType ResolveAggregateType(AggregateExpression &aggr, unique_ptr<Expression> *child, SQLType child_type) {
	switch (aggr.type) {
	case ExpressionType::AGGREGATE_COUNT_STAR:
	case ExpressionType::AGGREGATE_COUNT:
	case ExpressionType::AGGREGATE_COUNT_DISTINCT:
		// count always returns a BIGINT regardless of input
		// we also don't need to cast the child for COUNT(*)/COUNT/COUNT_DISTINCT
		// as only the nullmask is used
		return SQLType(SQLTypeId::BIGINT);
	default:
		break;
	}
	// the remaining types require a child operator
	assert(*child);
	SQLType result_type;
	switch (aggr.type) {
	case ExpressionType::AGGREGATE_MAX:
	case ExpressionType::AGGREGATE_MIN:
	case ExpressionType::AGGREGATE_FIRST:
		// MAX/MIN/FIRST aggregates return the input type
		result_type = child_type;
		break;
	case ExpressionType::AGGREGATE_SUM:
	case ExpressionType::AGGREGATE_SUM_DISTINCT:
		result_type = ResolveSumType(child_type);
		break;
	default:
		assert(aggr.type == ExpressionType::AGGREGATE_STDDEV_SAMP);
		result_type = ResolveSTDDevType(child_type);
		break;
	}
	// add a cast to the child node
	*child = AddCastToType(move(*child), child_type, result_type);
	return result_type;
}

BindResult SelectBinder::BindAggregate(AggregateExpression &aggr, index_t depth) {
	auto aggr_name = aggr.GetName();
	// first bind the child of the aggregate expression (if any)
	unique_ptr<Expression> child;
	SQLType child_type;
	if (aggr.child) {
		AggregateBinder aggregate_binder(binder, context);
		string error = aggregate_binder.Bind(&aggr.child, 0);
		if (!error.empty()) {
			// failed to bind child
			if (aggregate_binder.BoundColumns()) {
				// however, we bound columns!
				// that means this aggregation belongs to this node
				// check if we have to resolve any errors by binding with parent binders
				bool success = aggregate_binder.BindCorrelatedColumns(aggr.child);
				// if there is still an error after this, we could not successfully bind the aggregate
				if (!success) {
					throw BinderException(error);
				}
				auto &bound_expr = (BoundExpression &)*aggr.child;
				ExtractCorrelatedExpressions(binder, *bound_expr.expr);
			} else {
				// we didn't bind columns, try again in children
				return BindResult(error);
			}
		}
		auto &bound_expr = (BoundExpression &)*aggr.child;
		child_type = bound_expr.sql_type;
		child = move(bound_expr.expr);
	}
	SQLType result_type = ResolveAggregateType(aggr, &child, child_type);
	// create the aggregate
	auto aggregate = make_unique<BoundAggregateExpression>(GetInternalType(result_type), aggr.type, move(child));
	// now create a column reference referring to this aggregate

	auto colref = make_unique<BoundColumnRefExpression>(
	    aggr_name, aggregate->return_type, ColumnBinding(node.aggregate_index, node.aggregates.size()), depth);
	// move the aggregate expression into the set of bound aggregates
	node.aggregates.push_back(move(aggregate));
	return BindResult(move(colref), result_type);
}
