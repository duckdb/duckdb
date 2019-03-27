#include "parser/expression/aggregate_expression.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"
#include "planner/expression_binder/aggregate_binder.hpp"
#include "planner/expression_binder/select_binder.hpp"

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
	case SQLTypeId::REAL:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return SQLType(SQLTypeId::DECIMAL);
	default:
		throw BinderException("Unsupported SQLType %s for SUM aggregate", SQLTypeToString(input_type));
	}
}

static SQLType ResolveSTDDevType(SQLType input_type) {
	switch (input_type.id) {
	case SQLTypeId::SQLNULL:
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::REAL:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return SQLType(SQLTypeId::DECIMAL);
	default:
		throw BinderException("Unsupported SQLType %s for STDDEV_SAMP aggregate", SQLTypeToString(input_type));
	}
}

static SQLType ResolveAggregateType(AggregateExpression &aggr, unique_ptr<Expression> *child) {
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
	SQLType child_type = (*child)->sql_type;
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
	*child = AddCastToType(move(*child), result_type);
}

BindResult SelectBinder::BindAggregate(AggregateExpression &aggr, uint32_t depth) {
	// first bind the child of the aggregate expression (if any)
	if (aggr.child) {
		AggregateBinder aggregate_binder(binder, context, node);
		string result = aggregate_binder.Bind(&aggr.child, 0);
		if (!result.empty()) {
			// FIXME: check if columns were bound for subqueries
			return BindResult(result);
		}
	}
	auto child = GetExpression(aggr.child);
	SQLType result_type = ResolveAggregateType(aggr, &child);
	// create the aggregate
	auto aggregate = make_unique<BoundAggregateExpression>(GetInternalType(result_type), result_type, aggr.type, move(child));
	// now create a column reference referring to this aggregate
	auto colref = make_unique<BoundColumnRefExpression>(
		aggr.GetName(), aggr->return_type, aggr->sql_type,
		ColumnBinding(node.aggregate_index, node.aggregates.size()), depth);
	// move the aggregate expression into the set of bound aggregates
	node.aggregates.push_back(move(aggregate));
	return BindResult(move(colref));
}
