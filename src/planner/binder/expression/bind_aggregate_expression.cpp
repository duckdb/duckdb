#include "parser/expression/aggregate_expression.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"
#include "planner/expression_binder/aggregate_binder.hpp"
#include "planner/expression_binder/select_binder.hpp"

using namespace duckdb;
using namespace std;

static SQLType ResolveSumType(SQLType input_type) {
	switch (child.sql_type.id) {
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
		throw BinderException("Unsupported SQLType %s for SUM aggregate", SQLTypeToString(child.sql_type));
	}
}

static SQLType ResolveSTDDevType(SQLType input_type) {
	switch (child.sql_type.id) {
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
		throw BinderException("Unsupported SQLType %s for STDDEV_SAMP aggregate", SQLTypeToString(child.sql_type));
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

BindResult SelectBinder::BindExpression(AggregateExpression &aggr, uint32_t depth) {
	// first bind the child of the aggregate expression (if any)
	if (aggr.child) {
		AggregateBinder aggregate_binder(binder, context, node);
		string result = aggregate_binder.Bind(&aggr.child, 0);
		if (!result.empty()) {
			return BindResult(result);
		}
	}
	// successfully bound child, determine result types
	auto child = aggr.child ? GetExpression(*aggr.child) : nullptr;
	SQLType result_type = ResolveAggregateType(aggr, &child);
	return BindResult(
	    make_unique<BoundAggregateExpression>(GetInternalType(result_type), result_type, aggr.type, move(child)));
}

// BindResult SelectBinder::BindAggregate(unique_ptr<Expression> expr, uint32_t depth) {
// 	assert(expr && expr->GetExpressionClass() == ExpressionClass::AGGREGATE);
// 	// bind the children of the aggregation
// 	AggregateBinder aggregate_binder(binder, context, node);
// 	auto bind_result = aggregate_binder.BindChildren(move(expr), 0);
// 	if (aggregate_binder.BoundColumns() || !bind_result.HasError()) {
// 		// columns were bound or binding was successful!
// 		// that means this aggregation belongs to this node
// 		// check if we have to resolve any errors by binding with parent binders
// 		bind_result = BindCorrelatedColumns(move(bind_result), true);
// 		// if there is still an error after this, we could not successfully bind the aggregate
// 		if (bind_result.HasError()) {
// 			throw BinderException(bind_result.error);
// 		}
// 		bind_result.expression->ResolveType();
// 		ExtractCorrelatedExpressions(binder, *bind_result.expression);
// 		// successfully bound: extract the aggregation and place it inside the set of aggregates for this node
// 		auto colref = make_unique<BoundColumnRefExpression>(
// 		    *bind_result.expression, bind_result.expression->return_type,
// 		    ColumnBinding(node.binding.aggregate_index, node.binding.aggregates.size()), depth);
// 		// move the aggregate expression into the set of bound aggregates
// 		node.binding.aggregates.push_back(move(bind_result.expression));
// 		return BindResult(move(colref));
// 	}
// 	return bind_result;
// }
