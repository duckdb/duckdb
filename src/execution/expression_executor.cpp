
#include "execution/expression_executor.hpp"
#include "execution/vector/vector_operations.hpp"

#include "common/exception.hpp"

#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/basetableref_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/comparison_expression.hpp"
#include "parser/expression/conjunction_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/expression/crossproduct_expression.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/expression/groupref_expression.hpp"
#include "parser/expression/join_expression.hpp"
#include "parser/expression/operator_expression.hpp"
#include "parser/expression/subquery_expression.hpp"
#include "parser/expression/tableref_expression.hpp"

#include "execution/operator/physical_aggregate.hpp"
#include "execution/operator/physical_hash_aggregate.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Reset() { vector.Reset(); }

void ExpressionExecutor::Execute(AbstractExpression *expr, Vector &result) {
	vector.Reset();
	expr->Accept(this);

	if (result.type != vector.type) {
		throw NotImplementedException("FIXME: cast");
	}
	vector.Move(result);
}

void ExpressionExecutor::Merge(AbstractExpression *expr, Vector &result) {
	vector.Reset();
	if (result.type != TypeId::BOOLEAN) {
		throw NotImplementedException("Expected a boolean!");
	}
	expr->Accept(this);

	if (vector.type != TypeId::BOOLEAN) {
		throw NotImplementedException("Expected a boolean!");
	}
	VectorOperations::And(vector, result, result);
}

void ExpressionExecutor::Merge(AggregateExpression &expr, Value &result) {
	vector.Reset();
	if (result.type != expr.return_type) {
		throw NotImplementedException(
		    "Aggregate type does not match value type!");
	}

	if (expr.type == ExpressionType::AGGREGATE_COUNT) {
		// COUNT(*)
		// Without FROM clause return "1", else return "count"
		size_t count = chunk.column_count == 0 ? 1 : chunk.count;
		Value v = Value::NumericValue(result.type, count);
		Value::Add(result, v, result);
	} else if (expr.children.size() > 0) {
		Vector child;
		expr.children[0]->Accept(this);
		vector.Move(child);
		vector.Resize(1, expr.return_type);
		switch (expr.type) {
		case ExpressionType::AGGREGATE_SUM: {
			VectorOperations::Sum(child, vector);
			Value v = vector.GetValue(0);
			Value::Add(result, v, result);
			break;
		}
		case ExpressionType::AGGREGATE_COUNT: {
			VectorOperations::Count(child, vector);
			Value v = vector.GetValue(0);
			Value::Add(result, v, result);
			break;
		}
		case ExpressionType::AGGREGATE_AVG: {
			VectorOperations::Sum(child, vector);
			Value v = vector.GetValue(0);
			Value::Add(result, v, result);
			break;
		}
		case ExpressionType::AGGREGATE_MIN: {
			VectorOperations::Min(child, vector);
			Value v = vector.GetValue(0);
			Value::Min(result, v, result);
			break;
		}
		case ExpressionType::AGGREGATE_MAX: {
			VectorOperations::Max(child, vector);
			Value v = vector.GetValue(0);
			Value::Max(result, v, result);
			break;
		}
		default:
			throw NotImplementedException("Unsupported aggregate type");
		}
	} else {
		throw NotImplementedException("Aggregate expression without children!");
	}
}

void ExpressionExecutor::Visit(AggregateExpression &expr) {
	auto state =
	    reinterpret_cast<PhysicalAggregateOperatorState *>(this->state);
	if (!state) {
		throw NotImplementedException("Aggregate node without aggregate state");
	}
	if (state->aggregates.size() == 0) {
		vector.Reference(*state->aggregate_chunk.data[expr.index].get());
	} else {
		Vector v(state->aggregates[expr.index]);
		v.Move(vector);
	}
}

void ExpressionExecutor::Visit(BaseTableRefExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(ColumnRefExpression &expr) {
	if (expr.index == (size_t)-1) {
		throw Exception("Column Reference not bound!");
	}
	vector.Reference(*chunk.data[expr.index].get());
}

void ExpressionExecutor::Visit(ComparisonExpression &expr) {
	Vector l, r;
	expr.children[0]->Accept(this);
	vector.Move(l);
	expr.children[1]->Accept(this);
	vector.Move(r);
	vector.Resize(std::max(l.count, r.count), TypeId::BOOLEAN);
	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
		VectorOperations::Equals(l, r, vector);
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		VectorOperations::NotEquals(l, r, vector);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		VectorOperations::LessThan(l, r, vector);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		VectorOperations::GreaterThan(l, r, vector);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		VectorOperations::LessThanEquals(l, r, vector);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		VectorOperations::GreaterThanEquals(l, r, vector);
		break;
	case ExpressionType::COMPARE_LIKE:
		throw NotImplementedException("Unimplemented compare: COMPARE_LIKE");
	case ExpressionType::COMPARE_NOTLIKE:
		throw NotImplementedException("Unimplemented compare: COMPARE_NOTLIKE");
	case ExpressionType::COMPARE_IN:
		throw NotImplementedException("Unimplemented compare: COMPARE_IN");
	case ExpressionType::COMPARE_DISTINCT_FROM:
		throw NotImplementedException(
		    "Unimplemented compare: COMPARE_DISTINCT_FROM");
	default:
		throw NotImplementedException("Unknown comparison type!");
	}
}

void ExpressionExecutor::Visit(ConjunctionExpression &expr) {
	Vector l, r, result;
	expr.children[0]->Accept(this);
	vector.Move(l);
	expr.children[1]->Accept(this);
	vector.Move(r);
	vector.Resize(std::max(l.count, r.count), TypeId::BOOLEAN);
	switch (expr.type) {
	case ExpressionType::CONJUNCTION_AND:
		VectorOperations::And(l, r, vector);
		break;
	case ExpressionType::CONJUNCTION_OR:
		VectorOperations::Or(l, r, vector);
		break;
	default:
		throw NotImplementedException("Unknown conjunction type!");
	}
}

void ExpressionExecutor::Visit(ConstantExpression &expr) {
	Vector v(expr.value);
	v.Move(vector);
}

void ExpressionExecutor::Visit(CrossProductExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(FunctionExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(GroupRefExpression &expr) {
	auto state =
	    reinterpret_cast<PhysicalAggregateOperatorState *>(this->state);
	if (!state) {
		throw NotImplementedException("Aggregate node without aggregate state");
	}
	vector.Reference(*state->group_chunk.data[expr.group_index].get());
}

void ExpressionExecutor::Visit(JoinExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(OperatorExpression &expr) {
	if (expr.children.size() == 1) {
		Vector l;
		expr.children[0]->Accept(this);
		vector.Move(l);

		switch (expr.type) {
		default:
			throw NotImplementedException(
			    "Unsupported operator type with 1 child!");
		}
	} else if (expr.children.size() == 2) {
		Vector l, r;
		expr.children[0]->Accept(this);
		vector.Move(l);
		expr.children[1]->Accept(this);
		vector.Move(r);

		vector.Resize(std::max(l.count, r.count));

		switch (expr.type) {
		case ExpressionType::OPERATOR_PLUS:
			VectorOperations::Add(l, r, vector);
			break;
		case ExpressionType::OPERATOR_MINUS:
			VectorOperations::Subtract(l, r, vector);
			break;
		case ExpressionType::OPERATOR_MULTIPLY:
			VectorOperations::Multiply(l, r, vector);
			break;
		case ExpressionType::OPERATOR_DIVIDE:
			VectorOperations::Divide(l, r, vector);
			break;
		case ExpressionType::OPERATOR_MOD:
			VectorOperations::Modulo(l, r, vector);
			break;
		default:
			throw NotImplementedException(
			    "Unsupported operator type with 2 children!");
		}
	} else {
		throw NotImplementedException("operator");
	}
}

void ExpressionExecutor::Visit(SubqueryExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(TableRefExpression &expr) {
	throw NotImplementedException("");
}
