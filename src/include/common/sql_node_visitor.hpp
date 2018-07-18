
#pragma once

namespace duckdb {

class SelectStatement;

class AggregateExpression;
class BaseTableRefExpression;
class ColumnRefExpression;
class ComparisonExpression;
class ConjunctionExpression;
class ConstantExpression;
class CrossProductExpression;
class FunctionExpression;
class JoinExpression;
class OperatorExpression;
class SubqueryExpression;
class TableRefExpression;

class SQLNodeVisitor {
  public:
	virtual ~SQLNodeVisitor(){};

	virtual void Visit(SelectStatement &);

	virtual void Visit(AggregateExpression &expr);
	virtual void Visit(BaseTableRefExpression &expr);
	virtual void Visit(ColumnRefExpression &expr);
	virtual void Visit(ComparisonExpression &expr);
	virtual void Visit(ConjunctionExpression &expr);
	virtual void Visit(ConstantExpression &expr);
	virtual void Visit(CrossProductExpression &expr);
	virtual void Visit(FunctionExpression &expr);
	virtual void Visit(JoinExpression &expr);
	virtual void Visit(OperatorExpression &expr);
	virtual void Visit(SubqueryExpression &expr);
	virtual void Visit(TableRefExpression &expr);
};
}
