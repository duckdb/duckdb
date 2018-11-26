
#include "parser/sql_node_visitor.hpp"

#include "parser/constraints/list.hpp"
#include "parser/expression/list.hpp"
#include "parser/tableref/list.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> SQLNodeVisitor::Visit(AggregateExpression &expr) {
	expr.AcceptChildren(this);
	return nullptr;
}
unique_ptr<Expression> SQLNodeVisitor::Visit(CaseExpression &expr) {
	expr.AcceptChildren(this);
	return nullptr;
}
unique_ptr<Expression> SQLNodeVisitor::Visit(CastExpression &expr) {
	expr.AcceptChildren(this);
	return nullptr;
}
unique_ptr<Expression> SQLNodeVisitor::Visit(ColumnRefExpression &expr) {
	expr.AcceptChildren(this);
	return nullptr;
}
unique_ptr<Expression> SQLNodeVisitor::Visit(ComparisonExpression &expr) {
	expr.AcceptChildren(this);
	return nullptr;
}
unique_ptr<Expression> SQLNodeVisitor::Visit(ConjunctionExpression &expr) {
	expr.AcceptChildren(this);
	return nullptr;
}
unique_ptr<Expression> SQLNodeVisitor::Visit(ConstantExpression &expr) {
	expr.AcceptChildren(this);
	return nullptr;
}
unique_ptr<Expression> SQLNodeVisitor::Visit(DefaultExpression &expr) {
	expr.AcceptChildren(this);
	return nullptr;
}
unique_ptr<Expression> SQLNodeVisitor::Visit(FunctionExpression &expr) {
	expr.AcceptChildren(this);
	return nullptr;
}
unique_ptr<Expression> SQLNodeVisitor::Visit(GroupRefExpression &expr) {
	expr.AcceptChildren(this);
	return nullptr;
}
unique_ptr<Expression> SQLNodeVisitor::Visit(OperatorExpression &expr) {
	expr.AcceptChildren(this);
	return nullptr;
}
unique_ptr<Expression> SQLNodeVisitor::Visit(SubqueryExpression &expr) {
	expr.AcceptChildren(this);
	return nullptr;
}
unique_ptr<Expression> SQLNodeVisitor::Visit(StarExpression &expr) {
	expr.AcceptChildren(this);
	return nullptr;
}

unique_ptr<Constraint> SQLNodeVisitor::Visit(CheckConstraint &con) {
	con.expression->Accept(this);
	return nullptr;
}

unique_ptr<TableRef> SQLNodeVisitor::Visit(CrossProductRef &expr) {
	AcceptChild(&expr.left);
	AcceptChild(&expr.right);
	return nullptr;
}

unique_ptr<TableRef> SQLNodeVisitor::Visit(JoinRef &expr) {
	AcceptChild(&expr.left);
	AcceptChild(&expr.right);
	return nullptr;
}
