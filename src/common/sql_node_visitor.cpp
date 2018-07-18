
#include "common/sql_node_visitor.hpp"

#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/basetableref_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/comparison_expression.hpp"
#include "parser/expression/conjunction_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/expression/crossproduct_expression.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/expression/join_expression.hpp"
#include "parser/expression/operator_expression.hpp"
#include "parser/expression/subquery_expression.hpp"
#include "parser/expression/tableref_expression.hpp"

using namespace duckdb;
using namespace std;

void SQLNodeVisitor::Visit(SelectStatement &statement) {}

void SQLNodeVisitor::Visit(AggregateExpression &expr) { expr.AcceptChildren(this); }
void SQLNodeVisitor::Visit(BaseTableRefExpression &expr) { expr.AcceptChildren(this); }
void SQLNodeVisitor::Visit(ColumnRefExpression &expr) { expr.AcceptChildren(this); }
void SQLNodeVisitor::Visit(ComparisonExpression &expr) { expr.AcceptChildren(this); }
void SQLNodeVisitor::Visit(ConjunctionExpression &expr) { expr.AcceptChildren(this); }
void SQLNodeVisitor::Visit(ConstantExpression &expr) { expr.AcceptChildren(this); }
void SQLNodeVisitor::Visit(CrossProductExpression &expr) { expr.AcceptChildren(this); }
void SQLNodeVisitor::Visit(FunctionExpression &expr) { expr.AcceptChildren(this); }
void SQLNodeVisitor::Visit(JoinExpression &expr) { expr.AcceptChildren(this); }
void SQLNodeVisitor::Visit(OperatorExpression &expr) { expr.AcceptChildren(this); }
void SQLNodeVisitor::Visit(SubqueryExpression &expr) { expr.AcceptChildren(this); }
void SQLNodeVisitor::Visit(TableRefExpression &expr) { expr.AcceptChildren(this); }