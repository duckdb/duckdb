#include "duckdb_python/expression/pyexpression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"

namespace duckdb {

DuckDBPyExpression::DuckDBPyExpression(unique_ptr<ParsedExpression> expr_p, OrderType order_type,
                                       OrderByNullType null_order)
    : expression(std::move(expr_p)), null_order(null_order), order_type(order_type) {
	if (!expression) {
		throw InternalException("DuckDBPyExpression created without an expression");
	}
}

string DuckDBPyExpression::Type() const {
	return ExpressionTypeToString(expression->type);
}

string DuckDBPyExpression::ToString() const {
	return expression->ToString();
}

void DuckDBPyExpression::Print() const {
	Printer::Print(expression->ToString());
}

const ParsedExpression &DuckDBPyExpression::GetExpression() const {
	return *expression;
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Copy() const {
	auto expr = GetExpression().Copy();
	return make_shared_ptr<DuckDBPyExpression>(std::move(expr), order_type, null_order);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::SetAlias(const string &name) const {
	auto copied_expression = GetExpression().Copy();
	copied_expression->alias = name;
	return make_shared_ptr<DuckDBPyExpression>(std::move(copied_expression));
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Cast(const DuckDBPyType &type) const {
	auto copied_expression = GetExpression().Copy();
	auto case_expr = make_uniq<duckdb::CastExpression>(type.Type(), std::move(copied_expression));
	return make_shared_ptr<DuckDBPyExpression>(std::move(case_expr));
}

// Case Expression modifiers

void DuckDBPyExpression::AssertCaseExpression() const {
	if (expression->type != ExpressionType::CASE_EXPR) {
		throw py::value_error("This method can only be used on a Expression resulting from CaseExpression or When");
	}
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::InternalWhen(unique_ptr<duckdb::CaseExpression> expr,
                                                                const DuckDBPyExpression &condition,
                                                                const DuckDBPyExpression &value) {
	CaseCheck check;
	check.when_expr = condition.GetExpression().Copy();
	check.then_expr = value.GetExpression().Copy();
	expr->case_checks.push_back(std::move(check));
	return make_shared_ptr<DuckDBPyExpression>(std::move(expr));
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::When(const DuckDBPyExpression &condition,
                                                        const DuckDBPyExpression &value) {
	AssertCaseExpression();
	auto expr_p = expression->Copy();
	auto expr = unique_ptr_cast<ParsedExpression, duckdb::CaseExpression>(std::move(expr_p));

	return InternalWhen(std::move(expr), condition, value);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Else(const DuckDBPyExpression &value) {
	AssertCaseExpression();
	auto expr_p = expression->Copy();
	auto expr = unique_ptr_cast<ParsedExpression, duckdb::CaseExpression>(std::move(expr_p));

	expr->else_expr = value.GetExpression().Copy();
	return make_shared_ptr<DuckDBPyExpression>(std::move(expr));
}

// Binary operators

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Add(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::BinaryOperator("+", *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Subtract(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::BinaryOperator("-", *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Multiply(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::BinaryOperator("*", *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Division(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::BinaryOperator("/", *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::FloorDivision(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::BinaryOperator("//", *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Modulo(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::BinaryOperator("%", *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Power(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::BinaryOperator("**", *this, other);
}

// Comparison expressions

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Equality(const DuckDBPyExpression &other) {
	return ComparisonExpression(ExpressionType::COMPARE_EQUAL, *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Inequality(const DuckDBPyExpression &other) {
	return ComparisonExpression(ExpressionType::COMPARE_NOTEQUAL, *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::GreaterThan(const DuckDBPyExpression &other) {
	return ComparisonExpression(ExpressionType::COMPARE_GREATERTHAN, *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::GreaterThanOrEqual(const DuckDBPyExpression &other) {
	return ComparisonExpression(ExpressionType::COMPARE_GREATERTHANOREQUALTO, *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::LessThan(const DuckDBPyExpression &other) {
	return ComparisonExpression(ExpressionType::COMPARE_LESSTHAN, *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::LessThanOrEqual(const DuckDBPyExpression &other) {
	return ComparisonExpression(ExpressionType::COMPARE_LESSTHANOREQUALTO, *this, other);
}

// AND, OR and NOT

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Not() {
	return DuckDBPyExpression::InternalUnaryOperator(ExpressionType::OPERATOR_NOT, *this);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::And(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::InternalConjunction(ExpressionType::CONJUNCTION_AND, *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Or(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::InternalConjunction(ExpressionType::CONJUNCTION_OR, *this, other);
}

// NULL

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::IsNull() {
	return DuckDBPyExpression::InternalUnaryOperator(ExpressionType::OPERATOR_IS_NULL, *this);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::IsNotNull() {
	return DuckDBPyExpression::InternalUnaryOperator(ExpressionType::OPERATOR_IS_NOT_NULL, *this);
}

// IN

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::In(const py::args &args) {
	vector<unique_ptr<ParsedExpression>> expressions;
	expressions.reserve(args.size() + 1);
	expressions.push_back(GetExpression().Copy());

	for (auto arg : args) {
		shared_ptr<DuckDBPyExpression> py_expr;
		if (!py::try_cast<shared_ptr<DuckDBPyExpression>>(arg, py_expr)) {
			throw InvalidInputException("Please provide arguments of type Expression!");
		}
		auto expr = py_expr->GetExpression().Copy();
		expressions.push_back(std::move(expr));
	}
	auto operator_expr = make_uniq<OperatorExpression>(ExpressionType::COMPARE_IN, std::move(expressions));
	return make_shared_ptr<DuckDBPyExpression>(std::move(operator_expr));
}

// COALESCE

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Coalesce(const py::args &args) {
	vector<unique_ptr<ParsedExpression>> expressions;
	expressions.reserve(args.size());

	for (auto arg : args) {
		shared_ptr<DuckDBPyExpression> py_expr;
		if (!py::try_cast<shared_ptr<DuckDBPyExpression>>(arg, py_expr)) {
			throw InvalidInputException("Please provide arguments of type Expression!");
		}
		auto expr = py_expr->GetExpression().Copy();
		expressions.push_back(std::move(expr));
	}
	if (expressions.empty()) {
		throw InvalidInputException("Please provide at least one argument");
	}
	auto operator_expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE, std::move(expressions));
	return make_shared_ptr<DuckDBPyExpression>(std::move(operator_expr));
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::NotIn(const py::args &args) {
	auto in_expr = In(args);
	return in_expr->Not();
}

// Order modifiers

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Ascending() {
	auto py_expr = Copy();
	py_expr->order_type = OrderType::ASCENDING;
	return py_expr;
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Descending() {
	auto py_expr = Copy();
	py_expr->order_type = OrderType::DESCENDING;
	return py_expr;
}

// Null order modifiers

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::NullsFirst() {
	auto py_expr = Copy();
	py_expr->null_order = OrderByNullType::NULLS_FIRST;
	return py_expr;
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::NullsLast() {
	auto py_expr = Copy();
	py_expr->null_order = OrderByNullType::NULLS_LAST;
	return py_expr;
}

// Unary operators

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Negate() {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(GetExpression().Copy());
	return DuckDBPyExpression::InternalFunctionExpression("-", std::move(children), true);
}

// Static creation methods

static void PopulateExcludeList(case_insensitive_set_t &exclude, py::object list_p) {
	if (py::none().is(list_p)) {
		list_p = py::list();
	}
	py::list list = py::cast<py::list>(list_p);
	for (auto item : list) {
		if (py::isinstance<py::str>(item)) {
			exclude.insert(std::string(py::str(item)));
			continue;
		}
		shared_ptr<DuckDBPyExpression> expr;
		if (!py::try_cast(item, expr)) {
			throw py::value_error("Items in the exclude list should either be 'str' or Expression");
		}
		if (expr->GetExpression().type != ExpressionType::COLUMN_REF) {
			throw py::value_error("Only ColumnExpressions are accepted Expression types here");
		}
		auto &column = expr->GetExpression().Cast<ColumnRefExpression>();
		exclude.insert(column.GetColumnName());
	}
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::StarExpression(py::object exclude_list) {
	case_insensitive_set_t exclude;
	auto star = make_uniq<duckdb::StarExpression>();
	PopulateExcludeList(star->exclude_list, std::move(exclude_list));
	return make_shared_ptr<DuckDBPyExpression>(std::move(star));
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::ColumnExpression(const string &column_name) {
	if (column_name == "*") {
		return StarExpression();
	}

	auto qualified_name = QualifiedName::Parse(column_name);
	vector<string> column_names;
	if (!qualified_name.catalog.empty()) {
		column_names.push_back(qualified_name.catalog);
	}
	if (!qualified_name.schema.empty()) {
		column_names.push_back(qualified_name.schema);
	}
	column_names.push_back(qualified_name.name);

	return make_shared_ptr<DuckDBPyExpression>(make_uniq<duckdb::ColumnRefExpression>(std::move(column_names)));
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::ConstantExpression(const py::object &value) {
	auto val = TransformPythonValue(value);
	return InternalConstantExpression(std::move(val));
}

// Private methods

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::BinaryOperator(const string &function_name,
                                                                  const DuckDBPyExpression &arg_one,
                                                                  const DuckDBPyExpression &arg_two) {
	vector<unique_ptr<ParsedExpression>> children;

	children.push_back(arg_one.GetExpression().Copy());
	children.push_back(arg_two.GetExpression().Copy());
	return InternalFunctionExpression(function_name, std::move(children), true);
}

shared_ptr<DuckDBPyExpression>
DuckDBPyExpression::InternalFunctionExpression(const string &function_name,
                                               vector<unique_ptr<ParsedExpression>> children, bool is_operator) {
	auto function_expression =
	    make_uniq<duckdb::FunctionExpression>(function_name, std::move(children), nullptr, nullptr, false, is_operator);
	return make_shared_ptr<DuckDBPyExpression>(std::move(function_expression));
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::InternalUnaryOperator(ExpressionType type,
                                                                         const DuckDBPyExpression &arg) {
	auto expr = arg.GetExpression().Copy();
	auto operator_expression = make_uniq<OperatorExpression>(type, std::move(expr));
	return make_shared_ptr<DuckDBPyExpression>(std::move(operator_expression));
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::InternalConjunction(ExpressionType type,
                                                                       const DuckDBPyExpression &arg,
                                                                       const DuckDBPyExpression &other) {
	vector<unique_ptr<ParsedExpression>> children;
	children.reserve(2);
	children.push_back(arg.GetExpression().Copy());
	children.push_back(other.GetExpression().Copy());

	auto operator_expression = make_uniq<ConjunctionExpression>(type, std::move(children));
	return make_shared_ptr<DuckDBPyExpression>(std::move(operator_expression));
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::InternalConstantExpression(Value val) {
	return make_shared_ptr<DuckDBPyExpression>(make_uniq<duckdb::ConstantExpression>(std::move(val)));
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::ComparisonExpression(ExpressionType type,
                                                                        const DuckDBPyExpression &left_p,
                                                                        const DuckDBPyExpression &right_p) {
	auto left = left_p.GetExpression().Copy();
	auto right = right_p.GetExpression().Copy();
	return make_shared_ptr<DuckDBPyExpression>(
	    make_uniq<duckdb::ComparisonExpression>(type, std::move(left), std::move(right)));
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::CaseExpression(const DuckDBPyExpression &condition,
                                                                  const DuckDBPyExpression &value) {
	auto expr = make_uniq<duckdb::CaseExpression>();
	auto case_expr = InternalWhen(std::move(expr), condition, value);

	// Add NULL as default Else expression
	auto &internal_expression = reinterpret_cast<duckdb::CaseExpression &>(*case_expr->expression);
	internal_expression.else_expr = make_uniq<duckdb::ConstantExpression>(Value(LogicalTypeId::SQLNULL));
	return case_expr;
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::FunctionExpression(const string &function_name,
                                                                      const py::args &args) {
	vector<unique_ptr<ParsedExpression>> expressions;
	for (auto arg : args) {
		shared_ptr<DuckDBPyExpression> py_expr;
		if (!py::try_cast<shared_ptr<DuckDBPyExpression>>(arg, py_expr)) {
			string actual_type = py::str(arg.get_type());
			throw InvalidInputException("Expected argument of type Expression, received '%s' instead", actual_type);
		}
		auto expr = py_expr->GetExpression().Copy();
		expressions.push_back(std::move(expr));
	}
	return InternalFunctionExpression(function_name, std::move(expressions));
}

} // namespace duckdb
