//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyrelation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb_python/python_conversion.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pytype.hpp"
#include "duckdb/common/enums/order_type.hpp"

namespace duckdb {

struct DuckDBPyExpression : public enable_shared_from_this<DuckDBPyExpression> {
public:
	explicit DuckDBPyExpression(unique_ptr<ParsedExpression> expr, OrderType order_type = OrderType::ORDER_DEFAULT,
	                            OrderByNullType null_order = OrderByNullType::ORDER_DEFAULT);

public:
	shared_ptr<DuckDBPyExpression> shared_from_this() {
		return enable_shared_from_this<DuckDBPyExpression>::shared_from_this();
	}

public:
	static void Initialize(py::module_ &m);

	string Type() const;

	string ToString() const;
	void Print() const;
	shared_ptr<DuckDBPyExpression> Add(const DuckDBPyExpression &other);
	shared_ptr<DuckDBPyExpression> Negate();
	shared_ptr<DuckDBPyExpression> Subtract(const DuckDBPyExpression &other);
	shared_ptr<DuckDBPyExpression> Multiply(const DuckDBPyExpression &other);
	shared_ptr<DuckDBPyExpression> Division(const DuckDBPyExpression &other);
	shared_ptr<DuckDBPyExpression> FloorDivision(const DuckDBPyExpression &other);
	shared_ptr<DuckDBPyExpression> Modulo(const DuckDBPyExpression &other);
	shared_ptr<DuckDBPyExpression> Power(const DuckDBPyExpression &other);

	// Equality operations

	shared_ptr<DuckDBPyExpression> Equality(const DuckDBPyExpression &other);
	shared_ptr<DuckDBPyExpression> Inequality(const DuckDBPyExpression &other);
	shared_ptr<DuckDBPyExpression> GreaterThan(const DuckDBPyExpression &other);
	shared_ptr<DuckDBPyExpression> GreaterThanOrEqual(const DuckDBPyExpression &other);
	shared_ptr<DuckDBPyExpression> LessThan(const DuckDBPyExpression &other);
	shared_ptr<DuckDBPyExpression> LessThanOrEqual(const DuckDBPyExpression &other);

	shared_ptr<DuckDBPyExpression> SetAlias(const string &alias) const;
	shared_ptr<DuckDBPyExpression> When(const DuckDBPyExpression &condition, const DuckDBPyExpression &value);
	shared_ptr<DuckDBPyExpression> Else(const DuckDBPyExpression &value);
	shared_ptr<DuckDBPyExpression> Cast(const DuckDBPyType &type) const;

	// AND, OR and NOT

	shared_ptr<DuckDBPyExpression> Not();
	shared_ptr<DuckDBPyExpression> And(const DuckDBPyExpression &other);
	shared_ptr<DuckDBPyExpression> Or(const DuckDBPyExpression &other);

	// IS NULL / IS NOT NULL

	shared_ptr<DuckDBPyExpression> IsNull();
	shared_ptr<DuckDBPyExpression> IsNotNull();

	// IN / NOT IN

	shared_ptr<DuckDBPyExpression> In(const py::args &args);
	shared_ptr<DuckDBPyExpression> NotIn(const py::args &args);

	// Order modifiers

	shared_ptr<DuckDBPyExpression> Ascending();
	shared_ptr<DuckDBPyExpression> Descending();

	// Null order modifiers

	shared_ptr<DuckDBPyExpression> NullsFirst();
	shared_ptr<DuckDBPyExpression> NullsLast();

public:
	const ParsedExpression &GetExpression() const;
	shared_ptr<DuckDBPyExpression> Copy() const;

public:
	static shared_ptr<DuckDBPyExpression> StarExpression(py::object exclude = py::none());
	static shared_ptr<DuckDBPyExpression> ColumnExpression(const string &column_name);
	static shared_ptr<DuckDBPyExpression> ConstantExpression(const py::object &value);
	static shared_ptr<DuckDBPyExpression> CaseExpression(const DuckDBPyExpression &condition,
	                                                     const DuckDBPyExpression &value);
	static shared_ptr<DuckDBPyExpression> FunctionExpression(const string &function_name, const py::args &args);
	static shared_ptr<DuckDBPyExpression> Coalesce(const py::args &args);

public:
	// Internal functions (not exposed to Python)
	static shared_ptr<DuckDBPyExpression> InternalFunctionExpression(const string &function_name,
	                                                                 vector<unique_ptr<ParsedExpression>> children,
	                                                                 bool is_operator = false);

	static shared_ptr<DuckDBPyExpression> InternalUnaryOperator(ExpressionType type, const DuckDBPyExpression &arg);
	static shared_ptr<DuckDBPyExpression> InternalConjunction(ExpressionType type, const DuckDBPyExpression &arg,
	                                                          const DuckDBPyExpression &other);
	static shared_ptr<DuckDBPyExpression> InternalConstantExpression(Value value);
	static shared_ptr<DuckDBPyExpression> BinaryOperator(const string &function_name, const DuckDBPyExpression &arg_one,
	                                                     const DuckDBPyExpression &arg_two);
	static shared_ptr<DuckDBPyExpression> ComparisonExpression(ExpressionType type, const DuckDBPyExpression &left,
	                                                           const DuckDBPyExpression &right);
	static shared_ptr<DuckDBPyExpression> InternalWhen(unique_ptr<duckdb::CaseExpression> expr,
	                                                   const DuckDBPyExpression &condition,
	                                                   const DuckDBPyExpression &value);
	void AssertCaseExpression() const;

private:
	unique_ptr<ParsedExpression> expression;

public:
	OrderByNullType null_order = OrderByNullType::ORDER_DEFAULT;
	OrderType order_type = OrderType::ORDER_DEFAULT;
};

} // namespace duckdb
