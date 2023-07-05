#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/expression/pyexpression.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

void InitializeStaticMethods(py::module_ &m) {
	const char *docs;

	// Constant Expression
	docs = "Create a constant expression from the provided value";
	m.def("ConstantExpression", &DuckDBPyExpression::ConstantExpression, py::arg("value"), docs);

	// ColumnRef Expression
	docs = "Create a column reference from the provided column name";
	m.def("ColumnExpression", &DuckDBPyExpression::ColumnExpression, py::arg("name"), docs);

	// Function Expression
	docs = "";
	m.def("BinaryFunctionExpression", &DuckDBPyExpression::BinaryFunctionExpression, py::arg("function_name"),
	      py::arg("arg_one"), py::arg("arg_two"), docs);
}

static void InitializeDunderMethods(py::class_<DuckDBPyExpression, shared_ptr<DuckDBPyExpression>> &m) {
	const char *docs;

	docs = R"(
		Add two expressions.

		Parameters:
			expr: The expression to add together with

		Returns:
			FunctionExpression: A '+' on the two input expressions.
	)";
	m.def("__add__", &DuckDBPyExpression::Add, py::arg("expr"), docs);
	m.def("__radd__", &DuckDBPyExpression::Add, py::arg("expr"), docs);

	docs = R"(
		TODO: add docs
	)";
	m.def("__neg__", &DuckDBPyExpression::Negate, docs);

	docs = R"(
		TODO: add docs
	)";
	m.def("__sub__", &DuckDBPyExpression::Subtract, docs);
	m.def("__rsub__", &DuckDBPyExpression::Subtract, docs);

	docs = R"(
		TODO: add docs
	)";
	m.def("__mul__", &DuckDBPyExpression::Multiply, docs);
	m.def("__rmul__", &DuckDBPyExpression::Multiply, docs);

	docs = R"(
		TODO: add docs
	)";
	m.def("__div__", &DuckDBPyExpression::Division, docs);
	m.def("__rdiv__", &DuckDBPyExpression::Division, docs);

	docs = R"(
		TODO: add docs
	)";
	m.def("__truediv__", &DuckDBPyExpression::Division, docs);
	m.def("__rtruediv__", &DuckDBPyExpression::Division, docs);

	docs = R"(
		TODO: add docs
	)";
	m.def("__floordiv__", &DuckDBPyExpression::FloorDivision, docs);
	m.def("__rfloordiv__", &DuckDBPyExpression::FloorDivision, docs);

	docs = R"(
		TODO: add docs
	)";
	m.def("__mod__", &DuckDBPyExpression::Modulo, docs);
	m.def("__rmod__", &DuckDBPyExpression::Modulo, docs);

	docs = R"(
		TODO: add docs
	)";
	m.def("__pow__", &DuckDBPyExpression::Power, docs);
	m.def("__rpow__", &DuckDBPyExpression::Power, docs);

	docs = R"(
		Create an equality expression between two expressions
	)";
	m.def("__eq__", &DuckDBPyExpression::Equality, docs);

	docs = R"(
		TODO: add docs
	)";
	m.def("__ne__", &DuckDBPyExpression::Inequality, docs);

	docs = R"(
		TODO: add docs
	)";
	m.def("__gt__", &DuckDBPyExpression::GreaterThan, docs);

	docs = R"(
		TODO: add docs
	)";
	m.def("__ge__", &DuckDBPyExpression::GreaterThanOrEqual, docs);

	docs = R"(
		TODO: add docs
	)";
	m.def("__lt__", &DuckDBPyExpression::LessThan, docs);

	docs = R"(
		TODO: add docs
	)";
	m.def("__le__", &DuckDBPyExpression::LessThanOrEqual, docs);
}

void DuckDBPyExpression::Initialize(py::module_ &m) {
	auto expression =
	    py::class_<DuckDBPyExpression, shared_ptr<DuckDBPyExpression>>(m, "Expression", py::module_local());

	InitializeStaticMethods(m);
	InitializeDunderMethods(expression);

	const char *docs;

	docs = R"(
    Print the stringified version of the expression.
	)";
	expression.def("show", &DuckDBPyExpression::Print, docs);

	docs = R"(
        Return the stringified version of the expression.

        Returns:
            str: The string representation.
    )";
	expression.def("__repr__", &DuckDBPyExpression::ToString, docs);
}

} // namespace duckdb
