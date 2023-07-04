#include "duckdb_python/expression/pyexpression.hpp"

namespace duckdb {

void DuckDBPyExpression::Initialize(py::handle &m) {
	auto expression = py::class_<DuckDBPyExpression>(m, "Expression", py::module_local());

	const char *docs;
	auto constant_expression =
	    py::class_<PyConstantExpression, DuckDBPyExpression>(m, "ConstantExpression", py::module_local());
	constant_expression.def(py::init<const PythonValue &>(), py::arg("value"));

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
