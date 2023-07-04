#include "duckdb_python/expression/pyexpression.hpp"

namespace duckdb {

void DuckDBPyExpression::Initialize(py::handle &m) {
	auto expression_module = py::class_<DuckDBPyExpression>(m, "Expression", py::module_local());

	auto constant_expression =
	    py::class_<PyConstantExpression, DuckDBPyExpression>(m, "ConstantExpression", py::module_local());
}

} // namespace duckdb
