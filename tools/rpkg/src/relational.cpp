#include "cpp11.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

using namespace duckdb;
using namespace cpp11;

[[cpp11::register]] SEXP expr_ref_R(strings ref) {
	if (ref.size() != 1 || ref[0] == NA_STRING) {
		cpp11::stop("expr_ref(): need scalar string parameter");
	}
	return external_pointer<ColumnRefExpression>(new ColumnRefExpression(ref[0]));
}

[[cpp11::register]] SEXP expr_constant_R(SEXP ref) {
	return external_pointer<ConstantExpression>(new ConstantExpression(Value::INTEGER(42)));
}
