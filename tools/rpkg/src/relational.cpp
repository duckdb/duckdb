#include "cpp11.hpp"
#include "duckdb.hpp"
#include "typesr.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

using namespace duckdb;
using namespace cpp11;

template <typename T, typename... Args>
external_pointer<T> make_external(Args &&...args) {
	return external_pointer<T>(new T(std::forward<Args>(args)...));
}

[[cpp11::register]] SEXP expr_reference_R(strings ref) {
	return make_external<ColumnRefExpression>(ref[0]);
}

[[cpp11::register]] SEXP expr_constant_R(sexp val) {
	return make_external<ConstantExpression>(RApiTypes::SexpToValue(val, 0));
}

[[cpp11::register]] SEXP expr_function_R(strings name, list args) {
	vector<unique_ptr<ParsedExpression>> children;
	for (auto arg : args) {
		children.push_back(external_pointer<ParsedExpression>(arg)->Copy());
	}

	return make_external<FunctionExpression>(name[0], move(children));
}

[[cpp11::register]] SEXP expr_tostring_R(sexp expr) {
	return writable::strings({external_pointer<ParsedExpression>(expr)->ToString()});
}
