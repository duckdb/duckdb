#include "cpp11.hpp"
#include "duckdb.hpp"
#include "typesr.hpp"
#include "rapi.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"

#include "duckdb/main/relation/filter_relation.hpp"

using namespace duckdb;
using namespace cpp11;

template <typename T, typename... Args>
external_pointer<T> make_external(Args &&...args) {
	return external_pointer<T>(new T(std::forward<Args>(args)...));
}

// expressions

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
	auto operator_type = OperatorToExpressionType((string)name[0]);
	if (operator_type != ExpressionType::INVALID && children.size() == 2) {
		return make_external<ComparisonExpression>(operator_type, move(children[0]), move(children[1]));
	}
	return make_external<FunctionExpression>(name[0], move(children));
}

[[cpp11::register]] SEXP expr_tostring_R(sexp expr) {
	return writable::strings({external_pointer<ParsedExpression>(expr)->ToString()});
}

// relations

struct RelationWrapper {
	RelationWrapper(std::shared_ptr<Relation> rel_p) : rel(move(rel_p)) {
	}
	shared_ptr<Relation> rel;
};

[[cpp11::register]] SEXP rel_df_R(sexp con_p, sexp df) {
	external_pointer<ConnWrapper> con(con_p);
	auto rel = con->conn->TableFunction("r_dataframe_scan", {Value::POINTER((uintptr_t)(SEXP)df)});
	auto res = sexp(make_external<RelationWrapper>(move(rel)));
	res.attr("df") = df;
	return res;
}

[[cpp11::register]] SEXP rel_filter_R(sexp rel_p, sexp expr_p) {
	external_pointer<RelationWrapper> child(rel_p);
	auto expr = external_pointer<ParsedExpression>(expr_p)->Copy();
	auto res = std::make_shared<FilterRelation>(child->rel, move(expr));
	return make_external<RelationWrapper>(res);
}