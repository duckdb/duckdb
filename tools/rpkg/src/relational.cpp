#include "cpp11.hpp"
#include "duckdb.hpp"
#include "typesr.hpp"
#include "rapi.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"

#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/main/relation/order_relation.hpp"

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

[[cpp11::register]] SEXP expr_function_R(strings name_p, list args) {
	vector<unique_ptr<ParsedExpression>> children;
	for (auto arg : args) {
		children.push_back(external_pointer<ParsedExpression>(arg)->Copy());
	}
	string name = (string)name_p[0];
	auto operator_type = OperatorToExpressionType(name);
	if (operator_type != ExpressionType::INVALID && children.size() == 2) {
		return make_external<ComparisonExpression>(operator_type, move(children[0]), move(children[1]));
	} else if (name == "||") {
		return make_external<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, move(children[0]),
		                                            move(children[1]));
	}
	if (name == "mean") {
		name = "avg";
	} else if (name == "n") {
		name = "count_star";
	}
	return make_external<FunctionExpression>(name, move(children));
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

[[cpp11::register]] SEXP rel_from_df_R(sexp con_p, sexp df) {
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

[[cpp11::register]] SEXP rel_project_R(sexp rel_p, list exprs_p) {
	external_pointer<RelationWrapper> child(rel_p);
	vector<unique_ptr<ParsedExpression>> projections;
	vector<string> aliases;

	for (auto expr_p : exprs_p) {
		auto expr = external_pointer<ParsedExpression>(expr_p);
		aliases.push_back((expr->ToString()));
		projections.push_back(expr->Copy());
	}

	auto res = std::make_shared<ProjectionRelation>(child->rel, move(projections), move(aliases));
	return make_external<RelationWrapper>(res);
}

[[cpp11::register]] SEXP rel_aggregate_R(sexp rel_p, list groups_p, list aggregates_p) {
	external_pointer<RelationWrapper> child(rel_p);
	vector<unique_ptr<ParsedExpression>> groups;
	vector<unique_ptr<ParsedExpression>> aggregates;

	// TODO deal with aliases
	// TODO deal with empty groups
	vector<string> aliases;

	for (auto expr_p : groups_p) {
		auto expr = external_pointer<ParsedExpression>(expr_p);
		groups.push_back(expr->Copy());
	}

	int aggr_idx = 0; // has to be int for - reasons
	auto aggr_names = aggregates_p.names();
	for (auto expr_p : aggregates_p) {
		auto expr = external_pointer<ParsedExpression>(expr_p)->Copy();
		expr->alias = aggr_names[aggr_idx];
		aggregates.push_back(move(expr));
		aggr_idx++;
	}

	auto res = std::make_shared<AggregateRelation>(child->rel, move(aggregates), move(groups));
	return make_external<RelationWrapper>(res);
}

[[cpp11::register]] SEXP rel_order_R(sexp rel_p, list orders_p) {
	external_pointer<RelationWrapper> child(rel_p);
	vector<OrderByNode> orders;

	for (auto expr_p : orders_p) {
		auto expr = external_pointer<ParsedExpression>(expr_p);
		orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, expr->Copy());
	}

	auto res = std::make_shared<OrderRelation>(child->rel, move(orders));
	return make_external<RelationWrapper>(res);
}

[[cpp11::register]] SEXP rel_to_df_R(sexp rel_p) {
	external_pointer<RelationWrapper> rel(rel_p);
	auto res = rel->rel->Execute();
	if (res->type == QueryResultType::STREAM_RESULT) {
		res = ((StreamQueryResult &)*res).Materialize();
	}
	D_ASSERT(res->type == QueryResultType::MATERIALIZED_RESULT);
	auto mat_res = (MaterializedQueryResult *)res.get();
	return duckdb_execute_R_impl(mat_res);
}
