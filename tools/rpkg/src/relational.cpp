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
external_pointer<T> make_external(const string &rclass, Args &&...args) {
	auto extptr = external_pointer<T>(new T(std::forward<Args>(args)...));
	((sexp)extptr).attr("class") = rclass;
	return (extptr);
}

// DuckDB Expressions

[[cpp11::register]] SEXP expr_reference_cpp(std::string ref) {
	if (ref.size() == 0) {
		stop("expr_reference: Zero length name");
	}
	return make_external<ColumnRefExpression>("duckdb_expr", ref);
}

[[cpp11::register]] SEXP expr_constant_cpp(sexp val) {
	if (LENGTH(val) != 1) {
		stop("expr_constant: Need value of length one");
	}
	return make_external<ConstantExpression>("duckdb_expr", RApiTypes::SexpToValue(val, 0));
}

[[cpp11::register]] SEXP expr_function_cpp(std::string name, list args) {
	if (name.size() == 0) {
		stop("expr_function: Zero length name");
	}
	vector<unique_ptr<ParsedExpression>> children;
	for (auto arg : args) {
		children.push_back(expr_extptr(arg)->Copy());
	}
	auto operator_type = OperatorToExpressionType(name);
	if (operator_type != ExpressionType::INVALID && children.size() == 2) {
		return make_external<ComparisonExpression>("duckdb_expr", operator_type, move(children[0]), move(children[1]));
	} else if (name == "||") {
		return make_external<ConjunctionExpression>("duckdb_expr", ExpressionType::CONJUNCTION_OR, move(children[0]),
		                                            move(children[1]));
	}
	if (name == "mean") {
		name = "avg";
	} else if (name == "n") {
		name = "count_star";
	}
	return make_external<FunctionExpression>("duckdb_expr", name, move(children));
}

[[cpp11::register]] std::string expr_tostring_cpp(duckdb::expr_extptr expr) {
	return expr->ToString();
}

// DuckDB Relations

[[cpp11::register]] SEXP rel_from_df_cpp(duckdb::con_extptr con, data_frame df) {
	if (!con->conn) {
		stop("rel_from_df: Invalid connection");
	}
	if (df.size() == 0) {
		stop("rel_from_df: Invalid data frame");
	}

	auto rel = con->conn->TableFunction("r_dataframe_scan", {Value::POINTER((uintptr_t)(SEXP)df)});
	auto res = sexp(make_external<RelationWrapper>("duckdb_relation", move(rel)));
	res.attr("df") = df;
	return res;
}

[[cpp11::register]] SEXP rel_filter_cpp(duckdb::rel_extptr rel, duckdb::expr_extptr expr) {
	auto res = std::make_shared<FilterRelation>(rel->rel, expr->Copy());
	return make_external<RelationWrapper>("duckdb_relation", res);
}

[[cpp11::register]] SEXP rel_project_cpp(duckdb::rel_extptr rel, list exprs_p) {
	vector<unique_ptr<ParsedExpression>> projections;
	vector<string> aliases;

	for (expr_extptr expr : exprs_p) {
		aliases.push_back((expr->ToString()));
		projections.push_back(expr->Copy());
	}

	auto res = std::make_shared<ProjectionRelation>(rel->rel, move(projections), move(aliases));
	return make_external<RelationWrapper>("duckdb_relation", res);
}

[[cpp11::register]] SEXP rel_aggregate_cpp(duckdb::rel_extptr rel, list groups_p, list aggregates_p) {
	vector<unique_ptr<ParsedExpression>> groups;
	vector<unique_ptr<ParsedExpression>> aggregates;

	// TODO deal with empty groups
	vector<string> aliases;

	for (expr_extptr expr : groups_p) {
		aggregates.push_back(expr->Copy());
		groups.push_back(expr->Copy());
	}

	int aggr_idx = 0; // has to be int for - reasons
	auto aggr_names = aggregates_p.names();
	for (expr_extptr expr_p : aggregates_p) {
		auto expr = expr_p->Copy();
		expr->alias = aggr_names[aggr_idx];
		aggregates.push_back(move(expr));
		aggr_idx++;
	}

	auto res = std::make_shared<AggregateRelation>(rel->rel, move(aggregates), move(groups));
	return make_external<RelationWrapper>("duckdb_relation", res);
}

[[cpp11::register]] SEXP rel_order_cpp(duckdb::rel_extptr rel, list orders_p) {
	vector<OrderByNode> orders;

	for (expr_extptr expr : orders_p) {
		orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, expr->Copy());
	}

	auto res = std::make_shared<OrderRelation>(rel->rel, move(orders));
	return make_external<RelationWrapper>("duckdb_relation", res);
}

static SEXP result_to_df(unique_ptr<QueryResult> res) {
	if (res->type == QueryResultType::STREAM_RESULT) {
		res = ((StreamQueryResult &)*res).Materialize();
	}
	D_ASSERT(res->type == QueryResultType::MATERIALIZED_RESULT);
	auto mat_res = (MaterializedQueryResult *)res.get();

	writable::integers row_names;
	row_names.push_back(NA_INTEGER);
	row_names.push_back(-mat_res->collection.Count());

	// TODO this thing we can probably cache
	writable::strings classes;
	classes.push_back("tbl_df");
	classes.push_back("tbl");
	classes.push_back("data.frame");

	auto df = sexp(duckdb_execute_R_impl(mat_res));
	df.attr("class") = classes;
	df.attr("row.names") = row_names;
	return df;
}

[[cpp11::register]] SEXP rel_to_df_cpp(duckdb::rel_extptr rel) {
	return result_to_df(rel->rel->Execute());
}

[[cpp11::register]] std::string rel_tostring_cpp(duckdb::rel_extptr rel) {
	return rel->rel->ToString();
}

[[cpp11::register]] SEXP rel_explain_cpp(duckdb::rel_extptr rel) {
	return result_to_df(rel->rel->Explain());
}

[[cpp11::register]] SEXP rel_sql_cpp(duckdb::rel_extptr rel, std::string sql) {
	auto res = rel->rel->Query("_", sql);
	if (!res->success) {
		stop(res->error);
	}
	return result_to_df(move(res));
}
