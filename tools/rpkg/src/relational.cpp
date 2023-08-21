#include "cpp11.hpp"
#include "duckdb.hpp"
#include "typesr.hpp"
#include "rapi.hpp"

#include "R_ext/Random.h"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"

#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/main/relation/order_relation.hpp"
#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/relation/cross_product_relation.hpp"
#include "duckdb/main/relation/setop_relation.hpp"
#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/main/relation/distinct_relation.hpp"
#include "duckdb/main/relation/table_function_relation.hpp"

#include "duckdb/common/enums/joinref_type.hpp"

using namespace duckdb;
using namespace cpp11;

template <typename T, typename... ARGS>
external_pointer<T> make_external(const string &rclass, ARGS &&... args) {
	auto extptr = external_pointer<T>(new T(std::forward<ARGS>(args)...));
	((sexp)extptr).attr("class") = rclass;
	return extptr;
}

template <typename T, typename... ARGS>
external_pointer<T> make_external_prot(const string &rclass, SEXP prot, ARGS &&... args) {
	auto extptr = external_pointer<T>(new T(std::forward<ARGS>(args)...), true, true, prot);
	((sexp)extptr).attr("class") = rclass;
	return extptr;
}
// DuckDB Expressions

[[cpp11::register]] SEXP rapi_expr_reference(std::string name, std::string table) {
	if (name.size() == 0) {
		stop("expr_reference: Zero length name");
	}
	if (!table.empty()) {
		auto res = make_external<ColumnRefExpression>("duckdb_expr", name, table);
		res->alias = name; // TODO does this really make sense here?
		return res;
	} else {
		return make_external<ColumnRefExpression>("duckdb_expr", name);
	}
}

[[cpp11::register]] SEXP rapi_expr_constant(sexp val) {
	if (LENGTH(val) != 1) {
		stop("expr_constant: Need value of length one");
	}
	return make_external<ConstantExpression>("duckdb_expr", RApiTypes::SexpToValue(val, 0));
}

[[cpp11::register]] SEXP rapi_expr_function(std::string name, list args, list order_bys, list filter_bys) {
	if (name.size() == 0) {
		stop("expr_function: Zero length name");
	}
	vector<duckdb::unique_ptr<ParsedExpression>> children;
	for (auto arg : args) {
		children.push_back(expr_extptr_t(arg)->Copy());
		// remove the alias since it is assumed to be the name of the argument for the function
		// i.e if you have CREATE OR REPLACE MACRO eq(a, b) AS a = b
		// and a function expr_function("eq", list(expr_reference("left_b", left), expr_reference("right_b", right)))
		// then the macro gets called with eq(left_b=left.left_b, right_b=right.right_b)
		// and an error is thrown. If the alias is removed, the error is not thrown.
		children.back()->alias = "";
	}

	// For aggregates you can add orders
	auto order_modifier = make_uniq<OrderModifier>();
	for (expr_extptr_t expr : order_bys) {
		order_modifier->orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, expr->Copy());
	}

	// For Aggregates you can add filters
	unique_ptr<ParsedExpression> filter_expr;
	if (filter_bys.size() == 1) {
		filter_expr = ((expr_extptr_t)filter_bys[0])->Copy();
	} else {
		vector<unique_ptr<ParsedExpression>> filters;
		for (expr_extptr_t expr : filter_bys) {
			filters.push_back(expr->Copy());
		}
		filter_expr = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(filters));
	}

	auto func_expr = make_external<FunctionExpression>("duckdb_expr", name, std::move(children));
	if (!order_bys.empty()) {
		func_expr->order_bys = std::move(order_modifier);
	}
	if (!filter_bys.empty()) {
		func_expr->filter = std::move(filter_expr);
	}
	return func_expr;
}

[[cpp11::register]] void rapi_expr_set_alias(duckdb::expr_extptr_t expr, std::string alias) {
	expr->alias = alias;
}

[[cpp11::register]] std::string rapi_expr_tostring(duckdb::expr_extptr_t expr) {
	return expr->ToString();
}

// DuckDB Relations

[[cpp11::register]] SEXP rapi_rel_from_df(duckdb::conn_eptr_t con, data_frame df, bool experimental) {
	if (!con || !con.get() || !con->conn) {
		stop("rel_from_df: Invalid connection");
	}
	if (df.size() == 0) {
		stop("rel_from_df: Invalid data frame");
	}

	named_parameter_map_t other_params;
	other_params["experimental"] = Value::BOOLEAN(experimental);
	auto alias = StringUtil::Format("dataframe_%d_%d", (uintptr_t)(SEXP)df,
	                                (int32_t)(NumericLimits<int32_t>::Maximum() * unif_rand()));
	auto rel =
	    con->conn->TableFunction("r_dataframe_scan", {Value::POINTER((uintptr_t)(SEXP)df)}, other_params)->Alias(alias);

	cpp11::writable::list prot = {df};

	auto res = sexp(make_external_prot<RelationWrapper>("duckdb_relation", prot, std::move(rel)));
	res.attr("df") = df;
	return res;
}

[[cpp11::register]] SEXP rapi_rel_filter(duckdb::rel_extptr_t rel, list exprs) {
	duckdb::unique_ptr<ParsedExpression> filter_expr;
	if (exprs.size() == 0) { // nop
		warning("rel_filter without filter expressions has no effect");
		return rel;
	} else if (exprs.size() == 1) {
		filter_expr = ((expr_extptr_t)exprs[0])->Copy();
	} else {
		vector<duckdb::unique_ptr<ParsedExpression>> filters;
		for (expr_extptr_t expr : exprs) {
			filters.push_back(expr->Copy());
		}
		filter_expr = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(filters));
	}
	auto res = std::make_shared<FilterRelation>(rel->rel, std::move(filter_expr));

	cpp11::writable::list prot = {rel};

	return make_external_prot<RelationWrapper>("duckdb_relation", prot, res);
}

[[cpp11::register]] SEXP rapi_rel_project(duckdb::rel_extptr_t rel, list exprs) {
	if (exprs.size() == 0) {
		warning("rel_project without projection expressions has no effect");
		return rel;
	}
	vector<duckdb::unique_ptr<ParsedExpression>> projections;
	vector<string> aliases;

	for (expr_extptr_t expr : exprs) {
		auto dexpr = expr->Copy();
		aliases.push_back(dexpr->alias.empty() ? dexpr->ToString() : dexpr->alias);
		projections.push_back(std::move(dexpr));
	}

	auto res = std::make_shared<ProjectionRelation>(rel->rel, std::move(projections), std::move(aliases));

	cpp11::writable::list prot = {rel};

	return make_external_prot<RelationWrapper>("duckdb_relation", prot, res);
}

[[cpp11::register]] SEXP rapi_rel_aggregate(duckdb::rel_extptr_t rel, list groups, list aggregates) {
	vector<duckdb::unique_ptr<ParsedExpression>> res_groups, res_aggregates;

	// TODO deal with empty groups
	vector<string> aliases;

	for (expr_extptr_t expr : groups) {
		res_groups.push_back(expr->Copy());
		res_aggregates.push_back(expr->Copy());
	}

	int aggr_idx = 0; // has to be int for - reasons
	auto aggr_names = aggregates.names();

	for (expr_extptr_t expr_p : aggregates) {
		auto expr = expr_p->Copy();
		if (aggr_names.size() > aggr_idx) {
			expr->alias = aggr_names[aggr_idx];
		}
		res_aggregates.push_back(std::move(expr));
		aggr_idx++;
	}

	auto res = std::make_shared<AggregateRelation>(rel->rel, std::move(res_aggregates), std::move(res_groups));

	cpp11::writable::list prot = {rel};

	return make_external_prot<RelationWrapper>("duckdb_relation", prot, res);
}

[[cpp11::register]] SEXP rapi_rel_order(duckdb::rel_extptr_t rel, list orders) {
	vector<OrderByNode> res_orders;

	for (expr_extptr_t expr : orders) {
		res_orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, expr->Copy());
	}

	auto res = std::make_shared<OrderRelation>(rel->rel, std::move(res_orders));

	cpp11::writable::list prot = {rel};

	return make_external_prot<RelationWrapper>("duckdb_relation", prot, res);
}

static WindowBoundary StringToWindowBoundary(string &window_boundary) {
	if (window_boundary == "unbounded_preceding") {
		return WindowBoundary::UNBOUNDED_PRECEDING;
	} else if (window_boundary == "unbounded_following") {
		return WindowBoundary::UNBOUNDED_FOLLOWING;
	} else if (window_boundary == "current_row_range") {
		return WindowBoundary::CURRENT_ROW_RANGE;
	} else if (window_boundary == "current_row_rows") {
		return WindowBoundary::CURRENT_ROW_ROWS;
	} else if (window_boundary == "expr_preceding_rows") {
		return WindowBoundary::EXPR_PRECEDING_ROWS;
	} else if (window_boundary == "expr_following_rows") {
		return WindowBoundary::EXPR_FOLLOWING_ROWS;
	} else if (window_boundary == "expr_preceding_range") {
		return WindowBoundary::EXPR_PRECEDING_RANGE;
	} else {
		return WindowBoundary::EXPR_FOLLOWING_RANGE;
	}
}

bool constant_expression_is_not_null(duckdb::expr_extptr_t expr) {
	if (expr->type == ExpressionType::VALUE_CONSTANT) {
		auto const_expr = expr->Cast<ConstantExpression>();
		return !const_expr.value.IsNull();
	}
	return true;
}

[[cpp11::register]] SEXP rapi_expr_window(duckdb::expr_extptr_t window_function, list partitions, list order_bys,
                                          std::string window_boundary_start, std::string window_boundary_end,
                                          duckdb::expr_extptr_t start_expr, duckdb::expr_extptr_t end_expr,
                                          duckdb::expr_extptr_t offset_expr, duckdb::expr_extptr_t default_expr) {

	if (!window_function || window_function->type != ExpressionType::FUNCTION) {
		stop("expected function expression");
	}

	auto &function = (FunctionExpression &)*window_function;
	auto window_type = WindowExpression::WindowToExpressionType(function.function_name);
	auto window_expr = make_external<WindowExpression>("duckdb_expr", window_type, "", "", function.function_name);

	for (expr_extptr_t expr : order_bys) {
		window_expr->orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, expr->Copy());
	}

	if (function.filter) {
		window_expr->filter_expr = function.filter->Copy();
	}

	window_expr->start = StringToWindowBoundary(window_boundary_start);
	window_expr->end = StringToWindowBoundary(window_boundary_end);
	for (auto &child : function.children) {
		window_expr->children.push_back(child->Copy());
	}
	for (expr_extptr_t partition : partitions) {
		window_expr->partitions.push_back(partition->Copy());
	}

	if (constant_expression_is_not_null(start_expr)) {
		window_expr->start_expr = start_expr->Copy();
	}
	if (constant_expression_is_not_null(end_expr)) {
		window_expr->end_expr = end_expr->Copy();
	}
	if (constant_expression_is_not_null(offset_expr)) {
		window_expr->offset_expr = offset_expr->Copy();
	}
	if (constant_expression_is_not_null(default_expr)) {
		window_expr->default_expr = default_expr->Copy();
	}

	return window_expr;
}

[[cpp11::register]] SEXP rapi_rel_join(duckdb::rel_extptr_t left, duckdb::rel_extptr_t right, list conds,
                                       std::string join, std::string join_ref_type) {
	auto join_type = JoinType::INNER;
	auto ref_type = JoinRefType::REGULAR;
	unique_ptr<ParsedExpression> cond;

	if (join_ref_type == "regular") {
		ref_type = JoinRefType::REGULAR;
	} else if (join_ref_type == "cross") {
		ref_type = JoinRefType::CROSS;
	} else if (join_ref_type == "positional") {
		ref_type = JoinRefType::POSITIONAL;
	} else if (join_ref_type == "asof") {
		ref_type = JoinRefType::ASOF;
	}

	cpp11::writable::list prot = {left, right};

	if (join == "left") {
		join_type = JoinType::LEFT;
	} else if (join == "right") {
		join_type = JoinType::RIGHT;
	} else if (join == "outer") {
		join_type = JoinType::OUTER;
	} else if (join == "semi") {
		join_type = JoinType::SEMI;
	} else if (join == "anti") {
		join_type = JoinType::ANTI;
	} else if (join == "cross" || ref_type == JoinRefType::POSITIONAL) {
		if (ref_type != JoinRefType::POSITIONAL && ref_type != JoinRefType::CROSS) {
			// users can only supply positional cross join, or cross join.
			warning("Using `rel_join(join_ref_type = \"cross\")`");
			ref_type = JoinRefType::CROSS;
		}
		auto res = std::make_shared<CrossProductRelation>(left->rel, right->rel, ref_type);
		auto rel = make_external_prot<RelationWrapper>("duckdb_relation", prot, res);
		// if the user described filters, apply them on top of the cross product relation
		if (conds.size() > 0) {
			return rapi_rel_filter(rel, conds);
		}
		return rel;
	}

	if (conds.size() == 1) {
		cond = ((expr_extptr_t)conds[0])->Copy();
	} else {
		vector<duckdb::unique_ptr<ParsedExpression>> cond_args;
		for (expr_extptr_t expr : conds) {
			cond_args.push_back(expr->Copy());
		}
		cond = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(cond_args));
	}

	auto res = std::make_shared<JoinRelation>(left->rel, right->rel, std::move(cond), join_type, ref_type);
	return make_external_prot<RelationWrapper>("duckdb_relation", prot, res);
}

static SEXP result_to_df(duckdb::unique_ptr<QueryResult> res) {
	if (res->HasError()) {
		stop("%s", res->GetError());
	}
	if (res->type == QueryResultType::STREAM_RESULT) {
		res = ((StreamQueryResult &)*res).Materialize();
	}
	D_ASSERT(res->type == QueryResultType::MATERIALIZED_RESULT);
	auto mat_res = (MaterializedQueryResult *)res.get();

	writable::integers row_names;
	row_names.push_back(NA_INTEGER);
	row_names.push_back(-mat_res->RowCount());

	// TODO this thing we can probably statically cache
	writable::strings classes;
	classes.push_back("tbl_df");
	classes.push_back("tbl");
	classes.push_back("data.frame");

	auto df = sexp(duckdb_execute_R_impl(mat_res, false));
	df.attr("class") = classes;
	df.attr("row.names") = row_names;
	return df;
}

[[cpp11::register]] SEXP rapi_rel_union_all(duckdb::rel_extptr_t rel_a, duckdb::rel_extptr_t rel_b) {
	auto res = std::make_shared<SetOpRelation>(rel_a->rel, rel_b->rel, SetOperationType::UNION);

	cpp11::writable::list prot = {rel_a, rel_b};

	return make_external_prot<RelationWrapper>("duckdb_relation", prot, res);
}

[[cpp11::register]] SEXP rapi_rel_limit(duckdb::rel_extptr_t rel, int64_t n) {

	cpp11::writable::list prot = {rel};

	return make_external_prot<RelationWrapper>("duckdb_relation", prot,
	                                           std::make_shared<LimitRelation>(rel->rel, n, 0));
}

[[cpp11::register]] SEXP rapi_rel_distinct(duckdb::rel_extptr_t rel) {

	cpp11::writable::list prot = {rel};

	return make_external_prot<RelationWrapper>("duckdb_relation", prot, std::make_shared<DistinctRelation>(rel->rel));
}

[[cpp11::register]] SEXP rapi_rel_to_df(duckdb::rel_extptr_t rel) {
	return result_to_df(rel->rel->Execute());
}

[[cpp11::register]] std::string rapi_rel_tostring(duckdb::rel_extptr_t rel) {
	return rel->rel->ToString();
}

[[cpp11::register]] std::string rapi_rel_to_sql(duckdb::rel_extptr_t rel) {
	return rel->rel->GetQueryNode()->ToString();
}

[[cpp11::register]] SEXP rapi_rel_explain(duckdb::rel_extptr_t rel) {
	return result_to_df(rel->rel->Explain());
}

[[cpp11::register]] std::string rapi_rel_alias(duckdb::rel_extptr_t rel) {
	return rel->rel->GetAlias();
}

// Call this function to avoid passing an empty list if an argument is optional
[[cpp11::register]] SEXP rapi_get_null_SEXP_ptr() {
	auto ret = make_external<ConstantExpression>("duckdb_null_ptr", nullptr);
	return ret;
}

[[cpp11::register]] SEXP rapi_rel_set_alias(duckdb::rel_extptr_t rel, std::string alias) {
	cpp11::writable::list prot = {rel};

	return make_external_prot<RelationWrapper>("duckdb_relation", prot, rel->rel->Alias(alias));
}

[[cpp11::register]] SEXP rapi_rel_sql(duckdb::rel_extptr_t rel, std::string sql) {
	auto res = rel->rel->Query("_", sql);
	if (res->HasError()) {
		stop("%s", res->GetError());
	}
	return result_to_df(std::move(res));
}

[[cpp11::register]] SEXP rapi_rel_names(duckdb::rel_extptr_t rel) {
	auto ret = writable::strings();
	for (auto &col : rel->rel->Columns()) {
		ret.push_back(col.Name());
	}
	return (ret);
}

[[cpp11::register]] SEXP rapi_rel_set_intersect(duckdb::rel_extptr_t rel_a, duckdb::rel_extptr_t rel_b) {
	auto res = std::make_shared<SetOpRelation>(rel_a->rel, rel_b->rel, SetOperationType::INTERSECT);

	cpp11::writable::list prot = {rel_a, rel_b};

	return make_external_prot<RelationWrapper>("duckdb_relation", prot, res);
}

[[cpp11::register]] SEXP rapi_rel_set_diff(duckdb::rel_extptr_t rel_a, duckdb::rel_extptr_t rel_b) {
	auto res = std::make_shared<SetOpRelation>(rel_a->rel, rel_b->rel, SetOperationType::EXCEPT);

	cpp11::writable::list prot = {rel_a, rel_b};

	return make_external_prot<RelationWrapper>("duckdb_relation", prot, res);
}

[[cpp11::register]] SEXP rapi_rel_set_symdiff(duckdb::rel_extptr_t rel_a, duckdb::rel_extptr_t rel_b) {
	// symdiff implemented using the equation below
	// A symdiff B = (A except B) UNION (B except A)
	auto a_except_b = std::make_shared<SetOpRelation>(rel_a->rel, rel_b->rel, SetOperationType::EXCEPT);
	auto b_except_a = std::make_shared<SetOpRelation>(rel_b->rel, rel_a->rel, SetOperationType::EXCEPT);
	auto symdiff = std::make_shared<SetOpRelation>(a_except_b, b_except_a, SetOperationType::UNION);

	cpp11::writable::list prot = {rel_a, rel_b};

	return make_external_prot<RelationWrapper>("duckdb_relation", prot, symdiff);
}

[[cpp11::register]] SEXP rapi_rel_from_table(duckdb::conn_eptr_t con, const std::string schema_name,
                                             const std::string table_name) {
	if (!con || !con.get() || !con->conn) {
		stop("rel_from_table: Invalid connection");
	}
	auto desc = make_uniq<TableDescription>();
	auto rel = con->conn->Table(schema_name, table_name);
	cpp11::writable::list prot = {};
	return make_external_prot<RelationWrapper>("duckdb_relation", prot, std::move(rel));
}

[[cpp11::register]] SEXP rapi_rel_from_table_function(duckdb::conn_eptr_t con, const std::string function_name,
                                                      list positional_parameters_sexps, list named_parameters_sexps) {
	if (!con || !con.get() || !con->conn) {
		stop("rel_from_table_function: Invalid connection");
	}
	vector<Value> positional_parameters;

	for (sexp parameter_sexp : positional_parameters_sexps) {
		if (LENGTH(parameter_sexp) < 1) {
			stop("rel_from_table_function: Can't have zero-length parameter");
		}
		positional_parameters.push_back(RApiTypes::SexpToValue(parameter_sexp, 0));
	}

	named_parameter_map_t named_parameters;

	auto names = named_parameters_sexps.names();
	if (names.size() != named_parameters_sexps.size()) {
		stop("rel_from_table_function: Named parameters need names");
	}
	R_xlen_t named_parameter_idx = 0;
	for (sexp parameter_sexp : named_parameters_sexps) {
		if (LENGTH(parameter_sexp) != 1) {
			stop("rel_from_table_function: Need scalar parameter");
		}
		named_parameters[names[named_parameter_idx]] = RApiTypes::SexpToValue(parameter_sexp, 0);
		named_parameter_idx++;
	}

	auto rel = con->conn->TableFunction(function_name, std::move(positional_parameters), std::move(named_parameters));
	return make_external<RelationWrapper>("duckdb_relation", std::move(rel));
}
