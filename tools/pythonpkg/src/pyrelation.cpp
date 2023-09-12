#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pytype.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"
#include "duckdb/main/relation/query_relation.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/function/pragma/pragma_functions.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/main/relation/value_relation.hpp"
#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb_python/expression/pyexpression.hpp"

namespace duckdb {

DuckDBPyRelation::DuckDBPyRelation(shared_ptr<Relation> rel_p) : rel(std::move(rel_p)) {
	if (!rel) {
		throw InternalException("DuckDBPyRelation created without a relation");
	}
	this->executed = false;
	auto &columns = rel->Columns();
	for (auto &col : columns) {
		names.push_back(col.GetName());
		types.push_back(col.GetType());
	}
}

DuckDBPyRelation::DuckDBPyRelation(unique_ptr<DuckDBPyResult> result_p) : rel(nullptr), result(std::move(result_p)) {
	if (!result) {
		throw InternalException("DuckDBPyRelation created without a result");
	}
	this->executed = true;
	this->types = result->GetTypes();
	this->names = result->GetNames();
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::ProjectFromExpression(const string &expression) {
	auto projected_relation = make_uniq<DuckDBPyRelation>(rel->Project(expression));
	projected_relation->rel->extra_dependencies = this->rel->extra_dependencies;
	return projected_relation;
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Project(const py::args &args) {
	if (!rel) {
		return nullptr;
	}
	auto arg_count = args.size();
	if (arg_count == 0) {
		return nullptr;
	}
	py::handle first_arg = args[0];
	if (arg_count == 1 && py::isinstance<py::str>(first_arg)) {
		string expr_string = py::str(first_arg);
		return ProjectFromExpression(expr_string);
	} else {
		vector<unique_ptr<ParsedExpression>> expressions;
		for (auto arg : args) {
			shared_ptr<DuckDBPyExpression> py_expr;
			if (!py::try_cast<shared_ptr<DuckDBPyExpression>>(arg, py_expr)) {
				throw InvalidInputException("Please provide arguments of type Expression!");
			}
			auto expr = py_expr->GetExpression().Copy();
			expressions.push_back(std::move(expr));
		}
		vector<string> empty_aliases;
		return make_uniq<DuckDBPyRelation>(rel->Project(std::move(expressions), empty_aliases));
	}
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::ProjectFromTypes(const py::object &obj) {
	if (!rel) {
		return nullptr;
	}
	if (!py::isinstance<py::list>(obj)) {
		throw InvalidInputException("'columns_by_type' expects a list containing types");
	}
	auto list = py::list(obj);
	vector<LogicalType> types_filter;
	// Collect the list of types specified that will be our filter
	for (auto &item : list) {
		LogicalType type;
		if (py::isinstance<py::str>(item)) {
			string type_str = py::str(item);
			type = TransformStringToLogicalType(type_str, *rel->context.GetContext());
		} else if (py::isinstance<DuckDBPyType>(item)) {
			auto *type_p = item.cast<DuckDBPyType *>();
			type = type_p->Type();
		} else {
			string actual_type = py::str(item.get_type());
			throw InvalidInputException("Can only project on objects of type DuckDBPyType or str, not '%s'",
			                            actual_type);
		}
		types_filter.push_back(std::move(type));
	}

	if (types_filter.empty()) {
		throw InvalidInputException("List of types can not be empty!");
	}

	string projection = "";
	for (idx_t i = 0; i < types.size(); i++) {
		auto &type = types[i];
		// Check if any of the types in the filter match the current type
		if (std::find_if(types_filter.begin(), types_filter.end(),
		                 [&](const LogicalType &filter) { return filter == type; }) != types_filter.end()) {
			if (!projection.empty()) {
				projection += ", ";
			}
			projection += names[i];
		}
	}
	if (projection.empty()) {
		throw InvalidInputException("None of the columns matched the provided type filter!");
	}
	return ProjectFromExpression(projection);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::EmptyResult(const std::shared_ptr<ClientContext> &context,
                                                           const vector<LogicalType> &types, vector<string> names) {
	vector<Value> dummy_values;
	D_ASSERT(types.size() == names.size());
	dummy_values.reserve(types.size());
	D_ASSERT(!types.empty());
	for (auto &type : types) {
		dummy_values.emplace_back(type);
	}
	vector<vector<Value>> single_row(1, dummy_values);
	auto values_relation =
	    make_uniq<DuckDBPyRelation>(make_shared<ValueRelation>(context, single_row, std::move(names)));
	// Add a filter on an impossible condition
	return values_relation->FilterFromExpression("true = false");
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::SetAlias(const string &expr) {
	return make_uniq<DuckDBPyRelation>(rel->Alias(expr));
}

py::str DuckDBPyRelation::GetAlias() {
	return py::str(string(rel->GetAlias()));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Filter(const py::object &expr) {
	if (py::isinstance<py::str>(expr)) {
		string expression = py::cast<py::str>(expr);
		return FilterFromExpression(expression);
	}
	shared_ptr<DuckDBPyExpression> expression;
	if (!py::try_cast(expr, expression)) {
		throw InvalidInputException("Please provide either a string or a DuckDBPyExpression object to 'filter'");
	}
	auto expr_p = expression->GetExpression().Copy();
	return make_uniq<DuckDBPyRelation>(rel->Filter(std::move(expr_p)));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FilterFromExpression(const string &expr) {
	return make_uniq<DuckDBPyRelation>(rel->Filter(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Limit(int64_t n, int64_t offset) {
	return make_uniq<DuckDBPyRelation>(rel->Limit(n, offset));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Order(const string &expr) {
	return make_uniq<DuckDBPyRelation>(rel->Order(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Sort(const py::args &args) {
	vector<OrderByNode> order_nodes;
	order_nodes.reserve(args.size());

	for (auto arg : args) {
		shared_ptr<DuckDBPyExpression> py_expr;
		if (!py::try_cast<shared_ptr<DuckDBPyExpression>>(arg, py_expr)) {
			string actual_type = py::str(arg.get_type());
			throw InvalidInputException("Expected argument of type Expression, received '%s' instead", actual_type);
		}
		auto expr = py_expr->GetExpression().Copy();
		order_nodes.emplace_back(py_expr->order_type, py_expr->null_order, std::move(expr));
	}
	if (order_nodes.empty()) {
		throw InvalidInputException("Please provide at least one expression to sort on");
	}
	return make_uniq<DuckDBPyRelation>(rel->Order(std::move(order_nodes)));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Aggregate(const string &expr, const string &groups) {
	if (!groups.empty()) {
		return make_uniq<DuckDBPyRelation>(rel->Aggregate(expr, groups));
	}
	return make_uniq<DuckDBPyRelation>(rel->Aggregate(expr));
}

void DuckDBPyRelation::AssertResult() const {
	if (!result) {
		throw InvalidInputException("No open result set");
	}
}

void DuckDBPyRelation::AssertRelation() const {
	if (!rel) {
		throw InvalidInputException("This relation was created from a result");
	}
}

void DuckDBPyRelation::AssertResultOpen() const {
	if (!result || result->IsClosed()) {
		throw InvalidInputException("No open result set");
	}
}

py::list DuckDBPyRelation::Description() {
	return DuckDBPyResult::GetDescription(names, types);
}

Relation &DuckDBPyRelation::GetRel() {
	if (!rel) {
		throw InternalException("DuckDBPyRelation - calling GetRel, but no rel was present");
	}
	return *rel;
}

struct DescribeAggregateInfo {
	explicit DescribeAggregateInfo(string name_p, bool numeric_only = false)
	    : name(std::move(name_p)), numeric_only(numeric_only) {
	}

	string name;
	bool numeric_only;
};

vector<string> CreateExpressionList(const vector<ColumnDefinition> &columns,
                                    const vector<DescribeAggregateInfo> &aggregates) {
	vector<string> expressions;
	expressions.reserve(columns.size());

	string aggr_names = "UNNEST([";
	for (idx_t i = 0; i < aggregates.size(); i++) {
		if (i > 0) {
			aggr_names += ", ";
		}
		aggr_names += "'";
		aggr_names += aggregates[i].name;
		aggr_names += "'";
	}
	aggr_names += "])";
	aggr_names += " AS aggr";
	expressions.push_back(aggr_names);
	for (idx_t c = 0; c < columns.size(); c++) {
		auto &col = columns[c];
		string expr = "UNNEST([";
		for (idx_t i = 0; i < aggregates.size(); i++) {
			if (i > 0) {
				expr += ", ";
			}
			if (aggregates[i].numeric_only && !col.GetType().IsNumeric()) {
				expr += "NULL";
				continue;
			}
			expr += aggregates[i].name;
			expr += "(";
			expr += KeywordHelper::WriteOptionallyQuoted(col.GetName());
			expr += ")";
			if (col.GetType().IsNumeric()) {
				expr += "::DOUBLE";
			} else {
				expr += "::VARCHAR";
			}
		}
		expr += "])";
		expr += " AS " + KeywordHelper::WriteOptionallyQuoted(col.GetName());
		expressions.push_back(expr);
	}
	return expressions;
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Describe() {
	auto &columns = rel->Columns();
	vector<DescribeAggregateInfo> aggregates;
	aggregates = {DescribeAggregateInfo("count"),        DescribeAggregateInfo("mean", true),
	              DescribeAggregateInfo("stddev", true), DescribeAggregateInfo("min"),
	              DescribeAggregateInfo("max"),          DescribeAggregateInfo("median", true)};
	auto expressions = CreateExpressionList(columns, aggregates);
	return make_uniq<DuckDBPyRelation>(rel->Aggregate(expressions));
}

string DuckDBPyRelation::ToSQL() {
	if (!rel) {
		// This relation is just a wrapper around a result set, can't figure out what the SQL was
		return "";
	}
	try {
		return rel->GetQueryNode()->ToString();
	} catch (const std::exception &e) {
		return "";
	}
}

string DuckDBPyRelation::GenerateExpressionList(const string &function_name, const string &aggregated_columns,
                                                const string &groups, const string &function_parameter,
                                                const bool &ignore_nulls, const string &projected_columns,
                                                const string &window_spec) {
	auto input = StringUtil::Split(aggregated_columns, ',');
	return GenerateExpressionList(function_name, input, groups, function_parameter, ignore_nulls, projected_columns,
	                              window_spec);
}

string DuckDBPyRelation::GenerateExpressionList(const string &function_name, const vector<string> &input,
                                                const string &groups, const string &function_parameter,
                                                const bool &ignore_nulls, const string &projected_columns,
                                                const string &window_spec) {
	string expr;
	if (!projected_columns.empty()) {
		expr = projected_columns + ", ";
	}
	if (input.empty() && !function_parameter.empty()) {
		return expr +=
		       function_name + "(" + function_parameter + ((ignore_nulls) ? " ignore nulls) " : ") ") + window_spec;
	}
	for (idx_t i = 0; i < input.size(); i++) {
		if (function_parameter.empty()) {
			expr += function_name + "(" + input[i] + ((ignore_nulls) ? " ignore nulls) " : ") ") + window_spec;
		} else {
			expr += function_name + "(" + input[i] + "," + function_parameter +
			        ((ignore_nulls) ? " ignore nulls) " : ") ") + window_spec;
		}

		if (i < input.size() - 1) {
			expr += ",";
		}
	}
	return expr;
}

/* General aggregate functions */

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::GenericAggregator(const string &function_name,
                                                                 const string &aggregated_columns, const string &groups,
                                                                 const string &function_parameter,
                                                                 const string &projected_columns) {

	//! Construct Aggregation Expression
	auto expr = GenerateExpressionList(function_name, aggregated_columns, groups, function_parameter, false,
	                                   projected_columns, "");
	return Aggregate(expr, groups);
}

unique_ptr<DuckDBPyRelation>
DuckDBPyRelation::GenericWindowFunction(const string &function_name, const string &function_parameters,
                                        const string &aggr_columns, const string &window_spec, const bool &ignore_nulls,
                                        const string &projected_columns) {
	auto expr = GenerateExpressionList(function_name, aggr_columns, "", function_parameters, ignore_nulls,
	                                   projected_columns, window_spec);
	return make_uniq<DuckDBPyRelation>(rel->Project(expr));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::ApplyAggOrWin(const string &function_name, const string &agg_columns,
                                                             const string &function_parameters, const string &groups,
                                                             const string &window_spec, const string &projected_columns,
                                                             const bool &ignore_nulls) {
	if (!groups.empty() && !window_spec.empty()) {
		throw InvalidInputException("Either groups or window must be set (can't be both at the same time)");
	}
	if (!window_spec.empty()) {
		return GenericWindowFunction(function_name, function_parameters, agg_columns, window_spec, ignore_nulls,
		                             projected_columns);
	} else {
		return GenericAggregator(function_name, agg_columns, groups, function_parameters, projected_columns);
	}
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::AnyValue(const std::string &column, const std::string &groups,
                                                        const std::string &window_spec,
                                                        const std::string &projected_columns) {
	return ApplyAggOrWin("any_value", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::ArgMax(const std::string &arg_column, const std::string &value_column,
                                                      const std::string &groups, const std::string &window_spec,
                                                      const std::string &projected_columns) {
	return ApplyAggOrWin("arg_max", arg_column, value_column, groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::ArgMin(const std::string &arg_column, const std::string &value_column,
                                                      const std::string &groups, const std::string &window_spec,
                                                      const std::string &projected_columns) {
	return ApplyAggOrWin("arg_min", arg_column, value_column, groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Avg(const std::string &column, const std::string &groups,
                                                   const std::string &window_spec,
                                                   const std::string &projected_columns) {
	return ApplyAggOrWin("avg", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::BitAnd(const std::string &column, const std::string &groups,
                                                      const std::string &window_spec,
                                                      const std::string &projected_columns) {
	return ApplyAggOrWin("bit_and", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::BitOr(const std::string &column, const std::string &groups,
                                                     const std::string &window_spec,
                                                     const std::string &projected_columns) {
	return ApplyAggOrWin("bit_or", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::BitXor(const std::string &column, const std::string &groups,
                                                      const std::string &window_spec,
                                                      const std::string &projected_columns) {
	return ApplyAggOrWin("bit_xor", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::BitStringAgg(const std::string &column, const Optional<py::object> &min,
                                                            const Optional<py::object> &max, const std::string &groups,
                                                            const std::string &window_spec,
                                                            const std::string &projected_columns) {
	if ((min.is_none() && !max.is_none()) || (!min.is_none() && max.is_none())) {
		throw InvalidInputException("Both min and max values must be set");
	}
	if (!min.is_none()) {
		if (!py::isinstance<py::int_>(min) || !py::isinstance<py::int_>(max)) {
			throw InvalidTypeException("min and max must be of type int");
		}
	}
	auto bitstring_agg_params =
	    min.is_none() ? "" : (std::to_string(min.cast<int>()) + "," + std::to_string(max.cast<int>()));
	return ApplyAggOrWin("bitstring_agg", column, bitstring_agg_params, groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::BoolAnd(const std::string &column, const std::string &groups,
                                                       const std::string &window_spec,
                                                       const std::string &projected_columns) {
	return ApplyAggOrWin("bool_and", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::BoolOr(const std::string &column, const std::string &groups,
                                                      const std::string &window_spec,
                                                      const std::string &projected_columns) {
	return ApplyAggOrWin("bool_or", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Count(const std::string &column, const std::string &groups,
                                                     const std::string &window_spec,
                                                     const std::string &projected_columns) {
	return ApplyAggOrWin("count", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FAvg(const std::string &column, const std::string &groups,
                                                    const std::string &window_spec,
                                                    const std::string &projected_columns) {
	return ApplyAggOrWin("favg", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::First(const string &column, const std::string &groups,
                                                     const string &projected_columns) {
	return GenericAggregator("first", column, groups, "", projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FSum(const std::string &column, const std::string &groups,
                                                    const std::string &window_spec,
                                                    const std::string &projected_columns) {
	return ApplyAggOrWin("fsum", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::GeoMean(const std::string &column, const std::string &groups,
                                                       const std::string &projected_columns) {
	return GenericAggregator("geomean", column, groups, "", projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Histogram(const std::string &column, const std::string &groups,
                                                         const std::string &window_spec,
                                                         const std::string &projected_columns) {
	return ApplyAggOrWin("histogram", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::List(const std::string &column, const std::string &groups,
                                                    const std::string &window_spec,
                                                    const std::string &projected_columns) {
	return ApplyAggOrWin("list", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Last(const std::string &column, const std::string &groups,
                                                    const std::string &projected_columns) {
	return GenericAggregator("last", column, groups, "", projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Max(const std::string &column, const std::string &groups,
                                                   const std::string &window_spec,
                                                   const std::string &projected_columns) {
	return ApplyAggOrWin("max", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Min(const std::string &column, const std::string &groups,
                                                   const std::string &window_spec,
                                                   const std::string &projected_columns) {
	return ApplyAggOrWin("min", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Product(const std::string &column, const std::string &groups,
                                                       const std::string &window_spec,
                                                       const std::string &projected_columns) {
	return ApplyAggOrWin("product", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::StringAgg(const std::string &column, const std::string &sep,
                                                         const std::string &groups, const std::string &window_spec,
                                                         const std::string &projected_columns) {
	auto string_agg_params = "\'" + sep + "\'";
	return ApplyAggOrWin("string_agg", column, string_agg_params, groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Sum(const std::string &column, const std::string &groups,
                                                   const std::string &window_spec,
                                                   const std::string &projected_columns) {
	return ApplyAggOrWin("sum", column, "", groups, window_spec, projected_columns);
}

/* TODO: Approximate aggregate functions */

/* TODO: Statistical aggregate functions */
unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Median(const std::string &column, const std::string &groups,
                                                      const std::string &window_spec,
                                                      const std::string &projected_columns) {
	return ApplyAggOrWin("median", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Mode(const std::string &column, const std::string &groups,
                                                    const std::string &window_spec,
                                                    const std::string &projected_columns) {
	return ApplyAggOrWin("mode", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::QuantileCont(const std::string &column, const py::object &q,
                                                            const std::string &groups, const std::string &window_spec,
                                                            const std::string &projected_columns) {
	string quantile_params = "";
	if (py::isinstance<py::float_>(q)) {
		quantile_params = std::to_string(q.cast<float>());
	} else if (py::isinstance<py::list>(q)) {
		auto aux = q.cast<std::vector<double>>();
		quantile_params += "[";
		for (idx_t i = 0; i < aux.size(); i++) {
			quantile_params += std::to_string(aux[i]);
			if (i < aux.size() - 1) {
				quantile_params += ",";
			}
		}
		quantile_params += "]";
	} else {
		throw InvalidTypeException("Unsupported type for quantile");
	}
	return ApplyAggOrWin("quantile_cont", column, quantile_params, groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::QuantileDisc(const std::string &column, const py::object &q,
                                                            const std::string &groups, const std::string &window_spec,
                                                            const std::string &projected_columns) {
	string quantile_params = "";
	if (py::isinstance<py::float_>(q)) {
		quantile_params = std::to_string(q.cast<float>());
	} else if (py::isinstance<py::list>(q)) {
		auto aux = q.cast<std::vector<double>>();
		quantile_params += "[";
		for (idx_t i = 0; i < aux.size(); i++) {
			quantile_params += std::to_string(aux[i]);
			if (i < aux.size() - 1) {
				quantile_params += ",";
			}
		}
		quantile_params += "]";
	} else {
		throw InvalidTypeException("Unsupported type for quantile");
	}
	return ApplyAggOrWin("quantile_disc", column, quantile_params, groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::StdPop(const std::string &column, const std::string &groups,
                                                      const std::string &window_spec,
                                                      const std::string &projected_columns) {
	return ApplyAggOrWin("stddev_pop", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::StdSamp(const std::string &column, const std::string &groups,
                                                       const std::string &window_spec,
                                                       const std::string &projected_columns) {
	return ApplyAggOrWin("stddev_samp", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::VarPop(const std::string &column, const std::string &groups,
                                                      const std::string &window_spec,
                                                      const std::string &projected_columns) {
	return ApplyAggOrWin("var_pop", column, "", groups, window_spec, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::VarSamp(const std::string &column, const std::string &groups,
                                                       const std::string &window_spec,
                                                       const std::string &projected_columns) {
	return ApplyAggOrWin("var_samp", column, "", groups, window_spec, projected_columns);
}

idx_t DuckDBPyRelation::Length() {
	auto aggregate_rel = GenericAggregator("count", "*");
	aggregate_rel->Execute();
	D_ASSERT(aggregate_rel->result);
	auto tmp_res = std::move(aggregate_rel->result);
	return tmp_res->FetchChunk()->GetValue(0, 0).GetValue<idx_t>();
}

py::tuple DuckDBPyRelation::Shape() {
	auto length = Length();
	return py::make_tuple(length, rel->Columns().size());
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Unique(const string &std_columns) {
	return make_uniq<DuckDBPyRelation>(rel->Project(std_columns)->Distinct());
}

/* General-purpose window functions */

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::RowNumber(const string &window_spec, const string &projected_columns) {
	return GenericWindowFunction("row_number", "", "*", window_spec, false, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Rank(const string &window_spec, const string &projected_columns) {
	return GenericWindowFunction("rank", "", "*", window_spec, false, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::DenseRank(const string &window_spec, const string &projected_columns) {
	return GenericWindowFunction("dense_rank", "", "*", window_spec, false, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::PercentRank(const string &window_spec, const string &projected_columns) {
	return GenericWindowFunction("percent_rank", "", "*", window_spec, false, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::CumeDist(const string &window_spec, const string &projected_columns) {
	return GenericWindowFunction("cume_dist", "", "*", window_spec, false, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::FirstValue(const string &column, const string &window_spec,
                                                          const string &projected_columns) {
	return GenericWindowFunction("first_value", "", column, window_spec, false, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::NTile(const string &window_spec, const int &num_buckets,
                                                     const string &projected_columns) {
	return GenericWindowFunction("ntile", std::to_string(num_buckets), "", window_spec, false, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Lag(const string &column, const string &window_spec, const int &offset,
                                                   const string &default_value, const bool &ignore_nulls,
                                                   const string &projected_columns) {
	string lag_params = "";
	if (offset != 0) {
		lag_params += std::to_string(offset);
	}
	if (!default_value.empty()) {
		lag_params += "," + default_value;
	}
	return GenericWindowFunction("lag", lag_params, column, window_spec, ignore_nulls, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::LastValue(const std::string &column, const std::string &window_spec,
                                                         const std::string &projected_columns) {
	return GenericWindowFunction("last_value", "", column, window_spec, false, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Lead(const string &column, const string &window_spec, const int &offset,
                                                    const string &default_value, const bool &ignore_nulls,
                                                    const string &projected_columns) {
	string lead_params = "";
	if (offset != 0) {
		lead_params += std::to_string(offset);
	}
	if (!default_value.empty()) {
		lead_params += "," + default_value;
	}
	return GenericWindowFunction("lead", lead_params, column, window_spec, ignore_nulls, projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::NthValue(const string &column, const string &window_spec,
                                                        const int &offset, const bool &ignore_nulls,
                                                        const string &projected_columns) {
	return GenericWindowFunction("nth_value", std::to_string(offset), column, window_spec, ignore_nulls,
	                             projected_columns);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Distinct() {
	return make_uniq<DuckDBPyRelation>(rel->Distinct());
}

duckdb::pyarrow::RecordBatchReader DuckDBPyRelation::FetchRecordBatchReader(idx_t rows_per_batch) {
	AssertResult();
	return result->FetchRecordBatchReader(rows_per_batch);
}

static unique_ptr<QueryResult> PyExecuteRelation(const shared_ptr<Relation> &rel, bool stream_result = false) {
	if (!rel) {
		return nullptr;
	}
	auto context = rel->context.GetContext();
	py::gil_scoped_release release;
	auto pending_query = context->PendingQuery(rel, stream_result);
	return DuckDBPyConnection::CompletePendingQuery(*pending_query);
}

unique_ptr<QueryResult> DuckDBPyRelation::ExecuteInternal(bool stream_result) {
	this->executed = true;
	return PyExecuteRelation(rel, stream_result);
}

void DuckDBPyRelation::ExecuteOrThrow(bool stream_result) {
	auto query_result = ExecuteInternal(stream_result);
	if (!query_result) {
		throw InternalException("ExecuteOrThrow - no query available to execute");
	}
	if (query_result->HasError()) {
		query_result->ThrowError();
	}
	result = make_uniq<DuckDBPyResult>(std::move(query_result));
}

PandasDataFrame DuckDBPyRelation::FetchDF(bool date_as_object) {
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow();
	}
	if (result->IsClosed()) {
		return py::none();
	}
	auto df = result->FetchDF(date_as_object);
	result = nullptr;
	return df;
}

Optional<py::tuple> DuckDBPyRelation::FetchOne() {
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow(true);
	}
	if (result->IsClosed()) {
		return py::none();
	}
	return result->Fetchone();
}

py::list DuckDBPyRelation::FetchMany(idx_t size) {
	if (!result) {
		if (!rel) {
			return py::list();
		}
		ExecuteOrThrow(true);
		D_ASSERT(result);
	}
	if (result->IsClosed()) {
		return py::list();
	}
	return result->Fetchmany(size);
}

py::list DuckDBPyRelation::FetchAll() {
	if (!result) {
		if (!rel) {
			return py::list();
		}
		ExecuteOrThrow();
	}
	if (result->IsClosed()) {
		return py::list();
	}
	auto res = result->Fetchall();
	result = nullptr;
	return res;
}

py::dict DuckDBPyRelation::FetchNumpy() {
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow();
	}
	if (result->IsClosed()) {
		return py::none();
	}
	auto res = result->FetchNumpy();
	result = nullptr;
	return res;
}

py::dict DuckDBPyRelation::FetchPyTorch() {
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow();
	}
	if (result->IsClosed()) {
		return py::none();
	}
	auto res = result->FetchPyTorch();
	result = nullptr;
	return res;
}

py::dict DuckDBPyRelation::FetchTF() {
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow();
	}
	if (result->IsClosed()) {
		return py::none();
	}
	auto res = result->FetchTF();
	result = nullptr;
	return res;
}

py::dict DuckDBPyRelation::FetchNumpyInternal(bool stream, idx_t vectors_per_chunk) {
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow();
	}
	AssertResultOpen();
	auto res = result->FetchNumpyInternal(stream, vectors_per_chunk);
	result = nullptr;
	return res;
}

//! Should this also keep track of when the result is empty and set result->result_closed accordingly?
PandasDataFrame DuckDBPyRelation::FetchDFChunk(idx_t vectors_per_chunk, bool date_as_object) {
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow(true);
	}
	AssertResultOpen();
	return result->FetchDFChunk(vectors_per_chunk, date_as_object);
}

duckdb::pyarrow::Table DuckDBPyRelation::ToArrowTable(idx_t batch_size) {
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow();
	}
	AssertResultOpen();
	auto res = result->FetchArrowTable(batch_size);
	result = nullptr;
	return res;
}

PolarsDataFrame DuckDBPyRelation::ToPolars(idx_t batch_size) {
	auto arrow = ToArrowTable(batch_size);
	return py::cast<PolarsDataFrame>(pybind11::module_::import("polars").attr("DataFrame")(arrow));
}

duckdb::pyarrow::RecordBatchReader DuckDBPyRelation::ToRecordBatch(idx_t batch_size) {
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow(true);
	}
	AssertResultOpen();
	return result->FetchRecordBatchReader(batch_size);
}

void DuckDBPyRelation::Close() {
	// We always want to execute the query at least once, for side-effect purposes.
	// if it has already been executed, we don't need to do it again.
	if (!executed && !result) {
		if (!rel) {
			return;
		}
		ExecuteOrThrow();
	}
	if (result) {
		result->Close();
	}
}

bool DuckDBPyRelation::ContainsColumnByName(const string &name) const {
	return std::find(names.begin(), names.end(), name) != names.end();
}

static bool ContainsStructFieldByName(LogicalType &type, const string &name) {
	if (type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto count = StructType::GetChildCount(type);
	for (idx_t i = 0; i < count; i++) {
		auto &field_name = StructType::GetChildName(type, i);
		if (StringUtil::CIEquals(name, field_name)) {
			return true;
		}
	}
	return false;
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::GetAttribute(const string &name) {
	// TODO: support fetching a result containing only column 'name' from a value_relation
	if (!rel) {
		throw py::attribute_error(
		    StringUtil::Format("This relation does not contain a column by the name of '%s'", name));
	}
	if (names.size() == 1 && ContainsStructFieldByName(types[0], name)) {
		return make_uniq<DuckDBPyRelation>(rel->Project({StringUtil::Format("%s.%s", names[0], name)}));
	}
	if (ContainsColumnByName(name)) {
		return make_uniq<DuckDBPyRelation>(rel->Project({StringUtil::Format("\"%s\"", name)}));
	}
	throw py::attribute_error(StringUtil::Format("This relation does not contain a column by the name of '%s'", name));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Union(DuckDBPyRelation *other) {
	return make_uniq<DuckDBPyRelation>(rel->Union(other->rel));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Except(DuckDBPyRelation *other) {
	return make_uniq<DuckDBPyRelation>(rel->Except(other->rel));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Intersect(DuckDBPyRelation *other) {
	return make_uniq<DuckDBPyRelation>(rel->Intersect(other->rel));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Join(DuckDBPyRelation *other, const string &condition,
                                                    const string &type) {
	JoinType dtype;
	string type_string = StringUtil::Lower(type);
	StringUtil::Trim(type_string);
	if (type_string == "inner") {
		dtype = JoinType::INNER;
	} else if (type_string == "left") {
		dtype = JoinType::LEFT;
	} else {
		throw InvalidInputException("Unsupported join type %s	 try 'inner' or 'left'", type_string);
	}
	auto alias = GetAlias();
	auto other_alias = other->GetAlias();
	if (StringUtil::CIEquals(alias, other_alias)) {
		throw InvalidInputException("Both relations have the same alias, please change the alias of one or both "
		                            "relations using 'rel = rel.set_alias(<new alias>)'");
	}
	return make_uniq<DuckDBPyRelation>(rel->Join(other->rel, condition, dtype));
}

void DuckDBPyRelation::ToParquet(const string &filename, const py::object &compression) {
	case_insensitive_map_t<vector<Value>> options;

	if (!py::none().is(compression)) {
		if (!py::isinstance<py::str>(compression)) {
			throw InvalidInputException("to_csv only accepts 'compression' as a string");
		}
		options["compression"] = {Value(py::str(compression))};
	}

	auto write_parquet = rel->WriteParquetRel(filename, std::move(options));
	PyExecuteRelation(write_parquet);
}

void DuckDBPyRelation::ToCSV(const string &filename, const py::object &sep, const py::object &na_rep,
                             const py::object &header, const py::object &quotechar, const py::object &escapechar,
                             const py::object &date_format, const py::object &timestamp_format,
                             const py::object &quoting, const py::object &encoding, const py::object &compression) {
	case_insensitive_map_t<vector<Value>> options;

	if (!py::none().is(sep)) {
		if (!py::isinstance<py::str>(sep)) {
			throw InvalidInputException("to_csv only accepts 'sep' as a string");
		}
		options["delimiter"] = {Value(py::str(sep))};
	}

	if (!py::none().is(na_rep)) {
		if (!py::isinstance<py::str>(na_rep)) {
			throw InvalidInputException("to_csv only accepts 'na_rep' as a string");
		}
		options["null"] = {Value(py::str(na_rep))};
	}

	if (!py::none().is(header)) {
		if (!py::isinstance<py::bool_>(header)) {
			throw InvalidInputException("to_csv only accepts 'header' as a boolean");
		}
		options["header"] = {Value::BOOLEAN(py::bool_(header))};
	}

	if (!py::none().is(quotechar)) {
		if (!py::isinstance<py::str>(quotechar)) {
			throw InvalidInputException("to_csv only accepts 'quotechar' as a string");
		}
		options["quote"] = {Value(py::str(quotechar))};
	}

	if (!py::none().is(escapechar)) {
		if (!py::isinstance<py::str>(escapechar)) {
			throw InvalidInputException("to_csv only accepts 'escapechar' as a string");
		}
		options["escape"] = {Value(py::str(escapechar))};
	}

	if (!py::none().is(date_format)) {
		if (!py::isinstance<py::str>(date_format)) {
			throw InvalidInputException("to_csv only accepts 'date_format' as a string");
		}
		options["dateformat"] = {Value(py::str(date_format))};
	}

	if (!py::none().is(timestamp_format)) {
		if (!py::isinstance<py::str>(timestamp_format)) {
			throw InvalidInputException("to_csv only accepts 'timestamp_format' as a string");
		}
		options["timestampformat"] = {Value(py::str(timestamp_format))};
	}

	if (!py::none().is(quoting)) {
		// TODO: add list of strings as valid option
		if (py::isinstance<py::str>(quoting)) {
			string quoting_option = StringUtil::Lower(py::str(quoting));
			if (quoting_option != "force" && quoting_option != "all") {
				throw InvalidInputException(
				    "to_csv 'quoting' supported options are ALL or FORCE (both set FORCE_QUOTE=True)");
			}
		} else if (py::isinstance<py::int_>(quoting)) {
			int64_t quoting_value = py::int_(quoting);
			// csv.QUOTE_ALL expands to 1
			static constexpr int64_t QUOTE_ALL = 1;
			if (quoting_value != QUOTE_ALL) {
				throw InvalidInputException("Only csv.QUOTE_ALL is a supported option for 'quoting' currently");
			}
		} else {
			throw InvalidInputException(
			    "to_csv only accepts 'quoting' as a string or a constant from the 'csv' package");
		}
		options["force_quote"] = {Value("*")};
	}

	if (!py::none().is(encoding)) {
		if (!py::isinstance<py::str>(encoding)) {
			throw InvalidInputException("to_csv only accepts 'encoding' as a string");
		}
		string encoding_option = StringUtil::Lower(py::str(encoding));
		if (encoding_option != "utf-8" && encoding_option != "utf8") {
			throw InvalidInputException("The only supported encoding option is 'UTF8");
		}
	}

	if (!py::none().is(compression)) {
		if (!py::isinstance<py::str>(compression)) {
			throw InvalidInputException("to_csv only accepts 'compression' as a string");
		}
		options["compression"] = {Value(py::str(compression))};
	}

	auto write_csv = rel->WriteCSVRel(filename, std::move(options));
	PyExecuteRelation(write_csv);
}

// should this return a rel with the new view?
unique_ptr<DuckDBPyRelation> DuckDBPyRelation::CreateView(const string &view_name, bool replace) {
	rel->CreateView(view_name, replace);
	// We need to pass ownership of any Python Object Dependencies to the connection
	auto all_dependencies = rel->GetAllDependencies();
	rel->context.GetContext()->external_dependencies[view_name] = std::move(all_dependencies);
	return make_uniq<DuckDBPyRelation>(rel);
}

static bool IsDescribeStatement(SQLStatement &statement) {
	if (statement.type != StatementType::PRAGMA_STATEMENT) {
		return false;
	}
	auto &pragma_statement = statement.Cast<PragmaStatement>();
	if (pragma_statement.info->name != "show") {
		return false;
	}
	return true;
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Query(const string &view_name, const string &sql_query) {
	auto view_relation = CreateView(view_name);
	auto all_dependencies = rel->GetAllDependencies();
	rel->context.GetContext()->external_dependencies[view_name] = std::move(all_dependencies);

	Parser parser(rel->context.GetContext()->GetParserOptions());
	parser.ParseQuery(sql_query);
	if (parser.statements.size() != 1) {
		throw InvalidInputException("'DuckDBPyRelation.query' only accepts a single statement");
	}
	auto &statement = *parser.statements[0];
	if (statement.type == StatementType::SELECT_STATEMENT) {
		auto select_statement = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
		auto query_relation =
		    make_shared<QueryRelation>(rel->context.GetContext(), std::move(select_statement), "query_relation");
		return make_uniq<DuckDBPyRelation>(std::move(query_relation));
	} else if (IsDescribeStatement(statement)) {
		FunctionParameters parameters;
		parameters.values.emplace_back(view_name);
		auto query = PragmaShow(*rel->context.GetContext(), parameters);
		return Query(view_name, query);
	}
	{
		py::gil_scoped_release release;
		auto query_result = rel->context.GetContext()->Query(std::move(parser.statements[0]), false);
		// Execute it anyways, for creation/altering statements
		// We only care that it succeeds, we can't store the result
		D_ASSERT(query_result);
		if (query_result->HasError()) {
			query_result->ThrowError();
		}
	}
	return nullptr;
}

DuckDBPyRelation &DuckDBPyRelation::Execute() {
	AssertRelation();
	ExecuteOrThrow();
	return *this;
}

void DuckDBPyRelation::InsertInto(const string &table) {
	AssertRelation();
	auto parsed_info = QualifiedName::Parse(table);
	auto insert = rel->InsertRel(parsed_info.schema, parsed_info.name);
	PyExecuteRelation(insert);
}

static bool IsAcceptedInsertRelationType(const Relation &relation) {
	return relation.type == RelationType::TABLE_RELATION;
}

void DuckDBPyRelation::Insert(const py::object &params) {
	AssertRelation();
	if (!IsAcceptedInsertRelationType(*this->rel)) {
		throw InvalidInputException("'DuckDBPyRelation.insert' can only be used on a table relation");
	}
	vector<vector<Value>> values {DuckDBPyConnection::TransformPythonParamList(params)};

	py::gil_scoped_release release;
	rel->Insert(values);
}

void DuckDBPyRelation::Create(const string &table) {
	AssertRelation();
	auto parsed_info = QualifiedName::Parse(table);
	auto create = rel->CreateRel(parsed_info.schema, parsed_info.name);
	PyExecuteRelation(create);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Map(py::function fun, Optional<py::object> schema) {
	AssertRelation();
	vector<Value> params;
	params.emplace_back(Value::POINTER(CastPointerToValue(fun.ptr())));
	params.emplace_back(Value::POINTER(CastPointerToValue(schema.ptr())));
	auto relation = make_uniq<DuckDBPyRelation>(rel->TableFunction("python_map_function", params));
	auto rel_dependency = make_uniq<PythonDependencies>();
	rel_dependency->map_function = std::move(fun);
	rel_dependency->py_object_list.push_back(make_uniq<RegisteredObject>(std::move(schema)));
	relation->rel->extra_dependencies = std::move(rel_dependency);
	return relation;
}

string DuckDBPyRelation::ToString() {
	AssertRelation();
	if (rendered_result.empty()) {
		idx_t limit_rows = 10000;
		BoxRenderer renderer;
		auto limit = Limit(limit_rows, 0);
		auto res = limit->ExecuteInternal();

		auto context = rel->context.GetContext();
		BoxRendererConfig config;
		config.limit = limit_rows;
		rendered_result = res->ToBox(*context, config);
	}
	return rendered_result;
}

void DuckDBPyRelation::Print() {
	py::print(py::str(ToString()));
}

string DuckDBPyRelation::Explain(ExplainType type) {
	AssertRelation();
	py::gil_scoped_release release;
	auto res = rel->Explain(type);
	D_ASSERT(res->type == duckdb::QueryResultType::MATERIALIZED_RESULT);
	auto &materialized = res->Cast<MaterializedQueryResult>();
	auto &coll = materialized.Collection();
	string result;
	for (auto &row : coll.Rows()) {
		// Skip the first column because it just contains 'physical plan'
		for (idx_t col_idx = 1; col_idx < coll.ColumnCount(); col_idx++) {
			if (col_idx > 1) {
				result += "\t";
			}
			auto val = row.GetValue(col_idx);
			result += val.IsNull() ? "NULL" : StringUtil::Replace(val.ToString(), string("\0", 1), "\\0");
		}
		result += "\n";
	}
	return result;
}

// TODO: RelationType to a python enum
py::str DuckDBPyRelation::Type() {
	AssertRelation();
	return py::str(RelationTypeToString(rel->type));
}

py::list DuckDBPyRelation::Columns() {
	AssertRelation();
	py::list res;
	for (auto &col : rel->Columns()) {
		res.append(col.Name());
	}
	return res;
}

py::list DuckDBPyRelation::ColumnTypes() {
	AssertRelation();
	py::list res;
	for (auto &col : rel->Columns()) {
		res.append(DuckDBPyType(col.Type()));
	}
	return res;
}

bool DuckDBPyRelation::IsRelation(const py::object &object) {
	return py::isinstance<DuckDBPyRelation>(object);
}

} // namespace duckdb
