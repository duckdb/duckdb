#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pytype.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"
#include "duckdb/main/relation/query_relation.hpp"
#include "duckdb/main/relation/join_relation.hpp"
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
#include "duckdb/common/arrow/physical_arrow_collector.hpp"

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

bool DuckDBPyRelation::CanBeRegisteredBy(Connection &con) {
	return CanBeRegisteredBy(con.context);
}

bool DuckDBPyRelation::CanBeRegisteredBy(ClientContext &context) {
	if (!rel) {
		// PyRelation without an internal relation can not be registered
		return false;
	}
	auto this_context = rel->context->TryGetContext();
	if (!this_context) {
		return false;
	}
	return &context == this_context.get();
}

bool DuckDBPyRelation::CanBeRegisteredBy(shared_ptr<ClientContext> &con) {
	if (!con) {
		return false;
	}
	return CanBeRegisteredBy(*con);
}

DuckDBPyRelation::~DuckDBPyRelation() {
	D_ASSERT(py::gil_check());
	py::gil_scoped_release gil;
	rel.reset();
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
	for (auto &dep : this->rel->external_dependencies) {
		projected_relation->rel->AddExternalDependency(dep);
	}
	return projected_relation;
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Project(const py::args &args, const string &groups) {
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
		if (groups.empty()) {
			// No groups provided
			return make_uniq<DuckDBPyRelation>(rel->Project(std::move(expressions), empty_aliases));
		}
		return make_uniq<DuckDBPyRelation>(rel->Aggregate(std::move(expressions), groups));
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
			type = TransformStringToLogicalType(type_str, *rel->context->GetContext());
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

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::EmptyResult(const shared_ptr<ClientContext> &context,
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
	    make_uniq<DuckDBPyRelation>(make_shared_ptr<ValueRelation>(context, single_row, std::move(names)));
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

vector<unique_ptr<ParsedExpression>> GetExpressions(ClientContext &context, const py::object &expr) {
	if (py::is_list_like(expr)) {
		vector<unique_ptr<ParsedExpression>> expressions;
		auto aggregate_list = py::list(expr);
		for (auto &item : aggregate_list) {
			shared_ptr<DuckDBPyExpression> py_expr;
			if (!py::try_cast<shared_ptr<DuckDBPyExpression>>(item, py_expr)) {
				throw InvalidInputException("Please provide arguments of type Expression!");
			}
			auto expr = py_expr->GetExpression().Copy();
			expressions.push_back(std::move(expr));
		}
		return expressions;
	} else if (py::isinstance<py::str>(expr)) {
		auto aggregate_list = std::string(py::str(expr));
		return Parser::ParseExpressionList(aggregate_list, context.GetParserOptions());
	} else {
		string actual_type = py::str(expr.get_type());
		throw InvalidInputException("Please provide either a string or list of Expression objects, not %s",
		                            actual_type);
	}
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Aggregate(const py::object &expr, const string &groups) {
	AssertRelation();
	auto expressions = GetExpressions(*rel->context->GetContext(), expr);
	if (!groups.empty()) {
		return make_uniq<DuckDBPyRelation>(rel->Aggregate(std::move(expressions), groups));
	}
	return make_uniq<DuckDBPyRelation>(rel->Aggregate(std::move(expressions)));
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
	} catch (const std::exception &) {
		return "";
	}
}

string DuckDBPyRelation::GenerateExpressionList(const string &function_name, const string &aggregated_columns,
                                                const string &groups, const string &function_parameter,
                                                bool ignore_nulls, const string &projected_columns,
                                                const string &window_spec) {
	auto input = StringUtil::Split(aggregated_columns, ',');
	return GenerateExpressionList(function_name, std::move(input), groups, function_parameter, ignore_nulls,
	                              projected_columns, window_spec);
}

string DuckDBPyRelation::GenerateExpressionList(const string &function_name, vector<string> input, const string &groups,
                                                const string &function_parameter, bool ignore_nulls,
                                                const string &projected_columns, const string &window_spec) {
	string expr;

	if (StringUtil::CIEquals("count", function_name) && input.empty()) {
		// Insert an artificial '*'
		input.push_back("*");
	}

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
	return Aggregate(py::str(expr), groups);
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
                                                             bool ignore_nulls) {
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

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::ValueCounts(const std::string &column, const std::string &groups) {
	return Count(column, groups, "", column);
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
	auto context = rel->context->GetContext();
	D_ASSERT(py::gil_check());
	py::gil_scoped_release release;
	auto pending_query = context->PendingQuery(rel, stream_result);
	return DuckDBPyConnection::CompletePendingQuery(*pending_query);
}

unique_ptr<QueryResult> DuckDBPyRelation::ExecuteInternal(bool stream_result) {
	this->executed = true;
	return PyExecuteRelation(rel, stream_result);
}

void DuckDBPyRelation::ExecuteOrThrow(bool stream_result) {
	py::gil_scoped_acquire gil;
	result.reset();
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

duckdb::pyarrow::Table DuckDBPyRelation::ToArrowTableInternal(idx_t batch_size, bool to_polars) {
	if (!result) {
		if (!rel) {
			return py::none();
		}
		auto &config = ClientConfig::GetConfig(*rel->context->GetContext());
		ScopedConfigSetting scoped_setting(
		    config,
		    [&batch_size](ClientConfig &config) {
			    config.result_collector = [&batch_size](ClientContext &context, PreparedStatementData &data) {
				    return PhysicalArrowCollector::Create(context, data, batch_size);
			    };
		    },
		    [](ClientConfig &config) { config.result_collector = nullptr; });
		ExecuteOrThrow();
	}
	AssertResultOpen();
	auto res = result->FetchArrowTable(batch_size, to_polars);
	result = nullptr;
	return res;
}

duckdb::pyarrow::Table DuckDBPyRelation::ToArrowTable(idx_t batch_size) {
	return ToArrowTableInternal(batch_size, false);
}

py::object DuckDBPyRelation::ToArrowCapsule(const py::object &requested_schema) {
	if (!result) {
		if (!rel) {
			return py::none();
		}
		ExecuteOrThrow();
	}
	AssertResultOpen();
	return result->FetchArrowCapsule();
}

PolarsDataFrame DuckDBPyRelation::ToPolars(idx_t batch_size) {
	auto arrow = ToArrowTableInternal(batch_size, true);
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
	return std::find_if(names.begin(), names.end(),
	                    [&](const string &item) { return StringUtil::CIEquals(name, item); }) != names.end();
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
	vector<string> column_names;
	if (names.size() == 1 && ContainsStructFieldByName(types[0], name)) {
		// e.g 'rel['my_struct']['my_field']:
		// first 'my_struct' is selected by the bottom condition
		// then 'my_field' is accessed on the result of this
		column_names.push_back(names[0]);
		column_names.push_back(name);
	} else if (ContainsColumnByName(name)) {
		column_names.push_back(name);
	}

	if (column_names.empty()) {
		throw py::attribute_error(
		    StringUtil::Format("This relation does not contain a column by the name of '%s'", name));
	}

	vector<unique_ptr<ParsedExpression>> expressions;
	expressions.push_back(std::move(make_uniq<ColumnRefExpression>(column_names)));
	vector<string> aliases;
	aliases.push_back(name);
	return make_uniq<DuckDBPyRelation>(rel->Project(std::move(expressions), aliases));
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

namespace {
struct SupportedPythonJoinType {
	string name;
	JoinType type;
};
} // namespace

static const SupportedPythonJoinType *GetSupportedJoinTypes(idx_t &length) {
	static const SupportedPythonJoinType SUPPORTED_TYPES[] = {{"left", JoinType::LEFT},   {"right", JoinType::RIGHT},
	                                                          {"outer", JoinType::OUTER}, {"semi", JoinType::SEMI},
	                                                          {"inner", JoinType::INNER}, {"anti", JoinType::ANTI}};
	static const auto SUPPORTED_TYPES_COUNT = sizeof(SUPPORTED_TYPES) / sizeof(SupportedPythonJoinType);
	length = SUPPORTED_TYPES_COUNT;
	return reinterpret_cast<const SupportedPythonJoinType *>(SUPPORTED_TYPES);
}

static JoinType ParseJoinType(const string &type) {
	idx_t supported_types_count;
	auto supported_types = GetSupportedJoinTypes(supported_types_count);
	for (idx_t i = 0; i < supported_types_count; i++) {
		auto &supported_type = supported_types[i];
		if (supported_type.name == type) {
			return supported_type.type;
		}
	}
	return JoinType::INVALID;
}

[[noreturn]] void ThrowUnsupportedJoinTypeError(const string &provided) {
	vector<string> supported_options;
	idx_t length;
	auto supported_types = GetSupportedJoinTypes(length);
	for (idx_t i = 0; i < length; i++) {
		supported_options.push_back(StringUtil::Format("'%s'", supported_types[i].name));
	}
	auto options = StringUtil::Join(supported_options, ", ");
	throw InvalidInputException("Unsupported join type %s, try one of: %s", provided, options);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Join(DuckDBPyRelation *other, const py::object &condition,
                                                    const string &type) {

	JoinType join_type;
	string type_string = StringUtil::Lower(type);
	StringUtil::Trim(type_string);

	join_type = ParseJoinType(type_string);
	if (join_type == JoinType::INVALID) {
		ThrowUnsupportedJoinTypeError(type);
	}
	auto alias = GetAlias();
	auto other_alias = other->GetAlias();
	if (StringUtil::CIEquals(alias, other_alias)) {
		throw InvalidInputException("Both relations have the same alias, please change the alias of one or both "
		                            "relations using 'rel = rel.set_alias(<new alias>)'");
	}
	if (py::isinstance<py::str>(condition)) {
		auto condition_string = std::string(py::cast<py::str>(condition));
		return make_uniq<DuckDBPyRelation>(rel->Join(other->rel, condition_string, join_type));
	}
	vector<string> using_list;
	if (py::is_list_like(condition)) {
		auto using_list_p = py::list(condition);
		for (auto &item : using_list_p) {
			if (!py::isinstance<py::str>(item)) {
				string actual_type = py::str(item.get_type());
				throw InvalidInputException("Using clause should be a list of strings, not %s", actual_type);
			}
			using_list.push_back(std::string(py::str(item)));
		}
		if (using_list.empty()) {
			throw InvalidInputException("Please provide at least one string in the condition to create a USING clause");
		}
		auto join_relation = make_shared_ptr<JoinRelation>(rel, other->rel, std::move(using_list), join_type);
		return make_uniq<DuckDBPyRelation>(std::move(join_relation));
	}
	shared_ptr<DuckDBPyExpression> condition_expr;
	if (!py::try_cast(condition, condition_expr)) {
		throw InvalidInputException(
		    "Please provide condition as an expression either in string form or as an Expression object");
	}
	vector<unique_ptr<ParsedExpression>> conditions;
	conditions.push_back(condition_expr->GetExpression().Copy());
	return make_uniq<DuckDBPyRelation>(rel->Join(other->rel, std::move(conditions), join_type));
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Cross(DuckDBPyRelation *other) {
	return make_uniq<DuckDBPyRelation>(rel->CrossProduct(other->rel));
}

static Value NestedDictToStruct(const py::object &dictionary) {
	if (!py::isinstance<py::dict>(dictionary)) {
		throw InvalidInputException("NestedDictToStruct only accepts a dictionary as input");
	}
	py::dict dict_casted = py::dict(dictionary);

	child_list_t<Value> children;
	for (auto item : dict_casted) {
		py::object item_key = item.first.cast<py::object>();
		py::object item_value = item.second.cast<py::object>();

		if (!py::isinstance<py::str>(item_key)) {
			throw InvalidInputException("NestedDictToStruct only accepts a dictionary with string keys");
		}

		if (py::isinstance<py::int_>(item_value)) {
			int32_t item_value_int = py::int_(item_value);
			children.push_back(std::make_pair(py::str(item_key), Value(item_value_int)));
		} else if (py::isinstance<py::dict>(item_value)) {
			children.push_back(std::make_pair(py::str(item_key), NestedDictToStruct(item_value)));
		} else {
			throw InvalidInputException(
			    "NestedDictToStruct only accepts a dictionary with integer values or nested dictionaries");
		}
	}
	return Value::STRUCT(std::move(children));
}

void DuckDBPyRelation::ToParquet(const string &filename, const py::object &compression, const py::object &field_ids,
                                 const py::object &row_group_size_bytes, const py::object &row_group_size,
                                 const py::object &overwrite, const py::object &per_thread_output,
                                 const py::object &use_tmp_file, const py::object &partition_by,
                                 const py::object &write_partition_columns, const py::object &append) {
	case_insensitive_map_t<vector<Value>> options;

	if (!py::none().is(compression)) {
		if (!py::isinstance<py::str>(compression)) {
			throw InvalidInputException("to_parquet only accepts 'compression' as a string");
		}
		options["compression"] = {Value(py::str(compression))};
	}

	if (!py::none().is(field_ids)) {
		if (py::isinstance<py::dict>(field_ids)) {
			Value field_ids_value = NestedDictToStruct(field_ids);
			options["field_ids"] = {field_ids_value};
		} else if (py::isinstance<py::str>(field_ids)) {
			options["field_ids"] = {Value(py::str(field_ids))};
		} else {
			throw InvalidInputException("to_parquet only accepts 'field_ids' as a dictionary or 'auto'");
		}
	}

	if (!py::none().is(row_group_size_bytes)) {
		if (py::isinstance<py::int_>(row_group_size_bytes)) {
			int64_t row_group_size_bytes_int = py::int_(row_group_size_bytes);
			options["row_group_size_bytes"] = {Value(row_group_size_bytes_int)};
		} else if (py::isinstance<py::str>(row_group_size_bytes)) {
			options["row_group_size_bytes"] = {Value(py::str(row_group_size_bytes))};
		} else {
			throw InvalidInputException(
			    "to_parquet only accepts 'row_group_size_bytes' as an integer or 'auto' string");
		}
	}

	if (!py::none().is(row_group_size)) {
		if (!py::isinstance<py::int_>(row_group_size)) {
			throw InvalidInputException("to_parquet only accepts 'row_group_size' as an integer");
		}
		int64_t row_group_size_int = py::int_(row_group_size);
		options["row_group_size"] = {Value(row_group_size_int)};
	}

	if (!py::none().is(partition_by)) {
		if (!py::isinstance<py::list>(partition_by)) {
			throw InvalidInputException("to_parquet only accepts 'partition_by' as a list of strings");
		}
		vector<Value> partition_by_values;
		const py::list &partition_fields = partition_by;
		for (auto &field : partition_fields) {
			if (!py::isinstance<py::str>(field)) {
				throw InvalidInputException("to_parquet only accepts 'partition_by' as a list of strings");
			}
			partition_by_values.emplace_back(Value(py::str(field)));
		}
		options["partition_by"] = {partition_by_values};
	}

	if (!py::none().is(write_partition_columns)) {
		if (!py::isinstance<py::bool_>(write_partition_columns)) {
			throw InvalidInputException("to_parquet only accepts 'write_partition_columns' as a boolean");
		}
		options["write_partition_columns"] = {Value::BOOLEAN(py::bool_(write_partition_columns))};
	}

	if (!py::none().is(append)) {
		if (!py::isinstance<py::bool_>(append)) {
			throw InvalidInputException("to_parquet only accepts 'append' as a boolean");
		}
		options["append"] = {Value::BOOLEAN(py::bool_(append))};
	}

	if (!py::none().is(overwrite)) {
		if (!py::isinstance<py::bool_>(overwrite)) {
			throw InvalidInputException("to_parquet only accepts 'overwrite' as a boolean");
		}
		options["overwrite_or_ignore"] = {Value::BOOLEAN(py::bool_(overwrite))};
	}

	if (!py::none().is(per_thread_output)) {
		if (!py::isinstance<py::bool_>(per_thread_output)) {
			throw InvalidInputException("to_parquet only accepts 'per_thread_output' as a boolean");
		}
		options["per_thread_output"] = {Value::BOOLEAN(py::bool_(per_thread_output))};
	}

	if (!py::none().is(use_tmp_file)) {
		if (!py::isinstance<py::bool_>(use_tmp_file)) {
			throw InvalidInputException("to_parquet only accepts 'use_tmp_file' as a boolean");
		}
		options["use_tmp_file"] = {Value::BOOLEAN(py::bool_(use_tmp_file))};
	}

	auto write_parquet = rel->WriteParquetRel(filename, std::move(options));
	PyExecuteRelation(write_parquet);
}

void DuckDBPyRelation::ToCSV(const string &filename, const py::object &sep, const py::object &na_rep,
                             const py::object &header, const py::object &quotechar, const py::object &escapechar,
                             const py::object &date_format, const py::object &timestamp_format,
                             const py::object &quoting, const py::object &encoding, const py::object &compression,
                             const py::object &overwrite, const py::object &per_thread_output,
                             const py::object &use_tmp_file, const py::object &partition_by,
                             const py::object &write_partition_columns) {
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

	if (!py::none().is(overwrite)) {
		if (!py::isinstance<py::bool_>(overwrite)) {
			throw InvalidInputException("to_csv only accepts 'overwrite' as a boolean");
		}
		options["overwrite_or_ignore"] = {Value::BOOLEAN(py::bool_(overwrite))};
	}

	if (!py::none().is(per_thread_output)) {
		if (!py::isinstance<py::bool_>(per_thread_output)) {
			throw InvalidInputException("to_csv only accepts 'per_thread_output' as a boolean");
		}
		options["per_thread_output"] = {Value::BOOLEAN(py::bool_(per_thread_output))};
	}

	if (!py::none().is(use_tmp_file)) {
		if (!py::isinstance<py::bool_>(use_tmp_file)) {
			throw InvalidInputException("to_csv only accepts 'use_tmp_file' as a boolean");
		}
		options["use_tmp_file"] = {Value::BOOLEAN(py::bool_(use_tmp_file))};
	}

	if (!py::none().is(partition_by)) {
		if (!py::isinstance<py::list>(partition_by)) {
			throw InvalidInputException("to_csv only accepts 'partition_by' as a list of strings");
		}
		vector<Value> partition_by_values;
		const py::list &partition_fields = partition_by;
		for (auto &field : partition_fields) {
			if (!py::isinstance<py::str>(field)) {
				throw InvalidInputException("to_csv only accepts 'partition_by' as a list of strings");
			}
			partition_by_values.emplace_back(Value(py::str(field)));
		}
		options["partition_by"] = {partition_by_values};
	}

	if (!py::none().is(write_partition_columns)) {
		if (!py::isinstance<py::bool_>(write_partition_columns)) {
			throw InvalidInputException("to_csv only accepts 'write_partition_columns' as a boolean");
		}
		options["write_partition_columns"] = {Value::BOOLEAN(py::bool_(write_partition_columns))};
	}

	auto write_csv = rel->WriteCSVRel(filename, std::move(options));
	PyExecuteRelation(write_csv);
}

// should this return a rel with the new view?
unique_ptr<DuckDBPyRelation> DuckDBPyRelation::CreateView(const string &view_name, bool replace) {
	rel->CreateView(view_name, replace);
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

	Parser parser(rel->context->GetContext()->GetParserOptions());
	parser.ParseQuery(sql_query);
	if (parser.statements.size() != 1) {
		throw InvalidInputException("'DuckDBPyRelation.query' only accepts a single statement");
	}
	auto &statement = *parser.statements[0];
	if (statement.type == StatementType::SELECT_STATEMENT) {
		auto select_statement = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
		auto query_relation = make_shared_ptr<QueryRelation>(rel->context->GetContext(), std::move(select_statement),
		                                                     sql_query, "query_relation");
		return make_uniq<DuckDBPyRelation>(std::move(query_relation));
	} else if (IsDescribeStatement(statement)) {
		auto query = PragmaShow(view_name);
		return Query(view_name, query);
	}
	{
		D_ASSERT(py::gil_check());
		py::gil_scoped_release release;
		auto query_result = rel->context->GetContext()->Query(std::move(parser.statements[0]), false);
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

void DuckDBPyRelation::Update(const py::object &set_p, const py::object &where) {
	AssertRelation();
	unique_ptr<ParsedExpression> condition;
	if (!py::none().is(where)) {
		shared_ptr<DuckDBPyExpression> py_expr;
		if (!py::try_cast<shared_ptr<DuckDBPyExpression>>(where, py_expr)) {
			throw InvalidInputException("Please provide an Expression to 'condition'");
		}
		condition = py_expr->GetExpression().Copy();
	}

	if (!py::is_dict_like(set_p)) {
		throw InvalidInputException("Please provide 'set' as a dictionary of column name to Expression");
	}

	vector<string> names;
	vector<unique_ptr<ParsedExpression>> expressions;

	py::dict set = py::dict(set_p);
	auto arg_count = set.size();
	if (arg_count == 0) {
		throw InvalidInputException("Please provide at least one set expression");
	}

	for (auto item : set) {
		py::object item_key = item.first.cast<py::object>();
		py::object item_value = item.second.cast<py::object>();

		if (!py::isinstance<py::str>(item_key)) {
			throw InvalidInputException("Please provide the column name as the key of the dictionary");
		}
		shared_ptr<DuckDBPyExpression> py_expr;
		if (!py::try_cast<shared_ptr<DuckDBPyExpression>>(item_value, py_expr)) {
			string actual_type = py::str(item_value.get_type());
			throw InvalidInputException("Please provide an object of type Expression as the value, not %s",
			                            actual_type);
		}
		names.push_back(std::string(py::str(item_key)));
		expressions.push_back(py_expr->GetExpression().Copy());
	}

	return rel->Update(std::move(names), std::move(expressions), std::move(condition));
}

void DuckDBPyRelation::Insert(const py::object &params) {
	AssertRelation();
	if (!IsAcceptedInsertRelationType(*this->rel)) {
		throw InvalidInputException("'DuckDBPyRelation.insert' can only be used on a table relation");
	}
	vector<vector<Value>> values {DuckDBPyConnection::TransformPythonParamList(params)};

	D_ASSERT(py::gil_check());
	py::gil_scoped_release release;
	rel->Insert(values);
}

void DuckDBPyRelation::Create(const string &table) {
	AssertRelation();
	auto parsed_info = QualifiedName::Parse(table);
	auto create = rel->CreateRel(parsed_info.schema, parsed_info.name, false);
	PyExecuteRelation(create);
}

unique_ptr<DuckDBPyRelation> DuckDBPyRelation::Map(py::function fun, Optional<py::object> schema) {
	AssertRelation();
	vector<Value> params;
	params.emplace_back(Value::POINTER(CastPointerToValue(fun.ptr())));
	params.emplace_back(Value::POINTER(CastPointerToValue(schema.ptr())));
	auto relation = make_uniq<DuckDBPyRelation>(rel->TableFunction("python_map_function", params));
	auto rel_dependency = make_uniq<ExternalDependency>();
	rel_dependency->AddDependency("map", PythonDependencyItem::Create(std::move(fun)));
	rel_dependency->AddDependency("schema", PythonDependencyItem::Create(std::move(schema)));
	relation->rel->AddExternalDependency(std::move(rel_dependency));
	return relation;
}

string DuckDBPyRelation::ToStringInternal(const BoxRendererConfig &config, bool invalidate_cache) {
	AssertRelation();
	if (rendered_result.empty() || invalidate_cache) {
		BoxRenderer renderer;
		auto limit = Limit(config.limit, 0);
		auto res = limit->ExecuteInternal();

		auto context = rel->context->GetContext();
		rendered_result = res->ToBox(*context, config);
	}
	return rendered_result;
}

string DuckDBPyRelation::ToString() {
	BoxRendererConfig config;
	config.limit = 10000;
	if (DuckDBPyConnection::IsJupyter()) {
		config.max_width = 10000;
	}
	return ToStringInternal(config);
}

static idx_t IndexFromPyInt(const py::object &object) {
	auto index = py::cast<idx_t>(object);
	return index;
}

void DuckDBPyRelation::Print(const Optional<py::int_> &max_width, const Optional<py::int_> &max_rows,
                             const Optional<py::int_> &max_col_width, const Optional<py::str> &null_value,
                             const py::object &render_mode) {
	BoxRendererConfig config;
	config.limit = 10000;
	if (DuckDBPyConnection::IsJupyter()) {
		config.max_width = 10000;
	}

	bool invalidate_cache = false;
	if (!py::none().is(max_width)) {
		invalidate_cache = true;
		config.max_width = IndexFromPyInt(max_width);
	}
	if (!py::none().is(max_rows)) {
		invalidate_cache = true;
		config.max_rows = IndexFromPyInt(max_rows);
	}
	if (!py::none().is(max_col_width)) {
		invalidate_cache = true;
		config.max_col_width = IndexFromPyInt(max_col_width);
	}
	if (!py::none().is(null_value)) {
		invalidate_cache = true;
		config.null_value = py::cast<std::string>(null_value);
	}
	if (!py::none().is(render_mode)) {
		invalidate_cache = true;
		if (!py::try_cast(render_mode, config.render_mode)) {
			throw InvalidInputException("'render_mode' accepts either a string, RenderMode or int value");
		}
	}

	py::print(py::str(ToStringInternal(config, invalidate_cache)));
}

static ExplainFormat GetExplainFormat(ExplainType type) {
	if (DuckDBPyConnection::IsJupyter() && type != ExplainType::EXPLAIN_ANALYZE) {
		return ExplainFormat::HTML;
	} else {
		return ExplainFormat::DEFAULT;
	}
}

static void DisplayHTML(const string &html) {
	py::gil_scoped_acquire gil;
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	auto html_attr = import_cache.IPython.display.HTML();
	auto html_object = html_attr(py::str(html));
	auto display_attr = import_cache.IPython.display.display();
	display_attr(html_object);
}

string DuckDBPyRelation::Explain(ExplainType type) {
	AssertRelation();
	D_ASSERT(py::gil_check());
	py::gil_scoped_release release;

	auto explain_format = GetExplainFormat(type);
	auto res = rel->Explain(type, explain_format);
	D_ASSERT(res->type == duckdb::QueryResultType::MATERIALIZED_RESULT);
	auto &materialized = res->Cast<MaterializedQueryResult>();
	auto &coll = materialized.Collection();
	if (explain_format != ExplainFormat::HTML || !DuckDBPyConnection::IsJupyter()) {
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

	auto chunk = materialized.Fetch();
	for (idx_t i = 0; i < chunk->size(); i++) {
		auto plan = chunk->GetValue(1, i);
		auto plan_string = plan.GetValue<string>();
		DisplayHTML(plan_string);
	}

	const string tree_resize_script = R"(
<script>
function toggleDisplay(button) {
    const parentLi = button.closest('li');
    const nestedUl = parentLi.querySelector('ul');
    if (nestedUl) {
        const currentDisplay = getComputedStyle(nestedUl).getPropertyValue('display');
        if (currentDisplay === 'none') {
            nestedUl.classList.toggle('hidden');
            button.textContent = '-';
        } else {
            nestedUl.classList.toggle('hidden');
            button.textContent = '+';
        }
    }
}

function updateTreeHeight(tfTree) {
	if (!tfTree) {
		return;
	}

	const closestElement = tfTree.closest('.lm-Widget.jp-OutputArea.jp-Cell-outputArea');
	if (!closestElement) {
		return;
	}

	console.log(closestElement);

	const height = getComputedStyle(closestElement).getPropertyValue('height');
	tfTree.style.height = height;
}

function resizeTFTree() {
	const tfTrees = document.querySelectorAll('.tf-tree');
	tfTrees.forEach(tfTree => {
		console.log(tfTree);
		if (tfTree) {
			const jupyterViewPort = tfTree.closest('.lm-Widget.jp-OutputArea.jp-Cell-outputArea');
			console.log(jupyterViewPort);
			if (jupyterViewPort) {
				const resizeObserver = new ResizeObserver(() => {
					updateTreeHeight(tfTree);
				});
				resizeObserver.observe(jupyterViewPort);
			}
		}
	});
}

resizeTFTree();

</script>
	)";
	DisplayHTML(tree_resize_script);
	return "";
}

// TODO: RelationType to a python enum
py::str DuckDBPyRelation::Type() {
	if (!rel) {
		return py::str("QUERY_RESULT");
	}
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
