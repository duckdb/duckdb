#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb_python/vector_conversion.hpp"
#include "duckdb_python/pandas_type.hpp"
#include "duckdb/main/relation/query_relation.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/function/pragma/pragma_functions.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"

namespace duckdb {

static void InitializeReadOnlyProperties(py::class_<DuckDBPyRelation> &m) {
	m.def_property_readonly("type", &DuckDBPyRelation::Type, "Get the type of the relation.")
	    .def_property_readonly("columns", &DuckDBPyRelation::Columns,
	                           "Return a list containing the names of the columns of the relation.")
	    .def_property_readonly("types", &DuckDBPyRelation::ColumnTypes,
	                           "Return a list containing the types of the columns of the relation.")
	    .def_property_readonly("dtypes", &DuckDBPyRelation::ColumnTypes,
	                           "Return a list containing the types of the columns of the relation.")
	    .def_property_readonly("description", &DuckDBPyRelation::Description, "Return the description of the result")
	    .def_property_readonly("alias", &DuckDBPyRelation::GetAlias, "Get the name of the current alias")
	    .def("__len__", &DuckDBPyRelation::Length, "Number of rows in relation.")
	    .def_property_readonly("shape", &DuckDBPyRelation::Shape, " Tuple of # of rows, # of columns in relation.");
}

static void InitializeConsumers(py::class_<DuckDBPyRelation> &m) {
	m.def("execute", &DuckDBPyRelation::Execute, "Transform the relation into a result set")
	    .def("close", &DuckDBPyRelation::Close, "Closes the result")
	    .def("write_csv", &DuckDBPyRelation::WriteCsv, "Write the relation object to a CSV file in file_name",
	         py::arg("file_name"))
	    .def("fetchone", &DuckDBPyRelation::FetchOne, "Execute and fetch a single row as a tuple")
	    .def("fetchmany", &DuckDBPyRelation::FetchMany, "Execute and fetch the next set of rows as a list of tuples",
	         py::arg("size") = 1)
	    .def("fetchall", &DuckDBPyRelation::FetchAll, "Execute and fetch all rows as a list of tuples")
	    .def("fetchnumpy", &DuckDBPyRelation::FetchNumpy,
	         "Execute and fetch all rows as a Python dict mapping each column to one numpy arrays")
	    .def("df", &DuckDBPyRelation::FetchDF, "Execute and fetch all rows as a pandas DataFrame", py::kw_only(),
	         py::arg("date_as_object") = false)
	    .def("fetchdf", &DuckDBPyRelation::FetchDF, "Execute and fetch all rows as a pandas DataFrame", py::kw_only(),
	         py::arg("date_as_object") = false)
	    .def("to_df", &DuckDBPyRelation::FetchDF, "Execute and fetch all rows as a pandas DataFrame", py::kw_only(),
	         py::arg("date_as_object") = false)
	    .def("arrow", &DuckDBPyRelation::ToArrowTable, "Execute and fetch all rows as an Arrow Table",
	         py::arg("batch_size") = 1000000)
	    .def("fetch_arrow_table", &DuckDBPyRelation::ToArrowTable, "Execute and fetch all rows as an Arrow Table",
	         py::arg("batch_size") = 1000000)
	    .def("to_arrow_table", &DuckDBPyRelation::ToArrowTable, "Execute and fetch all rows as an Arrow Table",
	         py::arg("batch_size") = 1000000)
	    .def("record_batch", &DuckDBPyRelation::ToRecordBatch,
	         "Execute and return an Arrow Record Batch Reader that yields all rows", py::arg("batch_size") = 1000000)
	    .def("fetch_arrow_reader", &DuckDBPyRelation::ToRecordBatch,
	         "Execute and return an Arrow Record Batch Reader that yields all rows", py::arg("batch_size") = 1000000);
}

static void InitializeAggregates(py::class_<DuckDBPyRelation> &m) {
	m.def("sum", &DuckDBPyRelation::Sum,
	      "Compute the aggregate sum of a single column or a list of columns by the optional groups on the relation",
	      py::arg("sum_aggr"), py::arg("group_expr") = "")
	    .def("count", &DuckDBPyRelation::Count,
	         "Compute the aggregate count of a single column or a list of columns by the optional groups on the "
	         "relation",
	         py::arg("count_aggr"), py::arg("group_expr") = "")
	    .def("median", &DuckDBPyRelation::Median,
	         "Compute the aggregate median of a single column or a list of columns by the optional groups on the "
	         "relation",
	         py::arg("median_aggr"), py::arg("group_expr") = "")
	    .def("quantile", &DuckDBPyRelation::Quantile,
	         "Compute the quantile of a single column or a list of columns by the optional groups on the relation",
	         py::arg("q"), py::arg("quantile_aggr"), py::arg("group_expr") = "")
	    .def("min", &DuckDBPyRelation::Min,
	         "Compute the aggregate min of a single column or a list of columns by the optional groups on the relation",
	         py::arg("min_aggr"), py::arg("group_expr") = "")
	    .def("max", &DuckDBPyRelation::Max,
	         "Compute the aggregate max of a single column or a list of columns by the optional groups on the relation",
	         py::arg("max_aggr"), py::arg("group_expr") = "")
	    .def(
	        "mean", &DuckDBPyRelation::Mean,
	        "Compute the aggregate mean of a single column or a list of columns by the optional groups on the relation",
	        py::arg("mean_aggr"), py::arg("group_expr") = "")
	    .def("var", &DuckDBPyRelation::Var,
	         "Compute the variance of a single column or a list of columns by the optional groups on the relation",
	         py::arg("var_aggr"), py::arg("group_expr") = "")
	    .def("std", &DuckDBPyRelation::STD,
	         "Compute the standard deviation of a single column or a list of columns by the optional groups on the "
	         "relation",
	         py::arg("std_aggr"), py::arg("group_expr") = "")
	    .def("value_counts", &DuckDBPyRelation::ValueCounts, "Count number of rows with each unique value of variable",
	         py::arg("value_counts_aggr"), py::arg("group_expr") = "")
	    .def("mad", &DuckDBPyRelation::MAD,
	         "Returns the median absolute deviation for the aggregate columns. NULL values are ignored. Temporal "
	         "types return a positive INTERVAL.",
	         py::arg("aggregation_columns"), py::arg("group_columns") = "")
	    .def("mode", &DuckDBPyRelation::Mode,
	         "Returns the most frequent value for the aggregate columns. NULL values are ignored.",
	         py::arg("aggregation_columns"), py::arg("group_columns") = "")
	    .def("abs", &DuckDBPyRelation::Abs, "Returns the absolute value for the specified columns.",
	         py::arg("aggregation_columns"))
	    .def("prod", &DuckDBPyRelation::Prod, "Calculates the product of the aggregate column.",
	         py::arg("aggregation_columns"), py::arg("group_columns") = "")
	    .def("skew", &DuckDBPyRelation::Skew, "Returns the skewness of the aggregate column.",
	         py::arg("aggregation_columns"), py::arg("group_columns") = "")
	    .def("kurt", &DuckDBPyRelation::Kurt, "Returns the excess kurtosis of the aggregate column.",
	         py::arg("aggregation_columns"), py::arg("group_columns") = "")
	    .def("sem", &DuckDBPyRelation::SEM, "Returns the standard error of the mean of the aggregate column.",
	         py::arg("aggregation_columns"), py::arg("group_columns") = "")
	    .def("unique", &DuckDBPyRelation::Unique, "Number of distinct values in a column.", py::arg("unique_aggr"))
	    .def("cumsum", &DuckDBPyRelation::CumSum, "Returns the cumulative sum of the aggregate column.",
	         py::arg("aggregation_columns"))
	    .def("cumprod", &DuckDBPyRelation::CumProd, "Returns the cumulative product of the aggregate column.",
	         py::arg("aggregation_columns"))
	    .def("cummax", &DuckDBPyRelation::CumMax, "Returns the cumulative maximum of the aggregate column.",
	         py::arg("aggregation_columns"))
	    .def("cummin", &DuckDBPyRelation::CumMin, "Returns the cumulative minimum of the aggregate column.",
	         py::arg("aggregation_columns"));
}

static void InitializeSetOperators(py::class_<DuckDBPyRelation> &m) {
	m.def("union", &DuckDBPyRelation::Union, py::arg("union_rel"),
	      "Create the set union of this relation object with another relation object in other_rel")
	    .def("except_", &DuckDBPyRelation::Except,
	         "Create the set except of this relation object with another relation object in other_rel",
	         py::arg("other_rel"))
	    .def("intersect", &DuckDBPyRelation::Intersect,
	         "Create the set intersection of this relation object with another relation object in other_rel",
	         py::arg("other_rel"));
}

static void InitializeMetaQueries(py::class_<DuckDBPyRelation> &m) {
	m.def("describe", &DuckDBPyRelation::Describe,
	      "Gives basic statistics (e.g., min,max) and if null exists for each column of the relation.")
	    .def("explain", &DuckDBPyRelation::Explain);
}

void DuckDBPyRelation::Initialize(py::handle &m) {
	auto relation_module = py::class_<DuckDBPyRelation>(m, "DuckDBPyRelation", py::module_local());
	InitializeReadOnlyProperties(relation_module);
	InitializeAggregates(relation_module);
	InitializeSetOperators(relation_module);
	InitializeMetaQueries(relation_module);
	InitializeConsumers(relation_module);

	relation_module
	    .def("filter", &DuckDBPyRelation::Filter, "Filter the relation object by the filter in filter_expr",
	         py::arg("filter_expr"))
	    .def("project", &DuckDBPyRelation::Project, "Project the relation object by the projection in project_expr",
	         py::arg("project_expr"))
	    .def("set_alias", &DuckDBPyRelation::SetAlias, "Rename the relation object to new alias", py::arg("alias"))
	    .def("order", &DuckDBPyRelation::Order, "Reorder the relation object by order_expr", py::arg("order_expr"))
	    .def("aggregate", &DuckDBPyRelation::Aggregate,
	         "Compute the aggregate aggr_expr by the optional groups group_expr on the relation", py::arg("aggr_expr"),
	         py::arg("group_expr") = "")
	    .def("apply", &DuckDBPyRelation::GenericAggregator,
	         "Compute the function of a single column or a list of columns by the optional groups on the relation",
	         py::arg("function_name"), py::arg("function_aggr"), py::arg("group_expr") = "",
	         py::arg("function_parameter") = "", py::arg("projected_columns") = "")

	    .def("join", &DuckDBPyRelation::Join,
	         "Join the relation object with another relation object in other_rel using the join condition expression "
	         "in join_condition. Types supported are 'inner' and 'left'",
	         py::arg("other_rel"), py::arg("condition"), py::arg("how") = "inner")
	    .def("distinct", &DuckDBPyRelation::Distinct, "Retrieve distinct rows from this relation object")
	    .def("limit", &DuckDBPyRelation::Limit,
	         "Only retrieve the first n rows from this relation object, starting at offset", py::arg("n"),
	         py::arg("offset") = 0)
	    .def("insert", &DuckDBPyRelation::Insert, "Inserts the given values into the relation", py::arg("values"))

	    // This should be deprecated in favor of a replacement scan
	    .def("query", &DuckDBPyRelation::Query,
	         "Run the given SQL query in sql_query on the view named virtual_table_name that refers to the relation "
	         "object",
	         py::arg("virtual_table_name"), py::arg("sql_query"))

	    // Aren't these also technically consumers?
	    .def("insert_into", &DuckDBPyRelation::InsertInto,
	         "Inserts the relation object into an existing table named table_name", py::arg("table_name"))
	    .def("create", &DuckDBPyRelation::Create,
	         "Creates a new table named table_name with the contents of the relation object", py::arg("table_name"))
	    .def("create_view", &DuckDBPyRelation::CreateView,
	         "Creates a view named view_name that refers to the relation object", py::arg("view_name"),
	         py::arg("replace") = true)
	    .def("map", &DuckDBPyRelation::Map, py::arg("map_function"), "Calls the passed function on the relation")
	    .def("__str__", &DuckDBPyRelation::Print)
	    .def("__repr__", &DuckDBPyRelation::Print);
}

} // namespace duckdb
