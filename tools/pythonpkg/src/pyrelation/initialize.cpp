#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"
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
	    .def("close", &DuckDBPyRelation::Close, "Closes the result");

	DefineMethod({"to_parquet", "write_parquet"}, m, &DuckDBPyRelation::ToParquet,
	             "Write the relation object to a Parquet file in 'file_name'", py::arg("file_name"), py::kw_only(),
	             py::arg("compression") = py::none(), py::arg("field_ids") = py::none(),
	             py::arg("row_group_size_bytes") = py::none(), py::arg("row_group_size") = py::none());

	DefineMethod({"to_csv", "write_csv"}, m, &DuckDBPyRelation::ToCSV,
	             "Write the relation object to a CSV file in 'file_name'", py::arg("file_name"), py::kw_only(),
	             py::arg("sep") = py::none(), py::arg("na_rep") = py::none(), py::arg("header") = py::none(),
	             py::arg("quotechar") = py::none(), py::arg("escapechar") = py::none(),
	             py::arg("date_format") = py::none(), py::arg("timestamp_format") = py::none(),
	             py::arg("quoting") = py::none(), py::arg("encoding") = py::none(), py::arg("compression") = py::none(),
	             py::arg("overwrite") = py::none(), py::arg("per_thread_output") = py::none(),
	             py::arg("use_tmp_file") = py::none(), py::arg("partition_by") = py::none());

	m.def("fetchone", &DuckDBPyRelation::FetchOne, "Execute and fetch a single row as a tuple")
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
	    .def("fetch_df_chunk", &DuckDBPyRelation::FetchDFChunk, "Execute and fetch a chunk of the rows",
	         py::arg("vectors_per_chunk") = 1, py::kw_only(), py::arg("date_as_object") = false)
	    .def("arrow", &DuckDBPyRelation::ToArrowTable, "Execute and fetch all rows as an Arrow Table",
	         py::arg("batch_size") = 1000000)
	    .def("fetch_arrow_table", &DuckDBPyRelation::ToArrowTable, "Execute and fetch all rows as an Arrow Table",
	         py::arg("batch_size") = 1000000)
	    .def("to_arrow_table", &DuckDBPyRelation::ToArrowTable, "Execute and fetch all rows as an Arrow Table",
	         py::arg("batch_size") = 1000000)
	    .def("pl", &DuckDBPyRelation::ToPolars, "Execute and fetch all rows as a Polars DataFrame",
	         py::arg("batch_size") = 1000000)
	    .def("torch", &DuckDBPyRelation::FetchPyTorch, "Fetch a result as dict of PyTorch Tensors")
	    .def("tf", &DuckDBPyRelation::FetchTF, "Fetch a result as dict of TensorFlow Tensors")
	    .def("record_batch", &DuckDBPyRelation::ToRecordBatch,
	         "Execute and return an Arrow Record Batch Reader that yields all rows", py::arg("batch_size") = 1000000)
	    .def("fetch_arrow_reader", &DuckDBPyRelation::ToRecordBatch,
	         "Execute and return an Arrow Record Batch Reader that yields all rows", py::arg("batch_size") = 1000000);
}

static void InitializeAggregates(py::class_<DuckDBPyRelation> &m) {
	/* General aggregate functions */
	m.def("any_value", &DuckDBPyRelation::AnyValue, "Returns the first non-null value from a given column",
	      py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("arg_max", &DuckDBPyRelation::ArgMax,
	         "Finds the row with the maximum value for a value column and returns the value of that row for an "
	         "argument column",
	         py::arg("arg_column"), py::arg("value_column"), py::arg("groups") = "", py::arg("window_spec") = "",
	         py::arg("projected_columns") = "")
	    .def("arg_min", &DuckDBPyRelation::ArgMin,
	         "Finds the row with the minimum value for a value column and returns the value of that row for an "
	         "argument column",
	         py::arg("arg_column"), py::arg("value_column"), py::arg("groups") = "", py::arg("window_spec") = "",
	         py::arg("projected_columns") = "");
	DefineMethod({"avg", "mean"}, m, &DuckDBPyRelation::Avg, "Computes the average on a given column",
	             py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "",
	             py::arg("projected_columns") = "");
	m.def("bit_and", &DuckDBPyRelation::BitAnd, "Computes the bitwise AND of all bits present in a given column",
	      py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("bit_or", &DuckDBPyRelation::BitOr, "Computes the bitwise OR of all bits present in a given column",
	         py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("bit_xor", &DuckDBPyRelation::BitXor, "Computes the bitwise XOR of all bits present in a given column",
	         py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("bitstring_agg", &DuckDBPyRelation::BitStringAgg,
	         "Computes a bitstring with bits set for each distinct value in a given column", py::arg("column"),
	         py::arg("min") = py::none(), py::arg("max") = py::none(), py::arg("groups") = "",
	         py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("bool_and", &DuckDBPyRelation::BoolAnd, "Computes the logical AND of all values present in a given column",
	         py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("bool_or", &DuckDBPyRelation::BoolOr, "Computes the logical OR of all values present in a given column",
	         py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("count", &DuckDBPyRelation::Count, "Computes the number of elements present in a given column",
	         py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("value_counts", &DuckDBPyRelation::ValueCounts,
	         "Computes the number of elements present in a given column, also projecting the original column",
	         py::arg("column"), py::arg("groups") = "")
	    .def("favg", &DuckDBPyRelation::FAvg,
	         "Computes the average of all values present in a given column using a more accurate floating point "
	         "summation (Kahan Sum)",
	         py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("first", &DuckDBPyRelation::First, "Returns the first value of a given column", py::arg("column"),
	         py::arg("groups") = "", py::arg("projected_columns") = "")
	    .def("fsum", &DuckDBPyRelation::FSum,
	         "Computes the sum of all values present in a given column using a more accurate floating point "
	         "summation (Kahan Sum)",
	         py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("geomean", &DuckDBPyRelation::GeoMean,
	         "Computes the geometric mean over all values present in a given column", py::arg("column"),
	         py::arg("groups") = "", py::arg("projected_columns") = "")
	    .def("histogram", &DuckDBPyRelation::Histogram,
	         "Computes the histogram over all values present in a given column", py::arg("column"),
	         py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("list", &DuckDBPyRelation::List, "Returns a list containing all values present in a given column",
	         py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("last", &DuckDBPyRelation::Last, "Returns the last value of a given column", py::arg("column"),
	         py::arg("groups") = "", py::arg("projected_columns") = "")
	    .def("max", &DuckDBPyRelation::Max, "Returns the maximum value present in a given column", py::arg("column"),
	         py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("min", &DuckDBPyRelation::Min, "Returns the minimum value present in a given column", py::arg("column"),
	         py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("product", &DuckDBPyRelation::Product, "Returns the product of all values present in a given column",
	         py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("string_agg", &DuckDBPyRelation::StringAgg,
	         "Concatenates the values present in a given column with a separator", py::arg("column"),
	         py::arg("sep") = ",", py::arg("groups") = "", py::arg("window_spec") = "",
	         py::arg("projected_columns") = "")
	    .def("sum", &DuckDBPyRelation::Sum, "Computes the sum of all values present in a given column",
	         py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("unique", &DuckDBPyRelation::Unique, "Number of distinct values in a column.", py::arg("unique_aggr"));
	/* TODO: Approximate aggregate functions */
	/* TODO: Statistical aggregate functions */
	m.def("median", &DuckDBPyRelation::Median, "Computes the median over all values present in a given column",
	      py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("mode", &DuckDBPyRelation::Mode, "Computes the mode over all values present in a given column",
	         py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("quantile_cont", &DuckDBPyRelation::QuantileCont,
	         "Computes the interpolated quantile value for a given column", py::arg("column"), py::arg("q") = 0.5,
	         py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "");
	DefineMethod({"quantile_disc", "quantile"}, m, &DuckDBPyRelation::QuantileDisc,
	             "Computes the exact quantile value for a given column", py::arg("column"), py::arg("q") = 0.5,
	             py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "");
	m.def("stddev_pop", &DuckDBPyRelation::StdPop, "Computes the population standard deviation for a given column",
	      py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "");
	DefineMethod({"stddev_samp", "stddev", "std"}, m, &DuckDBPyRelation::StdSamp,
	             "Computes the sample standard deviation for a given column", py::arg("column"), py::arg("groups") = "",
	             py::arg("window_spec") = "", py::arg("projected_columns") = "");
	m.def("var_pop", &DuckDBPyRelation::VarPop, "Computes the population variance for a given column",
	      py::arg("column"), py::arg("groups") = "", py::arg("window_spec") = "", py::arg("projected_columns") = "");
	DefineMethod({"var_samp", "variance", "var"}, m, &DuckDBPyRelation::VarSamp,
	             "Computes the sample variance for a given column", py::arg("column"), py::arg("groups") = "",
	             py::arg("window_spec") = "", py::arg("projected_columns") = "");
}

static void InitializeWindowOperators(py::class_<DuckDBPyRelation> &m) {
	m.def("row_number", &DuckDBPyRelation::RowNumber, "Computes the row number within the partition",
	      py::arg("window_spec"), py::arg("projected_columns") = "")
	    .def("rank", &DuckDBPyRelation::Rank, "Computes the rank within the partition", py::arg("window_spec"),
	         py::arg("projected_columns") = "");

	DefineMethod({"dense_rank", "rank_dense"}, m, &DuckDBPyRelation::DenseRank,
	             "Computes the dense rank within the partition", py::arg("window_spec"),
	             py::arg("projected_columns") = "");
	m.def("percent_rank", &DuckDBPyRelation::PercentRank, "Computes the relative rank within the partition",
	      py::arg("window_spec"), py::arg("projected_columns") = "")
	    .def("cume_dist", &DuckDBPyRelation::CumeDist, "Computes the cumulative distribution within the partition",
	         py::arg("window_spec"), py::arg("projected_columns") = "")
	    .def("first_value", &DuckDBPyRelation::FirstValue, "Computes the first value within the group or partition",
	         py::arg("column"), py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("n_tile", &DuckDBPyRelation::NTile, "Divides the partition as equally as possible into num_buckets",
	         py::arg("window_spec"), py::arg("num_buckets"), py::arg("projected_columns") = "")
	    .def("lag", &DuckDBPyRelation::Lag, "Computes the lag within the partition", py::arg("column"),
	         py::arg("window_spec"), py::arg("offset") = 1, py::arg("default_value") = "NULL",
	         py::arg("ignore_nulls") = false, py::arg("projected_columns") = "")
	    .def("last_value", &DuckDBPyRelation::LastValue, "Computes the last value within the group or partition",
	         py::arg("column"), py::arg("window_spec") = "", py::arg("projected_columns") = "")
	    .def("lead", &DuckDBPyRelation::Lead, "Computes the lead within the partition", py::arg("column"),
	         py::arg("window_spec"), py::arg("offset") = 1, py::arg("default_value") = "NULL",
	         py::arg("ignore_nulls") = false, py::arg("projected_columns") = "")
	    .def("nth_value", &DuckDBPyRelation::NthValue, "Computes the nth value within the partition", py::arg("column"),
	         py::arg("window_spec"), py::arg("offset"), py::arg("ignore_nulls") = false,
	         py::arg("projected_columns") = "");
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
	    .def("explain", &DuckDBPyRelation::Explain, py::arg("type") = "standard");
}

void DuckDBPyRelation::Initialize(py::handle &m) {
	auto relation_module = py::class_<DuckDBPyRelation>(m, "DuckDBPyRelation", py::module_local());
	InitializeReadOnlyProperties(relation_module);
	InitializeAggregates(relation_module);
	InitializeWindowOperators(relation_module);
	InitializeSetOperators(relation_module);
	InitializeMetaQueries(relation_module);
	InitializeConsumers(relation_module);

	relation_module.def("__getattr__", &DuckDBPyRelation::GetAttribute,
	                    "Get a projection relation created from this relation, on the provided column name",
	                    py::arg("name"));
	relation_module.def("__getitem__", &DuckDBPyRelation::GetAttribute,
	                    "Get a projection relation created from this relation, on the provided column name",
	                    py::arg("name"));

	relation_module.def("filter", &DuckDBPyRelation::Filter, "Filter the relation object by the filter in filter_expr",
	                    py::arg("filter_expr"));
	DefineMethod({"select", "project"}, relation_module, &DuckDBPyRelation::Project,
	             "Project the relation object by the projection in project_expr", py::kw_only(),
	             py::arg("groups") = "");
	DefineMethod({"select_types", "select_dtypes"}, relation_module, &DuckDBPyRelation::ProjectFromTypes,
	             "Select columns from the relation, by filtering based on type(s)", py::arg("types"));

	relation_module.def("__contains__", &DuckDBPyRelation::ContainsColumnByName, py::arg("name"));

	relation_module
	    .def("set_alias", &DuckDBPyRelation::SetAlias, "Rename the relation object to new alias", py::arg("alias"))
	    .def("order", &DuckDBPyRelation::Order, "Reorder the relation object by order_expr", py::arg("order_expr"))
	    .def("sort", &DuckDBPyRelation::Sort, "Reorder the relation object by the provided expressions")
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
	         "Inserts the relation object into an existing table named table_name", py::arg("table_name"));

	DefineMethod({"create", "to_table"}, relation_module, &DuckDBPyRelation::Create,
	             "Creates a new table named table_name with the contents of the relation object",
	             py::arg("table_name"));

	DefineMethod({"create_view", "to_view"}, relation_module, &DuckDBPyRelation::CreateView,
	             "Creates a view named view_name that refers to the relation object", py::arg("view_name"),
	             py::arg("replace") = true);

	relation_module
	    .def("map", &DuckDBPyRelation::Map, py::arg("map_function"), py::kw_only(), py::arg("schema") = py::none(),
	         "Calls the passed function on the relation")
	    .def("show", &DuckDBPyRelation::Print, "Display a summary of the data", py::kw_only(),
	         py::arg("max_width") = py::none(), py::arg("max_rows") = py::none(), py::arg("max_col_width") = py::none(),
	         py::arg("null_value") = py::none(), py::arg("render_mode") = py::none())
	    .def("__str__", &DuckDBPyRelation::ToString)
	    .def("__repr__", &DuckDBPyRelation::ToString);

	relation_module.def("sql_query", &DuckDBPyRelation::ToSQL, "Get the SQL query that is equivalent to the relation");
}

} // namespace duckdb
