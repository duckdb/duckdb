//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyresult.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb.hpp"
#include "arrow_array_stream.hpp"
#include "duckdb_python/pyconnection.hpp"
#include "duckdb/main/external_dependencies.hpp"

namespace duckdb {

struct DuckDBPyResult;

class PythonDependencies : public ExternalDependency {
public:
	explicit PythonDependencies(py::function map_function)
	    : ExternalDependency(ExternalDependenciesType::PYTHON_DEPENDENCY), map_function(std::move(map_function)) {};
	explicit PythonDependencies(unique_ptr<RegisteredObject> py_object)
	    : ExternalDependency(ExternalDependenciesType::PYTHON_DEPENDENCY) {
		py_object_list.push_back(move(py_object));
	};
	explicit PythonDependencies(unique_ptr<RegisteredObject> py_object_original,
	                            unique_ptr<RegisteredObject> py_object_copy)
	    : ExternalDependency(ExternalDependenciesType::PYTHON_DEPENDENCY) {
		py_object_list.push_back(move(py_object_original));
		py_object_list.push_back(move(py_object_copy));
	};
	py::function map_function;
	vector<unique_ptr<RegisteredObject>> py_object_list;
};

struct DuckDBPyRelation {
public:
	explicit DuckDBPyRelation(shared_ptr<Relation> rel);

	shared_ptr<Relation> rel;
	unique_ptr<PythonTableArrowArrayStreamFactory> arrow_stream_factory;

public:
	static void Initialize(py::handle &m);

	static unique_ptr<DuckDBPyRelation> FromDf(const py::object &df,
	                                           DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	static unique_ptr<DuckDBPyRelation> Values(py::object values = py::list(),
	                                           DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	static unique_ptr<DuckDBPyRelation> FromQuery(const string &query, const string &alias,
	                                              DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	static unique_ptr<DuckDBPyRelation> RunQuery(const string &query, const string &alias,
	                                             DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	static unique_ptr<DuckDBPyRelation> FromCsvAuto(const string &filename,
	                                                DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	static unique_ptr<DuckDBPyRelation> FromParquet(const string &filename, bool binary_as_string,
	                                                DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	static unique_ptr<DuckDBPyRelation>
	FromSubstrait(py::bytes &proto, DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	static unique_ptr<DuckDBPyRelation>
	GetSubstrait(const string &query, DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	static unique_ptr<DuckDBPyRelation>
	FromParquetDefault(const string &filename, DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	static unique_ptr<DuckDBPyRelation> FromArrow(py::object &arrow_object,
	                                              DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	unique_ptr<DuckDBPyRelation> Project(const string &expr);

	static unique_ptr<DuckDBPyRelation> ProjectDf(const py::object &df, const string &expr,
	                                              DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	py::str GetAlias();

	unique_ptr<DuckDBPyRelation> SetAlias(const string &expr);

	static unique_ptr<DuckDBPyRelation> AliasDF(const py::object &df, const string &expr,
	                                            DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	unique_ptr<DuckDBPyRelation> Filter(const string &expr);

	static unique_ptr<DuckDBPyRelation> FilterDf(const py::object &df, const string &expr,
	                                             DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	unique_ptr<DuckDBPyRelation> Limit(int64_t n, int64_t offset = 0);

	static unique_ptr<DuckDBPyRelation> LimitDF(const py::object &df, int64_t n,
	                                            DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	unique_ptr<DuckDBPyRelation> Order(const string &expr);

	static unique_ptr<DuckDBPyRelation> OrderDf(const py::object &df, const string &expr,
	                                            DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	unique_ptr<DuckDBPyRelation> Aggregate(const string &expr, const string &groups = "");

	unique_ptr<DuckDBPyRelation> GenericAggregator(const string &function_name, const string &aggregated_columns,
	                                               const string &groups = "", const string &function_parameter = "",
	                                               const string &projected_columns = "");

	unique_ptr<DuckDBPyRelation> Sum(const string &sum_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> Count(const string &count_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> Median(const string &median_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> Quantile(const string &q, const string &quantile_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> Min(const string &min_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> Max(const string &max_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> Mean(const string &mean_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> Var(const string &var_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> STD(const string &std_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> ValueCounts(const string &std_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> MAD(const string &aggr_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> Mode(const string &aggr_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> Abs(const string &aggr_columns);
	unique_ptr<DuckDBPyRelation> Prod(const string &aggr_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> Skew(const string &aggr_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> Kurt(const string &aggr_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> SEM(const string &aggr_columns, const string &groups = "");

	unique_ptr<DuckDBPyRelation> Describe();

	idx_t Length();

	py::tuple Shape();

	unique_ptr<DuckDBPyRelation> Unique(const string &aggr_columns);

	unique_ptr<DuckDBPyRelation> GenericWindowFunction(const string &function_name, const string &aggr_columns);
	unique_ptr<DuckDBPyRelation> CumSum(const string &aggr_columns);
	unique_ptr<DuckDBPyRelation> CumProd(const string &aggr_columns);
	unique_ptr<DuckDBPyRelation> CumMax(const string &aggr_columns);
	unique_ptr<DuckDBPyRelation> CumMin(const string &aggr_columns);

	static unique_ptr<DuckDBPyRelation> AggregateDF(const py::object &df, const string &expr, const string &groups = "",
	                                                DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	unique_ptr<DuckDBPyRelation> Distinct();

	static unique_ptr<DuckDBPyRelation> DistinctDF(const py::object &df,
	                                               DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	py::object ToDF();

	py::object Fetchone();

	py::object Fetchall();

	py::object ToArrowTable(idx_t batch_size);

	py::object ToRecordBatch(idx_t batch_size);

	unique_ptr<DuckDBPyRelation> Union(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Except(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Intersect(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Map(py::function fun);

	unique_ptr<DuckDBPyRelation> Join(DuckDBPyRelation *other, const string &condition, const string &type);

	void WriteCsv(const string &file);

	static void WriteCsvDF(const py::object &df, const string &file,
	                       DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	// should this return a rel with the new view?
	unique_ptr<DuckDBPyRelation> CreateView(const string &view_name, bool replace = true);

	unique_ptr<DuckDBPyResult> Query(const string &view_name, const string &sql_query);

	unique_ptr<DuckDBPyResult> Execute();

	static unique_ptr<DuckDBPyResult> QueryDF(const py::object &df, const string &view_name, const string &sql_query,
	                                          DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	void InsertInto(const string &table);

	void Insert(py::object params = py::list());

	void Create(const string &table);

	py::str Type();
	py::list Columns();
	py::list ColumnTypes();

	string Print();

private:
	string GenerateExpressionList(const string &function_name, const string &aggregated_columns,
	                              const string &groups = "", const string &function_parameter = "",
	                              const string &projected_columns = "", const string &window_function = "");
};

} // namespace duckdb
