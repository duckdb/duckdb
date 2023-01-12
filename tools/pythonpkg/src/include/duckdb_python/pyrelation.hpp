//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyrelation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb.hpp"
#include "arrow_array_stream.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb_python/pandas_type.hpp"
#include "duckdb_python/registered_py_object.hpp"
#include "duckdb_python/pyresult.hpp"

namespace duckdb {

struct DuckDBPyConnection;

class PythonDependencies : public ExternalDependency {
public:
	explicit PythonDependencies(py::function map_function)
	    : ExternalDependency(ExternalDependenciesType::PYTHON_DEPENDENCY), map_function(std::move(map_function)) {};
	explicit PythonDependencies(unique_ptr<RegisteredObject> py_object)
	    : ExternalDependency(ExternalDependenciesType::PYTHON_DEPENDENCY) {
		py_object_list.push_back(std::move(py_object));
	};
	explicit PythonDependencies(unique_ptr<RegisteredObject> py_object_original,
	                            unique_ptr<RegisteredObject> py_object_copy)
	    : ExternalDependency(ExternalDependenciesType::PYTHON_DEPENDENCY) {
		py_object_list.push_back(std::move(py_object_original));
		py_object_list.push_back(std::move(py_object_copy));
	};
	py::function map_function;
	vector<unique_ptr<RegisteredObject>> py_object_list;
};

struct DuckDBPyRelation {
public:
	explicit DuckDBPyRelation(shared_ptr<Relation> rel);
	explicit DuckDBPyRelation(unique_ptr<DuckDBPyResult> result);

	shared_ptr<Relation> rel;

public:
	static void Initialize(py::handle &m);

	py::list Description();

	void Close();

	static unique_ptr<DuckDBPyRelation> FromDf(const DataFrame &df, shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> Values(py::object values = py::list(),
	                                           shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> FromQuery(const string &query, const string &alias,
	                                              shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> RunQuery(const string &query, const string &alias,
	                                             shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> FromCsvAuto(const string &filename,
	                                                shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> FromParquet(const string &file_glob, bool binary_as_string,
	                                                bool file_row_number, bool filename, bool hive_partitioning,
	                                                shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> FromParquets(const vector<string> &file_globs, bool binary_as_string,
	                                                 bool file_row_number, bool filename, bool hive_partitioning,
	                                                 shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> FromSubstrait(py::bytes &proto, shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> GetSubstrait(const string &query,
	                                                 shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> GetSubstraitJSON(const string &query,
	                                                     shared_ptr<DuckDBPyConnection> conn = nullptr);
	static unique_ptr<DuckDBPyRelation> FromSubstraitJSON(const string &json,
	                                                      shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> FromParquetDefault(const string &filename,
	                                                       shared_ptr<DuckDBPyConnection> conn = nullptr);

	static unique_ptr<DuckDBPyRelation> FromArrow(py::object &arrow_object,
	                                              shared_ptr<DuckDBPyConnection> conn = nullptr);

	unique_ptr<DuckDBPyRelation> Project(const string &expr);

	static unique_ptr<DuckDBPyRelation> ProjectDf(const DataFrame &df, const string &expr,
	                                              shared_ptr<DuckDBPyConnection> conn = nullptr);

	py::str GetAlias();

	unique_ptr<DuckDBPyRelation> SetAlias(const string &expr);

	static unique_ptr<DuckDBPyRelation> AliasDF(const DataFrame &df, const string &expr,
	                                            shared_ptr<DuckDBPyConnection> conn = nullptr);

	unique_ptr<DuckDBPyRelation> Filter(const string &expr);

	static unique_ptr<DuckDBPyRelation> FilterDf(const DataFrame &df, const string &expr,
	                                             shared_ptr<DuckDBPyConnection> conn = nullptr);

	unique_ptr<DuckDBPyRelation> Limit(int64_t n, int64_t offset = 0);

	static unique_ptr<DuckDBPyRelation> LimitDF(const DataFrame &df, int64_t n,
	                                            shared_ptr<DuckDBPyConnection> conn = nullptr);

	unique_ptr<DuckDBPyRelation> Order(const string &expr);

	static unique_ptr<DuckDBPyRelation> OrderDf(const DataFrame &df, const string &expr,
	                                            shared_ptr<DuckDBPyConnection> conn = nullptr);

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

	duckdb::pyarrow::RecordBatchReader FetchRecordBatchReader(idx_t chunk_size);

	idx_t Length();

	py::tuple Shape();

	unique_ptr<DuckDBPyRelation> Unique(const string &aggr_columns);

	unique_ptr<DuckDBPyRelation> GenericWindowFunction(const string &function_name, const string &aggr_columns);
	unique_ptr<DuckDBPyRelation> CumSum(const string &aggr_columns);
	unique_ptr<DuckDBPyRelation> CumProd(const string &aggr_columns);
	unique_ptr<DuckDBPyRelation> CumMax(const string &aggr_columns);
	unique_ptr<DuckDBPyRelation> CumMin(const string &aggr_columns);

	static unique_ptr<DuckDBPyRelation> AggregateDF(const DataFrame &df, const string &expr, const string &groups = "",
	                                                shared_ptr<DuckDBPyConnection> conn = nullptr);

	unique_ptr<DuckDBPyRelation> Distinct();

	static unique_ptr<DuckDBPyRelation> DistinctDF(const DataFrame &df, shared_ptr<DuckDBPyConnection> conn = nullptr);

	DataFrame FetchDF(bool date_as_object);

	py::object FetchOne();

	py::object FetchAll();

	py::object FetchMany(idx_t size);

	py::dict FetchNumpy();

	py::dict FetchNumpyInternal(bool stream = false, idx_t vectors_per_chunk = 1);

	DataFrame FetchDFChunk(idx_t vectors_per_chunk, bool date_as_object);

	duckdb::pyarrow::Table ToArrowTable(idx_t batch_size);

	duckdb::pyarrow::RecordBatchReader ToRecordBatch(idx_t batch_size);

	unique_ptr<DuckDBPyRelation> Union(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Except(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Intersect(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Map(py::function fun);

	unique_ptr<DuckDBPyRelation> Join(DuckDBPyRelation *other, const string &condition, const string &type);

	void WriteCsv(const string &file);

	static void WriteCsvDF(const DataFrame &df, const string &file, shared_ptr<DuckDBPyConnection> conn = nullptr);

	// should this return a rel with the new view?
	unique_ptr<DuckDBPyRelation> CreateView(const string &view_name, bool replace = true);

	unique_ptr<DuckDBPyRelation> Query(const string &view_name, const string &sql_query);

	// Update the internal result of the relation
	DuckDBPyRelation &Execute();

	static unique_ptr<DuckDBPyRelation> QueryDF(const DataFrame &df, const string &view_name, const string &sql_query,
	                                            shared_ptr<DuckDBPyConnection> conn = nullptr);

	void InsertInto(const string &table);

	void Insert(const py::object &params = py::list());

	void Create(const string &table);

	py::str Type();
	py::list Columns();
	py::list ColumnTypes();

	string Print();

	string Explain();

private:
	string GenerateExpressionList(const string &function_name, const string &aggregated_columns,
	                              const string &groups = "", const string &function_parameter = "",
	                              const string &projected_columns = "", const string &window_function = "");
	string GenerateExpressionList(const string &function_name, const vector<string> &aggregated_columns,
	                              const string &groups = "", const string &function_parameter = "",
	                              const string &projected_columns = "", const string &window_function = "");
	void AssertResult() const;
	void AssertResultOpen() const;
	void ExecuteOrThrow();

private:
	unique_ptr<DuckDBPyResult> result;
};

} // namespace duckdb
