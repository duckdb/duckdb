//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyrelation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb_python/arrow/arrow_array_stream.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"
#include "duckdb_python/pybind11/registered_py_object.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb_python/pybind11/conversions/explain_enum.hpp"
#include "duckdb_python/pybind11/dataframe.hpp"

namespace duckdb {

struct DuckDBPyConnection;

class PythonDependencies : public ExternalDependency {
public:
	explicit PythonDependencies() : ExternalDependency(ExternalDependenciesType::PYTHON_DEPENDENCY) {
	}
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

public:
	static void Initialize(py::handle &m);

	py::list Description();

	void Close();

	unique_ptr<DuckDBPyRelation> GetAttribute(const string &name);

	py::str GetAlias();

	unique_ptr<DuckDBPyRelation> SetAlias(const string &expr);

	unique_ptr<DuckDBPyRelation> ProjectFromExpression(const string &expr);
	unique_ptr<DuckDBPyRelation> ProjectFromTypes(const py::object &types);
	unique_ptr<DuckDBPyRelation> Project(const string &expr);

	unique_ptr<DuckDBPyRelation> Filter(const string &expr);
	unique_ptr<DuckDBPyRelation> Limit(int64_t n, int64_t offset = 0);
	unique_ptr<DuckDBPyRelation> Order(const string &expr);

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

	string ToSQL();

	duckdb::pyarrow::RecordBatchReader FetchRecordBatchReader(idx_t rows_per_batch);

	idx_t Length();

	py::tuple Shape();

	unique_ptr<DuckDBPyRelation> Unique(const string &aggr_columns);

	unique_ptr<DuckDBPyRelation> GenericWindowFunction(const string &function_name, const string &aggr_columns);
	unique_ptr<DuckDBPyRelation> CumSum(const string &aggr_columns);
	unique_ptr<DuckDBPyRelation> CumProd(const string &aggr_columns);
	unique_ptr<DuckDBPyRelation> CumMax(const string &aggr_columns);
	unique_ptr<DuckDBPyRelation> CumMin(const string &aggr_columns);

	unique_ptr<DuckDBPyRelation> Distinct();

	PandasDataFrame FetchDF(bool date_as_object);

	Optional<py::tuple> FetchOne();

	py::list FetchAll();

	py::list FetchMany(idx_t size);

	py::dict FetchNumpy();

	py::dict FetchPyTorch();

	py::dict FetchTF();

	py::dict FetchNumpyInternal(bool stream = false, idx_t vectors_per_chunk = 1);

	PandasDataFrame FetchDFChunk(idx_t vectors_per_chunk, bool date_as_object);

	duckdb::pyarrow::Table ToArrowTable(idx_t batch_size);

	PolarsDataFrame ToPolars(idx_t batch_size);

	duckdb::pyarrow::RecordBatchReader ToRecordBatch(idx_t batch_size);

	unique_ptr<DuckDBPyRelation> Union(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Except(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Intersect(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Map(py::function fun, Optional<py::object> schema);

	unique_ptr<DuckDBPyRelation> Join(DuckDBPyRelation *other, const string &condition, const string &type);

	void ToParquet(const string &filename, const py::object &compression = py::none());

	void ToCSV(const string &filename, const py::object &sep = py::none(), const py::object &na_rep = py::none(),
	           const py::object &header = py::none(), const py::object &quotechar = py::none(),
	           const py::object &escapechar = py::none(), const py::object &date_format = py::none(),
	           const py::object &timestamp_format = py::none(), const py::object &quoting = py::none(),
	           const py::object &encoding = py::none(), const py::object &compression = py::none());

	// should this return a rel with the new view?
	unique_ptr<DuckDBPyRelation> CreateView(const string &view_name, bool replace = true);

	unique_ptr<DuckDBPyRelation> Query(const string &view_name, const string &sql_query);

	// Update the internal result of the relation
	DuckDBPyRelation &Execute();

	void InsertInto(const string &table);

	void Insert(const py::object &params = py::list());

	void Create(const string &table);

	py::str Type();
	py::list Columns();
	py::list ColumnTypes();

	string ToString();
	void Print();

	string Explain(ExplainType type);

	static bool IsRelation(const py::object &object);

	Relation &GetRel();

private:
	string GenerateExpressionList(const string &function_name, const string &aggregated_columns,
	                              const string &groups = "", const string &function_parameter = "",
	                              const string &projected_columns = "", const string &window_function = "");
	string GenerateExpressionList(const string &function_name, const vector<string> &aggregated_columns,
	                              const string &groups = "", const string &function_parameter = "",
	                              const string &projected_columns = "", const string &window_function = "");
	void AssertResult() const;
	void AssertResultOpen() const;
	void AssertRelation() const;
	bool ContainsColumnByName(const string &name) const;
	void ExecuteOrThrow(bool stream_result = false);
	unique_ptr<QueryResult> ExecuteInternal(bool stream_result = false);

private:
	shared_ptr<Relation> rel;
	vector<LogicalType> types;
	vector<string> names;
	unique_ptr<DuckDBPyResult> result;
	std::string rendered_result;
};

} // namespace duckdb
