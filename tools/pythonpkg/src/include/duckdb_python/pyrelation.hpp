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
#include "duckdb_python/pybind11/conversions/render_mode_enum.hpp"
#include "duckdb_python/pybind11/conversions/null_handling_enum.hpp"
#include "duckdb_python/pybind11/dataframe.hpp"
#include "duckdb_python/python_objects.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb {

struct DuckDBPyRelation {
public:
	explicit DuckDBPyRelation(shared_ptr<Relation> rel);
	explicit DuckDBPyRelation(unique_ptr<DuckDBPyResult> result);
	~DuckDBPyRelation();

public:
	static void Initialize(py::handle &m);

	py::list Description();

	void Close();

	unique_ptr<DuckDBPyRelation> GetAttribute(const string &name);

	py::str GetAlias();

	static unique_ptr<DuckDBPyRelation> EmptyResult(const shared_ptr<ClientContext> &context,
	                                                const vector<LogicalType> &types, vector<string> names);

	unique_ptr<DuckDBPyRelation> SetAlias(const string &expr);

	unique_ptr<DuckDBPyRelation> ProjectFromExpression(const string &expr);
	unique_ptr<DuckDBPyRelation> ProjectFromTypes(const py::object &types);
	unique_ptr<DuckDBPyRelation> Project(const py::args &args, const string &groups = "");
	unique_ptr<DuckDBPyRelation> Filter(const py::object &expr);
	unique_ptr<DuckDBPyRelation> FilterFromExpression(const string &expr);
	unique_ptr<DuckDBPyRelation> Limit(int64_t n, int64_t offset = 0);
	unique_ptr<DuckDBPyRelation> Order(const string &expr);
	unique_ptr<DuckDBPyRelation> Sort(const py::args &args);

	unique_ptr<DuckDBPyRelation> Aggregate(const py::object &expr, const string &groups = "");

	unique_ptr<DuckDBPyRelation> GenericAggregator(const string &function_name, const string &aggregated_columns,
	                                               const string &groups = "", const string &function_parameter = "",
	                                               const string &projected_columns = "");

	/* General aggregate functions */
	unique_ptr<DuckDBPyRelation> AnyValue(const string &column, const string &groups = "",
	                                      const string &window_spec = "", const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> ArgMax(const string &arg_column, const string &value_column, const string &groups = "",
	                                    const string &window_spec = "", const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> ArgMin(const string &arg_column, const string &value_column, const string &groups = "",
	                                    const string &window_spec = "", const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> Avg(const string &column, const string &groups = "", const string &window_spec = "",
	                                 const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> BitAnd(const string &column, const string &groups = "", const string &window_spec = "",
	                                    const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> BitOr(const string &column, const string &groups = "", const string &window_spec = "",
	                                   const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> BitXor(const string &column, const string &groups = "", const string &window_spec = "",
	                                    const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> BitStringAgg(const string &column, const Optional<py::object> &min,
	                                          const Optional<py::object> &max, const string &groups = "",
	                                          const string &window_spec = "", const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> BoolAnd(const string &column, const string &groups = "",
	                                     const string &window_spec = "", const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> BoolOr(const string &column, const string &groups = "", const string &window_spec = "",
	                                    const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> ValueCounts(const string &column, const string &groups = "");
	unique_ptr<DuckDBPyRelation> Count(const string &column, const string &groups = "", const string &window_spec = "",
	                                   const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> FAvg(const string &column, const string &groups = "", const string &window_spec = "",
	                                  const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> First(const string &column, const string &groups = "",
	                                   const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> FSum(const string &column, const string &groups = "", const string &window_spec = "",
	                                  const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> GeoMean(const string &column, const string &groups = "",
	                                     const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> Histogram(const string &column, const string &groups = "",
	                                       const string &window_spec = "", const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> Last(const string &column, const string &groups = "",
	                                  const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> List(const string &column, const string &groups = "", const string &window_spec = "",
	                                  const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> Max(const string &column, const string &groups = "", const string &window_spec = "",
	                                 const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> Min(const string &column, const string &groups = "", const string &window_spec = "",
	                                 const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> Product(const string &column, const string &groups = "",
	                                     const string &window_spec = "", const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> StringAgg(const string &column, const string &sep = ",", const string &groups = "",
	                                       const string &window_spec = "", const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> Sum(const string &column, const string &groups = "", const string &window_spec = "",
	                                 const string &projected_columns = "");
	/* TODO: Approximate aggregate functions */
	/* TODO: Statistical aggregate functions */
	unique_ptr<DuckDBPyRelation> Median(const string &column, const string &groups = "", const string &window_spec = "",
	                                    const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> Mode(const string &column, const string &groups = "", const string &window_spec = "",
	                                  const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> QuantileCont(const string &column, const py::object &q, const string &groups = "",
	                                          const string &window_spec = "", const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> QuantileDisc(const string &column, const py::object &q, const string &groups = "",
	                                          const string &window_spec = "", const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> StdPop(const string &column, const string &groups = "", const string &window_spec = "",
	                                    const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> StdSamp(const string &column, const string &groups = "",
	                                     const string &window_spec = "", const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> VarPop(const string &column, const string &groups = "", const string &window_spec = "",
	                                    const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> VarSamp(const string &column, const string &groups = "",
	                                     const string &window_spec = "", const string &projected_columns = "");

	unique_ptr<DuckDBPyRelation> Describe();

	string ToSQL();

	duckdb::pyarrow::RecordBatchReader FetchRecordBatchReader(idx_t rows_per_batch);

	idx_t Length();

	py::tuple Shape();

	unique_ptr<DuckDBPyRelation> Unique(const string &aggr_columns);

	unique_ptr<DuckDBPyRelation> GenericWindowFunction(const string &function_name, const string &function_parameters,
	                                                   const string &aggr_columns, const string &window_spec,
	                                                   const bool &ignore_nulls, const string &projected_columns);

	/* General purpose window functions */
	unique_ptr<DuckDBPyRelation> RowNumber(const string &window_spec, const string &projected_columns);
	unique_ptr<DuckDBPyRelation> Rank(const string &window_spec, const string &projected_columns);
	unique_ptr<DuckDBPyRelation> DenseRank(const string &window_spec, const string &projected_columns);
	unique_ptr<DuckDBPyRelation> PercentRank(const string &window_spec, const string &projected_columns);
	unique_ptr<DuckDBPyRelation> CumeDist(const string &window_spec, const string &projected_columns);
	unique_ptr<DuckDBPyRelation> FirstValue(const string &column, const string &window_spec = "",
	                                        const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> NTile(const string &window_spec, const int &num_buckets,
	                                   const string &projected_columns);
	unique_ptr<DuckDBPyRelation> Lag(const string &column, const string &window_spec, const int &offset,
	                                 const string &default_value, const bool &ignore_nulls,
	                                 const string &projected_columns);
	unique_ptr<DuckDBPyRelation> LastValue(const string &column, const string &window_spec = "",
	                                       const string &projected_columns = "");
	unique_ptr<DuckDBPyRelation> Lead(const string &column, const string &window_spec, const int &offset,
	                                  const string &default_value, const bool &ignore_nulls,
	                                  const string &projected_columns);

	unique_ptr<DuckDBPyRelation> NthValue(const string &column, const string &window_spec, const int &offset,
	                                      const bool &ignore_nulls, const string &projected_columns);

	unique_ptr<DuckDBPyRelation> Distinct();

	PandasDataFrame FetchDF(bool date_as_object);

	Optional<py::tuple> FetchOne();

	py::list FetchAll();

	py::list FetchMany(idx_t size);

	py::dict FetchNumpy();

	py::dict FetchPyTorch();

	py::dict FetchTF();

	py::dict FetchNumpyInternal(bool stream = false, idx_t vectors_per_chunk = 1);

	PandasDataFrame FetchDFChunk(const idx_t vectors_per_chunk = 1, bool date_as_object = false);

	duckdb::pyarrow::Table ToArrowTable(idx_t batch_size);

	duckdb::pyarrow::Table ToArrowTableInternal(idx_t batch_size, bool to_polars);

	PolarsDataFrame ToPolars(idx_t batch_size);

	py::object ToArrowCapsule(const py::object &requested_schema = py::none());

	duckdb::pyarrow::RecordBatchReader ToRecordBatch(idx_t batch_size);

	unique_ptr<DuckDBPyRelation> Union(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Except(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Intersect(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Map(py::function fun, Optional<py::object> schema);

	unique_ptr<DuckDBPyRelation> Join(DuckDBPyRelation *other, const py::object &condition, const string &type);

	void ToParquet(const string &filename, const py::object &compression = py::none(),
	               const py::object &field_ids = py::none(), const py::object &row_group_size_bytes = py::none(),
	               const py::object &row_group_size = py::none());

	void ToCSV(const string &filename, const py::object &sep = py::none(), const py::object &na_rep = py::none(),
	           const py::object &header = py::none(), const py::object &quotechar = py::none(),
	           const py::object &escapechar = py::none(), const py::object &date_format = py::none(),
	           const py::object &timestamp_format = py::none(), const py::object &quoting = py::none(),
	           const py::object &encoding = py::none(), const py::object &compression = py::none(),
	           const py::object &overwrite = py::none(), const py::object &per_thread_output = py::none(),
	           const py::object &use_tmp_file = py::none(), const py::object &partition_by = py::none(),
	           const py::object &write_partition_columns = py::none());

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
	void Print(const Optional<py::int_> &max_width, const Optional<py::int_> &max_rows,
	           const Optional<py::int_> &max_col_width, const Optional<py::str> &null_value,
	           const py::object &render_mode);

	string Explain(ExplainType type);

	static bool IsRelation(const py::object &object);

	bool CanBeRegisteredBy(Connection &con);
	bool CanBeRegisteredBy(ClientContext &context);
	bool CanBeRegisteredBy(shared_ptr<ClientContext> &context);

	Relation &GetRel();

	bool ContainsColumnByName(const string &name) const;

private:
	string ToStringInternal(const BoxRendererConfig &config, bool invalidate_cache = false);
	string GenerateExpressionList(const string &function_name, const string &aggregated_columns,
	                              const string &groups = "", const string &function_parameter = "",
	                              bool ignore_nulls = false, const string &projected_columns = "",
	                              const string &window_spec = "");
	string GenerateExpressionList(const string &function_name, vector<string> aggregated_columns,
	                              const string &groups = "", const string &function_parameter = "",
	                              bool ignore_nulls = false, const string &projected_columns = "",
	                              const string &window_spec = "");
	unique_ptr<DuckDBPyRelation> ApplyAggOrWin(const string &function_name, const string &agg_columns,
	                                           const string &function_parameters = "", const string &groups = "",
	                                           const string &window_spec = "", const string &projected_columns = "",
	                                           bool ignore_nulls = false);

	void AssertResult() const;
	void AssertResultOpen() const;
	void AssertRelation() const;
	void ExecuteOrThrow(bool stream_result = false);
	unique_ptr<QueryResult> ExecuteInternal(bool stream_result = false);

private:
	//! Whether the relation has been executed at least once
	bool executed;
	shared_ptr<Relation> rel;
	vector<LogicalType> types;
	vector<string> names;
	unique_ptr<DuckDBPyResult> result;
	std::string rendered_result;
};

} // namespace duckdb
