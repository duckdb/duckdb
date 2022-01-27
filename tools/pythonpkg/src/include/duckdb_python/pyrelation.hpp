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

namespace duckdb {
struct DuckDBPyResult;

struct DuckDBPyRelation {
public:
	explicit DuckDBPyRelation(shared_ptr<Relation> rel);

	shared_ptr<Relation> rel;
	unique_ptr<PythonTableArrowArrayStreamFactory> arrow_stream_factory;

public:
	static void Initialize(py::handle &m);

	static unique_ptr<DuckDBPyRelation> FromDf(py::object df,
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
	FromParquetDefault(const string &filename, DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	static unique_ptr<DuckDBPyRelation>
	FromArrowTable(py::object &table, DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	unique_ptr<DuckDBPyRelation> Project(const string &expr);

	static unique_ptr<DuckDBPyRelation> ProjectDf(py::object df, const string &expr,
	                                              DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	py::str GetAlias();

	unique_ptr<DuckDBPyRelation> SetAlias(const string &expr);

	static unique_ptr<DuckDBPyRelation> AliasDF(py::object df, const string &expr,
	                                            DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	unique_ptr<DuckDBPyRelation> Filter(const string &expr);

	static unique_ptr<DuckDBPyRelation> FilterDf(py::object df, const string &expr,
	                                             DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	unique_ptr<DuckDBPyRelation> Limit(int64_t n);

	static unique_ptr<DuckDBPyRelation> LimitDF(py::object df, int64_t n,
	                                            DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	unique_ptr<DuckDBPyRelation> Order(const string &expr);

	static unique_ptr<DuckDBPyRelation> OrderDf(py::object df, const string &expr,
	                                            DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	unique_ptr<DuckDBPyRelation> Aggregate(const string &expr, const string &groups = "");

	static unique_ptr<DuckDBPyRelation> AggregateDF(py::object df, const string &expr, const string &groups = "",
	                                                DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	unique_ptr<DuckDBPyRelation> Distinct();

	static unique_ptr<DuckDBPyRelation> DistinctDF(py::object df,
	                                               DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	py::object ToDF();

	py::object Fetchone();

	py::object Fetchall();

	py::object ToArrowTable();

	unique_ptr<DuckDBPyRelation> Union(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Except(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Intersect(DuckDBPyRelation *other);

	unique_ptr<DuckDBPyRelation> Map(py::function fun);

	unique_ptr<DuckDBPyRelation> Join(DuckDBPyRelation *other, const string &condition, const string &type);

	void WriteCsv(const string &file);

	static void WriteCsvDF(py::object df, const string &file,
	                       DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	// should this return a rel with the new view?
	unique_ptr<DuckDBPyRelation> CreateView(const string &view_name, bool replace = true);

	unique_ptr<DuckDBPyResult> Query(const string &view_name, const string &sql_query);

	unique_ptr<DuckDBPyResult> Execute();

	static unique_ptr<DuckDBPyResult> QueryDF(py::object df, const string &view_name, const string &sql_query,
	                                          DuckDBPyConnection *conn = DuckDBPyConnection::DefaultConnection());

	void InsertInto(const string &table);

	void Insert(py::object params = py::list());

	void Create(const string &table);

	py::str Type();
	py::list Columns();
	py::list ColumnTypes();

	string Print();

private:
	py::object map_function;
};

} // namespace duckdb
