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
#include "duckdb/common/string.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb_python/python_conversion.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"

namespace duckdb {

// pyduckdb.Value class
struct PythonValue : py::object {
public:
	PythonValue(const py::object &o) : py::object(o, borrowed_t {}) {
	}
	using py::object::object;

public:
	static bool check_(const py::handle &object) { // NOLINT
		auto &import_cache = *DuckDBPyConnection::ImportCache();
		return py::isinstance(object, import_cache.pyduckdb().value());
	}
};

struct DuckDBPyExpression : public std::enable_shared_from_this<DuckDBPyExpression> {
public:
	explicit DuckDBPyExpression(unique_ptr<ParsedExpression> expr);

public:
	std::shared_ptr<DuckDBPyExpression> shared_from_this() {
		return std::enable_shared_from_this<DuckDBPyExpression>::shared_from_this();
	}

public:
	static void Initialize(py::handle &m);

	string Type() const;

	string ToString() const;
	void Print() const;
	shared_ptr<DuckDBPyExpression> Add(const DuckDBPyExpression &other);

public:
	const ParsedExpression &GetExpression() const;

public:
	static shared_ptr<DuckDBPyExpression> BinaryFunctionExpression(const string &function_name,
	                                                               shared_ptr<DuckDBPyExpression> arg_one,
	                                                               shared_ptr<DuckDBPyExpression> arg_two);
	static shared_ptr<DuckDBPyExpression> ColumnExpression(const string &function_name);
	static shared_ptr<DuckDBPyExpression> ConstantExpression(const PythonValue &value);

private:
	static shared_ptr<DuckDBPyExpression> FunctionExpression(const string &function_name,
	                                                         vector<unique_ptr<ParsedExpression>> children);

private:
	unique_ptr<ParsedExpression> expression;
};

} // namespace duckdb
